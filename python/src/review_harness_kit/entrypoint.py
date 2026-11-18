from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlsplit

from google.protobuf import json_format

from review_harness_kit.adapters.aider import AiderAdapter
from review_harness_kit.adapters.codex import CodexAdapter
from review_harness_kit.adapters.command import CommandAdapter, MockLLMAdapter, SingleShotAdapter
from review_harness_kit.adapters.goose import GooseAdapter
from review_harness_kit.adapters.mini_swe_agent import MiniSWEAgentAdapter
from review_harness_kit.adapters.opencode import OpenCodeAdapter
from review_harness_kit.adapters.openhands import OpenHandsAdapter
from review_harness_kit.adapters.pr_agent import PRAgentAdapter
from review_harness_kit.contracts import (
    HarnessAdapter,
    HarnessInvocation,
    HarnessUnsupportedError,
    ProcessResult,
    RawOutput,
    RunContext,
    load_adapter,
)
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import EMBEDDED_SCHEMA_PATH, normalize, write_report

HarnessUnsupported = HarnessUnsupportedError

BUILTIN_ADAPTERS: dict[str, type[HarnessAdapter]] = {
    "pr-agent": PRAgentAdapter,
    "openhands": OpenHandsAdapter,
    "codex": CodexAdapter,
    "mini-swe-agent": MiniSWEAgentAdapter,
    "opencode": OpenCodeAdapter,
    "goose": GooseAdapter,
    "aider": AiderAdapter,
    "command": CommandAdapter,
    "external-cmd": CommandAdapter,
    "mock-llm": MockLLMAdapter,
    "single-shot": SingleShotAdapter,
}


def resolve_adapter(name: str) -> HarnessAdapter:
    try:
        return load_adapter(name)
    except LookupError:
        canonical = name.strip().replace("_", "-")
        adapter_cls = BUILTIN_ADAPTERS.get(canonical)
        if adapter_cls is not None:
            return adapter_cls()
        raise

DEFAULT_IN_DIR = Path("/workspace/.review/in")
DEFAULT_OUT_DIR = Path("/workspace/.review/out")
DEFAULT_SRC_DIR = Path("/workspace/src")
DEFAULT_SCRATCH_HOME = Path("/scratch/home")
CONTAINER_SCHEMA_PATH = Path("/opt/kit/share/llm_review_report.schema.json")
CONTAINER_PROMPT_PATH = Path("/opt/kit/share/review_task.md")
EMBEDDED_PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "review_task.md"
REPO_SCHEMA_PATH = Path(__file__).resolve().parents[3] / "gen" / "schema" / "llm_review_report.schema.json"

STATIC_OFFLINE_ENV = {
    "GIT_CONFIG_COUNT": "1",
    "GIT_CONFIG_KEY_0": "safe.directory",
    "GIT_CONFIG_VALUE_0": "*",
    "GIT_OPTIONAL_LOCKS": "0",
    "GIT_TERMINAL_PROMPT": "0",
    "LITELLM_LOCAL_MODEL_COST_MAP": "True",
    "DO_NOT_TRACK": "1",
    "PIP_NO_INDEX": "1",
    "NPM_CONFIG_OFFLINE": "true",
    "PYTHONDONTWRITEBYTECODE": "1",
    "NO_COLOR": "1",
}


def normalize_base_url(raw_url: str) -> str:
    trimmed = raw_url.strip()
    if not trimmed:
        return ""
    parts = urlsplit(trimmed)
    if parts.scheme and parts.netloc and parts.path.rstrip("/") == "":
        return trimmed.rstrip("/") + "/v1"
    return trimmed.rstrip("/")


def parse_stdin_credentials(stdin: BinaryIO) -> dict[str, Any]:
    raw_bytes = stdin.read()
    if not raw_bytes or not raw_bytes.strip():
        return {}
    decoded = json.loads(raw_bytes.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise TypeError("stdin payload must be a JSON object")
    return decoded


def resolve_task_file(args: argparse.Namespace, secret: dict[str, Any]) -> Path:
    explicit_task = (
        args.task
        or secret.get("task_file")
        or os.environ.get("REVIEW_TASK_FILE")
        or os.environ.get("REVIEW_KIT_TASK_FILE")
    )
    if explicit_task:
        return Path(str(explicit_task))

    explicit_in = (
        args.in_dir
        or secret.get("in_dir")
        or os.environ.get("REVIEW_IN_DIR")
        or os.environ.get("REVIEW_KIT_IN_DIR")
    )
    if explicit_in:
        return Path(str(explicit_in)) / "task.json"

    workspace_env = os.environ.get("REVIEW_WORKSPACE")
    if workspace_env:
        return Path(workspace_env) / ".review" / "in" / "task.json"

    default_task = DEFAULT_IN_DIR / "task.json"
    if default_task.is_file():
        return default_task

    cwd_task = Path.cwd() / ".review" / "in" / "task.json"
    if cwd_task.is_file():
        return cwd_task

    return default_task


def apply_task_defaults(task: review_pb2.AgentTask, task_file: Path, args: argparse.Namespace) -> None:
    in_dir = task_file.parent
    override_out = args.out_dir or os.environ.get("REVIEW_OUT_DIR") or os.environ.get("REVIEW_KIT_OUT_DIR")
    if override_out:
        task.out_dir = override_out
    elif not task.out_dir:
        task.out_dir = str(in_dir.parent / "out") if in_dir != DEFAULT_IN_DIR else str(DEFAULT_OUT_DIR)

    if not task.src_dir:
        sibling_src = in_dir.parent.parent / "src"
        task.src_dir = str(sibling_src) if in_dir != DEFAULT_IN_DIR and sibling_src.exists() else str(DEFAULT_SRC_DIR)

    if not task.diff_file:
        task.diff_file = str(in_dir / "diff.patch")

    if not task.rules_dir:
        rules_subdir = in_dir / "rules"
        rules_md = in_dir / "rules.md"
        if rules_subdir.exists():
            task.rules_dir = str(rules_subdir)
        elif rules_md.is_file():
            task.rules_dir = str(rules_md)
        else:
            task.rules_dir = str(rules_subdir)


def resolve_scratch_home(out_dir: Path, args: argparse.Namespace, secret: dict[str, Any]) -> Path:
    explicit_scratch = (
        args.scratch_dir
        or secret.get("scratch_dir")
        or os.environ.get("REVIEW_SCRATCH_DIR")
        or os.environ.get("REVIEW_KIT_SCRATCH_DIR")
    )
    if explicit_scratch:
        target = Path(str(explicit_scratch))
        target.mkdir(parents=True, exist_ok=True)
        return target

    scratch_root = Path("/scratch")
    if scratch_root.is_dir() and os.access(scratch_root, os.W_OK):
        DEFAULT_SCRATCH_HOME.mkdir(parents=True, exist_ok=True)
        return DEFAULT_SCRATCH_HOME

    for candidate in (out_dir.parent / "scratch" / "home", out_dir / ".scratch_home"):
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except OSError:
            continue
    raise OSError("unable to initialize writable scratch home directory")


def resolve_schema_path(args: argparse.Namespace) -> Path:
    explicit = args.schema or os.environ.get("REVIEW_SCHEMA_PATH")
    if explicit:
        return Path(explicit)
    for candidate in (CONTAINER_SCHEMA_PATH, REPO_SCHEMA_PATH, EMBEDDED_SCHEMA_PATH):
        if candidate.is_file():
            return candidate
    return EMBEDDED_SCHEMA_PATH


def read_rules(rules_path: Path, fallback_in_dir: Path) -> str:
    if rules_path.is_file():
        text = rules_path.read_text(encoding="utf-8", errors="replace").strip()
        return text or "(no repository-specific review rules provided)"
    if rules_path.is_dir():
        parts: list[str] = []
        for entry in sorted(rules_path.rglob("*")):
            if entry.is_file():
                rel = entry.relative_to(rules_path).as_posix()
                body = entry.read_text(encoding="utf-8", errors="replace").strip()
                if body:
                    parts.append(f"[{rel}]\n{body}")
        if parts:
            return "\n\n".join(parts)
    alt_md = fallback_in_dir / "rules.md"
    if alt_md.is_file():
        text = alt_md.read_text(encoding="utf-8", errors="replace").strip()
        if text:
            return text
    return "(no repository-specific review rules provided)"


def render_task_prompt(task: review_pb2.AgentTask, rules_text: str, schema_path: Path) -> str:
    template_path = CONTAINER_PROMPT_PATH if CONTAINER_PROMPT_PATH.is_file() else EMBEDDED_PROMPT_PATH
    template = template_path.read_text(encoding="utf-8")
    diff_path = Path(task.diff_file) if task.diff_file else None
    diff_text = (
        diff_path.read_text(encoding="utf-8", errors="replace")[:120_000]
        if diff_path is not None and diff_path.is_file()
        else ""
    )
    schema_text = (
        schema_path.read_text(encoding="utf-8")
        if schema_path.is_file()
        else EMBEDDED_SCHEMA_PATH.read_text(encoding="utf-8")
    )
    masked = ", ".join(task.masked_files) if task.masked_files else "none"
    replacements = {
        "{{ rules_text }}": rules_text,
        "{{ src_dir }}": task.src_dir,
        "{{ diff_file }}": task.diff_file,
        "{{ masked_files }}": masked,
        "{{ out_dir }}": task.out_dir,
        "{{ report_json_schema }}": schema_text.strip(),
        "{{ pr_title }}": task.pr_title or "(untitled)",
        "{{ pr_body }}": task.pr_body or "(empty)",
        "{{ diff_text }}": diff_text,
    }
    rendered = template
    for placeholder, val in replacements.items():
        rendered = rendered.replace(placeholder, val)
    return rendered


def build_subprocess_path() -> str:
    entries: list[str] = [str(Path(sys.executable).resolve().parent)]
    inherited = os.environ.get("PATH", "")
    for segment in inherited.split(":"):
        if segment and segment not in entries:
            entries.append(segment)
    for std_dir in ("/usr/local/bin", "/usr/bin", "/bin"):
        if std_dir not in entries:
            entries.append(std_dir)
    return ":".join(entries)


def kill_process_group(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        return


def run_harness(
    invocation: HarnessInvocation,
    ctx: RunContext,
    out_dir: Path,
    timeout_s: float,
) -> ProcessResult:
    tmp_dir = Path("/tmp")
    if not (tmp_dir.is_dir() and os.access(tmp_dir, os.W_OK)):
        tmp_dir = ctx.home / "tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
    for subdir in (ctx.home / ".config", ctx.home / ".cache", ctx.home / ".local" / "share"):
        subdir.mkdir(parents=True, exist_ok=True)

    env = {
        **STATIC_OFFLINE_ENV,
        "PATH": build_subprocess_path(),
        "TMPDIR": str(tmp_dir),
        "HOME": str(ctx.home),
        "XDG_CONFIG_HOME": str(ctx.home / ".config"),
        "XDG_CACHE_HOME": str(ctx.home / ".cache"),
        "XDG_DATA_HOME": str(ctx.home / ".local" / "share"),
        "OPENAI_BASE_URL": ctx.base_url,
        "OPENAI_API_KEY": ctx.api_key,
        **invocation.env,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "harness.log"
    try:
        with log_path.open("wb") as log_file:
            proc = subprocess.Popen(
                invocation.argv,
                cwd=invocation.cwd,
                env=env,
                stdin=subprocess.PIPE if invocation.stdin_text is not None else subprocess.DEVNULL,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            try:
                payload = invocation.stdin_text.encode("utf-8") if invocation.stdin_text is not None else None
                proc.communicate(payload, timeout=timeout_s)
                timed_out = False
            except subprocess.TimeoutExpired:
                kill_process_group(proc.pid)
                proc.wait()
                timed_out = True
    except OSError as exc:
        log_path.write_text(f"failed to execute harness binary: {exc}\n", encoding="utf-8")
        return ProcessResult(exit_code=127, timed_out=False, log_path=log_path)

    return ProcessResult(exit_code=proc.returncode, timed_out=timed_out, log_path=log_path)


def parse_run_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="review-kit run")
    parser.add_argument("--task", "--task-file", dest="task", default=None)
    parser.add_argument("--in-dir", dest="in_dir", default=None)
    parser.add_argument("--out-dir", dest="out_dir", default=None)
    parser.add_argument("--scratch-dir", dest="scratch_dir", default=None)
    parser.add_argument("--schema", dest="schema", default=None)
    return parser.parse_args(argv)


def run_command(stdin: BinaryIO, argv: list[str] | None = None) -> int:
    args = parse_run_args([] if argv is None else argv)
    secret = parse_stdin_credentials(stdin)

    raw_base_url = str(
        secret.get("openai_base_url")
        or secret.get("base_url")
        or os.environ.get("OPENAI_BASE_URL")
        or ""
    )
    api_key = str(
        secret.get("openai_api_key")
        or secret.get("api_key")
        or os.environ.get("OPENAI_API_KEY")
        or ""
    )
    base_url = normalize_base_url(raw_base_url)

    task_file = resolve_task_file(args, secret)
    task = json_format.Parse(
        task_file.read_text(encoding="utf-8"),
        review_pb2.AgentTask(),
        ignore_unknown_fields=True,
    )
    apply_task_defaults(task, task_file, args)

    adapter_name = task.harness.adapter or task.harness.profile
    adapter = resolve_adapter(adapter_name)

    out_dir = Path(task.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    scratch_home = resolve_scratch_home(out_dir, args, secret)
    schema_path = resolve_schema_path(args)
    rules_text = read_rules(Path(task.rules_dir), task_file.parent)

    ctx = RunContext(
        base_url=base_url,
        api_key=api_key,
        home=scratch_home,
        prompt=render_task_prompt(task, rules_text, schema_path),
        rules_text=rules_text,
        schema_path=schema_path,
    )

    timeout_s = float(task.budget.wall_clock_seconds if task.budget.wall_clock_seconds > 0 else 300)
    try:
        invocation = adapter.prepare(task, ctx)
        result = run_harness(invocation, ctx, out_dir=out_dir, timeout_s=timeout_s)
        raw = adapter.collect(task, ctx, result)
    except HarnessUnsupportedError:
        result = ProcessResult(exit_code=None, timed_out=False, log_path=out_dir / "harness.log")
        raw = RawOutput(kind="text", text="")

    report = normalize(adapter, raw, task, ctx, result)
    write_report(report, out_dir / "report.json")
    return 0


def main(argv: list[str] | None = None) -> None:
    cli_args = sys.argv[1:] if argv is None else argv
    cmd = cli_args[0] if cli_args else ""
    if cmd == "wait":
        signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
        signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
        while True:
            signal.pause()
    if cmd == "run":
        sys.exit(run_command(sys.stdin.buffer, cli_args[1:]))
    raise SystemExit(f"unknown command: {cmd!r}")


if __name__ == "__main__":
    main()
