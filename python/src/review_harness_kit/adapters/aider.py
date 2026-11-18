from __future__ import annotations

from pathlib import Path

from review_harness_kit.contracts import HarnessInvocation, ProcessResult, RawOutput, RunContext
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import parse_report_json

MAX_INLINE_MESSAGE_BYTES = 96_000
MAX_READ_CONTEXT_FILES = 20


def existing_changed_files(task: review_pb2.AgentTask) -> list[str]:
    if not task.diff_file or not task.src_dir:
        return []
    diff_path = Path(task.diff_file)
    src_root = Path(task.src_dir)
    if not diff_path.is_file() or not src_root.is_dir():
        return []
    resolved_root = src_root.resolve()
    files: list[str] = []
    for line in diff_path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith("+++ b/"):
            continue
        rel = line[6:].strip()
        if not rel or rel == "/dev/null" or rel in files or Path(rel).is_absolute():
            continue
        candidate = (resolved_root / rel).resolve()
        if candidate.is_relative_to(resolved_root) and candidate.is_file():
            files.append(rel)
            if len(files) >= MAX_READ_CONTEXT_FILES:
                break
    return files


class AiderAdapter:
    name = "aider"
    cache_shape = "single_shot"

    def prepare(self, task: review_pb2.AgentTask, ctx: RunContext) -> HarnessInvocation:
        aider_dir = ctx.home / ".aider"
        aider_dir.mkdir(parents=True, exist_ok=True)
        prompt_file = aider_dir / "review_task.md"
        prompt_file.write_text(ctx.prompt, encoding="utf-8")

        env = {
            "OPENAI_API_BASE": ctx.base_url,
            "OPENAI_API_KEY": ctx.api_key,
            "AIDER_ANALYTICS": "false",
            "AIDER_CHECK_UPDATE": "false",
            "AIDER_CACHE_DIR": str(aider_dir / "cache"),
        }
        binary = task.harness.options.get("bin") or "aider"
        argv = [
            binary,
            "--chat-mode",
            "ask",
            "--no-auto-commits",
            "--no-git",
            "--yes-always",
            "--no-stream",
            "--no-pretty",
            "--no-check-update",
            "--analytics-disable",
            "--no-show-model-warnings",
            "--input-history-file",
            str(aider_dir / "input.history"),
            "--chat-history-file",
            str(aider_dir / "chat.history.md"),
            "--llm-history-file",
            str(aider_dir / "llm.history.log"),
            "--model",
            f"openai/{task.model_alias or 'default'}",
            "--openai-api-base",
            ctx.base_url,
        ]
        for rel_file in existing_changed_files(task):
            argv.extend(["--read", rel_file])

        if len(ctx.prompt.encode("utf-8")) <= MAX_INLINE_MESSAGE_BYTES:
            argv.extend(["--message", ctx.prompt])
        else:
            argv.extend(["--message-file", str(prompt_file)])

        return HarnessInvocation(argv=argv, cwd=task.src_dir or "/workspace/src", env=env)

    def collect(self, task: review_pb2.AgentTask, ctx: RunContext, result: ProcessResult) -> RawOutput:
        out = Path(task.out_dir or "/workspace/.review/out")
        raw_json = out / "raw_report.json"
        if raw_json.is_file():
            content = raw_json.read_text(encoding="utf-8", errors="replace")
            if content.strip():
                return RawOutput(kind="json", text=content)

        log_text = (
            result.log_path.read_text(encoding="utf-8", errors="replace")[-60_000:]
            if result.log_path.is_file()
            else ""
        )
        return RawOutput(kind="markdown", text=log_text)

    def extract(self, raw: RawOutput, task: review_pb2.AgentTask) -> review_pb2.ReviewReport | None:
        if raw.kind != "json" or not raw.text.strip():
            return None
        return parse_report_json(raw.text)
