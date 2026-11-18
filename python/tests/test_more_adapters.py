from __future__ import annotations

import io
import json
import stat
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Self

import pytest
import yaml
from google.protobuf import json_format

from review_harness_kit.adapters.aider import AiderAdapter
from review_harness_kit.adapters.goose import GooseAdapter
from review_harness_kit.adapters.mini_swe_agent import MiniSWEAgentAdapter
from review_harness_kit.adapters.opencode import OpenCodeAdapter
from review_harness_kit.contracts import HarnessAdapter, ProcessResult, RunContext, load_adapter
from review_harness_kit.entrypoint import BUILTIN_ADAPTERS, resolve_adapter, run_command
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import EMBEDDED_SCHEMA_PATH


class MockNormalizeHTTPServer:
    def __init__(self, normalized_report: dict[str, Any]) -> None:
        self.normalized_report = normalized_report
        self.normalize_calls = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                _ = self.rfile.read(length)
                if self.headers.get("x-review-phase") == "normalize":
                    outer.normalize_calls += 1
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                payload = {
                    "id": "chatcmpl-m2",
                    "object": "chat.completion",
                    "created": 1700000000,
                    "model": "qwen-coder",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": json.dumps(outer.normalized_report),
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 90, "completion_tokens": 35, "total_tokens": 125},
                }
                self.wfile.write(json.dumps(payload).encode())

            def log_message(self, format: str, *args: object) -> None:
                return

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._httpd.server_address
        return f"http://{host}:{port}/v1"

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)


def make_ctx(tmp_path: Path, prompt: str = "Inspect PR diff for concrete bugs.") -> RunContext:
    home = tmp_path / "scratch" / "home"
    home.mkdir(parents=True, exist_ok=True)
    return RunContext(
        base_url="http://127.0.0.1:18080/v1",
        api_key="eyJ.m2.secret.token",
        home=home,
        prompt=prompt,
        rules_text="Strictly read-only code review.",
        schema_path=EMBEDDED_SCHEMA_PATH,
    )


def sample_report_dict(file_path: str = "pkg/queue.go", line: int = 4, target: str = "items = items[1:]") -> dict[str, Any]:
    return {
        "summary": "Identified slice bounds bug in pkg/queue.go.",
        "findings": [
            {
                "file": file_path,
                "line": line,
                "side": "DIFF_SIDE_RIGHT",
                "targetLineText": target,
                "anchor": "ANCHOR_KIND_QUOTED",
                "severity": "SEVERITY_HIGH",
                "confidence": 0.89,
                "category": "FINDING_CATEGORY_CORRECTNESS",
                "title": "Panic on empty slice pop",
                "explanation": "Slicing items[1:] panics when len(items) == 0.",
                "suggestion": "Check if len(items) == 0 before slicing.",
                "evidence": "Line 4 slices items without length guard.",
            }
        ],
    }


@pytest.mark.parametrize(
    "name",
    [
        "pr-agent",
        "openhands",
        "codex",
        "mini-swe-agent",
        "mini_swe_agent",
        "opencode",
        "goose",
        "aider",
    ],
)
def test_all_seven_planned_harnesses_registered(name: str) -> None:
    via_ep = load_adapter(name)
    via_entrypoint = resolve_adapter(name)
    assert isinstance(via_ep, HarnessAdapter)
    assert isinstance(via_entrypoint, HarnessAdapter)
    canonical = name.replace("_", "-")
    assert canonical in BUILTIN_ADAPTERS


def test_mini_swe_agent_prepare_collect_and_extract(tmp_path: Path) -> None:
    adapter = MiniSWEAgentAdapter()
    assert adapter.cache_shape == "append_only"
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True)
    task = review_pb2.AgentTask(
        src_dir=str(tmp_path / "src"),
        out_dir=str(out_dir),
        model_alias="qwen-coder",
    )
    ctx = make_ctx(tmp_path)
    inv = adapter.prepare(task, ctx)

    assert inv.argv[0] == "mini"
    assert "-y" in inv.argv
    assert "--exit-immediately" in inv.argv
    assert inv.argv[inv.argv.index("-m") + 1] == "openai/qwen-coder"
    assert inv.env["MSWEA_CONFIGURED"] == "true"
    assert inv.env["MSWEA_COST_TRACKING"] == "ignore_errors"
    assert inv.env["MSWEA_ENV_TYPE"] == "local"
    assert inv.env["OPENAI_API_BASE"] == "http://127.0.0.1:18080/v1"

    traj_json = {
        "info": {"exit_status": "Submitted"},
        "messages": [
            {"role": "user", "content": "Review PR"},
            {"role": "assistant", "content": "```json\n" + json.dumps(sample_report_dict()) + "\n```"},
        ],
    }
    (out_dir / "mini.traj.json").write_text(json.dumps(traj_json), encoding="utf-8")
    raw_det = adapter.collect(task, ctx, ProcessResult(exit_code=0, timed_out=False, log_path=out_dir / "harness.log"))
    assert raw_det.kind == "json"
    parsed = adapter.extract(raw_det, task)
    assert parsed is not None
    assert len(parsed.findings) == 1
    assert parsed.findings[0].file == "pkg/queue.go"

    prose_traj = {
        "messages": [
            {"role": "assistant", "content": "Found slice panic in pkg/queue.go at line 4 when empty."},
        ]
    }
    (out_dir / "mini.traj.json").write_text(json.dumps(prose_traj), encoding="utf-8")
    raw_md = adapter.collect(task, ctx, ProcessResult(exit_code=0, timed_out=False, log_path=out_dir / "harness.log"))
    assert raw_md.kind == "markdown"
    assert "pkg/queue.go" in raw_md.text
    assert adapter.extract(raw_md, task) is None


def test_opencode_prepare_json_stream_extract_and_fallback(tmp_path: Path) -> None:
    adapter = OpenCodeAdapter()
    assert adapter.cache_shape == "append_only"
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True)
    task = review_pb2.AgentTask(
        src_dir=str(tmp_path / "src"),
        out_dir=str(out_dir),
        model_alias="qwen-coder",
    )
    task.budget.max_context_tokens = 65536
    ctx = make_ctx(tmp_path)
    inv = adapter.prepare(task, ctx)

    assert inv.argv[:5] == ["opencode", "run", "--format", "json", "--model"]
    assert inv.argv[5] == "review/qwen-coder"
    cfg_path = Path(inv.env["OPENCODE_CONFIG"])
    assert cfg_path.is_file()
    raw_cfg = cfg_path.read_text(encoding="utf-8")
    assert "eyJ.m2.secret.token" not in raw_cfg
    cfg = json.loads(raw_cfg)
    assert cfg["autoupdate"] is False
    assert cfg["permission"]["edit"] == "deny"
    assert cfg["permission"]["webfetch"] == "deny"
    assert cfg["provider"]["review"]["npm"] == "@ai-sdk/openai-compatible"
    assert cfg["provider"]["review"]["options"]["apiKey"] == "{env:OPENAI_API_KEY}"

    report_str = json.dumps(sample_report_dict())
    mid = len(report_str) // 2
    stream_lines = [
        json.dumps({"type": "step_start", "sessionID": "s1"}),
        json.dumps({"type": "text", "part": {"type": "text", "text": report_str[:mid]}}),
        json.dumps({"type": "text", "part": {"type": "text", "text": report_str[mid:]}}),
        json.dumps({"type": "step_finish", "sessionID": "s1"}),
    ]
    log_path = out_dir / "harness.log"
    log_path.write_text("\n".join(stream_lines) + "\n", encoding="utf-8")

    raw = adapter.collect(task, ctx, ProcessResult(exit_code=0, timed_out=False, log_path=log_path))
    assert raw.kind == "json"
    extracted = adapter.extract(raw, task)
    assert extracted is not None
    assert len(extracted.findings) == 1
    assert extracted.findings[0].title == "Panic on empty slice pop"

    prose_stream = "\n".join(
        [
            json.dumps({"type": "text", "part": {"type": "text", "text": "Bug in pkg/queue.go line 4: empty slice."}}),
            json.dumps({"type": "step_finish"}),
        ]
    )
    log_path.write_text(prose_stream + "\n", encoding="utf-8")
    raw_fallback = adapter.collect(task, ctx, ProcessResult(exit_code=0, timed_out=False, log_path=log_path))
    assert adapter.extract(raw_fallback, task) is None


def test_goose_prepare_recipe_and_extract(tmp_path: Path) -> None:
    adapter = GooseAdapter()
    assert adapter.cache_shape == "append_only"
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True)
    task = review_pb2.AgentTask(
        src_dir=str(tmp_path / "src"),
        out_dir=str(out_dir),
        model_alias="qwen-coder",
    )
    ctx = make_ctx(tmp_path)
    inv = adapter.prepare(task, ctx)

    assert inv.argv[:3] == ["goose", "run", "--no-session"]
    assert "--recipe" in inv.argv
    recipe_path = Path(inv.argv[inv.argv.index("--recipe") + 1])
    recipe_raw = recipe_path.read_text(encoding="utf-8")
    assert "eyJ.m2.secret.token" not in recipe_raw
    recipe_obj = yaml.safe_load(recipe_raw)
    assert recipe_obj["prompt"] == ctx.prompt
    assert "findings" in recipe_obj["response"]["json_schema"]["properties"]

    assert inv.env["GOOSE_PROVIDER"] == "openai"
    assert inv.env["GOOSE_MODE"] == "auto"
    assert inv.env["GOOSE_DISABLE_KEYRING"] == "1"
    assert inv.env["OPENAI_HOST"] == "http://127.0.0.1:18080"
    assert inv.env["OPENAI_BASE_PATH"] == "v1/chat/completions"

    log_path = out_dir / "harness.log"
    log_path.write_text(
        "starting session | provider: openai model: qwen-coder\n"
        "--- shell: rg Pop pkg/ ---\n"
        + json.dumps(sample_report_dict(), indent=2)
        + "\n",
        encoding="utf-8",
    )
    raw = adapter.collect(task, ctx, ProcessResult(exit_code=0, timed_out=False, log_path=log_path))
    extracted = adapter.extract(raw, task)
    assert extracted is not None
    assert len(extracted.findings) == 1
    assert extracted.findings[0].file == "pkg/queue.go"


def test_aider_prepare_read_only_flags_and_large_prompt_fallback(tmp_path: Path) -> None:
    adapter = AiderAdapter()
    assert adapter.cache_shape == "single_shot"
    src_dir = tmp_path / "src"
    (src_dir / "pkg").mkdir(parents=True)
    (src_dir / "pkg" / "queue.go").write_text("package pkg\n", encoding="utf-8")
    diff_file = tmp_path / "diff.patch"
    diff_file.write_text(
        "--- a/pkg/queue.go\n+++ b/pkg/queue.go\n@@ -1 +1,2 @@\n+// changed\n"
        "--- a/pkg/deleted.go\n+++ /dev/null\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True)

    task = review_pb2.AgentTask(
        src_dir=str(src_dir),
        diff_file=str(diff_file),
        out_dir=str(out_dir),
        model_alias="qwen-coder",
    )
    ctx = make_ctx(tmp_path)
    inv = adapter.prepare(task, ctx)

    assert inv.argv[0] == "aider"
    for expected_flag in (
        "--chat-mode",
        "ask",
        "--no-auto-commits",
        "--no-git",
        "--yes-always",
        "--no-check-update",
        "--analytics-disable",
        "--message",
    ):
        assert expected_flag in inv.argv
    assert "--read" in inv.argv
    assert inv.argv[inv.argv.index("--read") + 1] == "pkg/queue.go"
    history_arg = inv.argv[inv.argv.index("--chat-history-file") + 1]
    assert Path(history_arg).is_relative_to(ctx.home)

    huge_ctx = make_ctx(tmp_path, prompt="x" * 110_000)
    huge_inv = adapter.prepare(task, huge_ctx)
    assert "--message-file" in huge_inv.argv
    assert "--message" not in huge_inv.argv

    log_path = out_dir / "harness.log"
    log_path.write_text("In `pkg/queue.go` line 4, slicing an empty queue panics.", encoding="utf-8")
    raw = adapter.collect(task, ctx, ProcessResult(exit_code=0, timed_out=False, log_path=log_path))
    assert raw.kind == "markdown"
    assert adapter.extract(raw, task) is None


def write_fake_cli_script(path: Path, body: str) -> Path:
    script = f"\x23!{sys.executable}\n" + body
    path.write_text(script, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


def test_entrypoint_dispatches_all_four_new_adapters_end_to_end(tmp_path: Path) -> None:
    report_payload = sample_report_dict()

    with MockNormalizeHTTPServer(report_payload) as srv:
        cases = [
            (
                "mini-swe-agent",
                (
                    "import json, sys\n"
                    "out_idx = sys.argv.index('-o') + 1\n"
                    f"traj = {{'messages': [{{'role': 'assistant', 'content': {json.dumps(json.dumps(report_payload))}}}]}}\n"
                    "open(sys.argv[out_idx], 'w', encoding='utf-8').write(json.dumps(traj))\n"
                ),
                review_pb2.NORMALIZER_PATH_DETERMINISTIC,
            ),
            (
                "opencode",
                (
                    "import json, sys\n"
                    f"event = {{'type': 'text', 'part': {{'type': 'text', 'text': {json.dumps(json.dumps(report_payload))}}}}}\n"
                    "sys.stdout.write(json.dumps(event) + '\\n')\n"
                ),
                review_pb2.NORMALIZER_PATH_DETERMINISTIC,
            ),
            (
                "goose",
                (
                    "import sys\n"
                    f"sys.stdout.write('goose session ready\\n' + {json.dumps(json.dumps(report_payload))} + '\\n')\n"
                ),
                review_pb2.NORMALIZER_PATH_DETERMINISTIC,
            ),
            (
                "aider",
                (
                    "import sys\n"
                    "assert '--no-auto-commits' in sys.argv and '--no-git' in sys.argv and '--yes-always' in sys.argv\n"
                    "sys.stdout.write('Defect in pkg/queue.go line 4 (`items = items[1:]`): panics when empty.\\n')\n"
                ),
                review_pb2.NORMALIZER_PATH_LLM,
            ),
        ]

        for adapter_name, script_body, expected_norm_path in cases:
            case_dir = tmp_path / adapter_name
            src_dir = case_dir / "src"
            in_dir = case_dir / ".review" / "in"
            out_dir = case_dir / ".review" / "out"
            bin_dir = case_dir / "bin"
            for d in (src_dir / "pkg", in_dir, out_dir, bin_dir):
                d.mkdir(parents=True, exist_ok=True)

            (src_dir / "pkg" / "queue.go").write_text(
                "package pkg\n\nfunc Pop(items []int) []int {\n\titems = items[1:]\n\treturn items\n}\n",
                encoding="utf-8",
            )
            (in_dir / "diff.patch").write_text(
                "--- a/pkg/queue.go\n+++ b/pkg/queue.go\n@@ -1,2 +1,6 @@\n+\titems = items[1:]\n",
                encoding="utf-8",
            )
            fake_bin = write_fake_cli_script(bin_dir / f"fake-{adapter_name}", script_body)

            task = review_pb2.AgentTask(
                run_id=f"run-{adapter_name}",
                shard_index=0,
                model_alias="qwen-coder",
                pr_title="Add Pop helper",
                src_dir=str(src_dir),
                diff_file=str(in_dir / "diff.patch"),
                out_dir=str(out_dir),
            )
            task.harness.profile = adapter_name
            task.harness.adapter = adapter_name
            task.harness.options["bin"] = str(fake_bin)
            task.budget.wall_clock_seconds = 20

            task_file = in_dir / "task.json"
            task_file.write_text(json_format.MessageToJson(task), encoding="utf-8")

            stdin_stream = io.BytesIO(
                json.dumps({"openai_base_url": srv.base_url, "openai_api_key": "eyJ.secret.m2"}).encode()
            )
            rc = run_command(
                stdin_stream,
                ["--task", str(task_file), "--scratch-dir", str(case_dir / "scratch")],
            )
            assert rc == 0

            report = json_format.Parse(
                (out_dir / "report.json").read_text(encoding="utf-8"),
                review_pb2.ReviewReport(),
            )
            assert report.provenance.status == review_pb2.REPORT_STATUS_OK
            assert report.provenance.profile == adapter_name
            assert report.provenance.normalizer == expected_norm_path
            assert len(report.findings) == 1
            assert report.findings[0].file == "pkg/queue.go"
            assert report.findings[0].target_line_text == "items = items[1:]"

        assert srv.normalize_calls == 1
