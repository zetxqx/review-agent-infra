from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from review_harness_kit.adapters.codex import CodexAdapter
from review_harness_kit.adapters.command import CommandAdapter, MockLLMAdapter, SingleShotAdapter
from review_harness_kit.adapters.openhands import OpenHandsAdapter
from review_harness_kit.adapters.pr_agent import PRAgentAdapter
from review_harness_kit.contracts import (
    HarnessUnsupportedError,
    ProcessResult,
    RawOutput,
    RunContext,
)
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import EMBEDDED_SCHEMA_PATH


def make_run_context(tmp_path: Path) -> RunContext:
    home = tmp_path / "scratch" / "home"
    home.mkdir(parents=True, exist_ok=True)
    return RunContext(
        base_url="http://127.0.0.1:18080/v1",
        api_key="eyJ.test.secret",
        home=home,
        prompt="Review the PR diff carefully.",
        rules_text="No comments in implementation code.",
        schema_path=EMBEDDED_SCHEMA_PATH,
    )


def test_pr_agent_requires_base_ref(tmp_path: Path) -> None:
    adapter = PRAgentAdapter()
    task = review_pb2.AgentTask(
        src_dir=str(tmp_path / "src"),
        out_dir=str(tmp_path / "out"),
        model_alias="qwen-coder",
    )
    task.git.head_ref = "pr"
    with pytest.raises(HarnessUnsupportedError):
        adapter.prepare(task, make_run_context(tmp_path))


def test_pr_agent_prepare_and_deterministic_yaml_extract(tmp_path: Path) -> None:
    adapter = PRAgentAdapter()
    task = review_pb2.AgentTask(
        src_dir=str(tmp_path / "src"),
        out_dir=str(tmp_path / "out"),
        model_alias="qwen-coder",
    )
    task.git.base_ref = "base"
    task.git.head_ref = "pr"
    task.budget.max_context_tokens = 65536

    inv = adapter.prepare(task, make_run_context(tmp_path))
    assert inv.env["CONFIG__GIT_PROVIDER"] == "local"
    assert inv.env["CONFIG__FALLBACK_MODELS"] == "[]"
    assert inv.env["CONFIG__NUM_RETRIES"] == "0"
    assert inv.env["CONFIG__USE_REPO_SETTINGS_FILE"] == "false"
    assert inv.env["OPENAI__API_BASE"] == "http://127.0.0.1:18080/v1"
    assert inv.env["OPENAI__KEY"] == "eyJ.test.secret"

    payload = {
        "review": {
            "security_concerns": "Token written to world-readable log file.",
            "key_issues_to_review": [
                {
                    "relevant_file": "internal/auth/verify.go",
                    "end_line": 42,
                    "severity": "high",
                    "issue_header": "Data race on shared map",
                    "issue_content": "Concurrent map writes without mutex protection.",
                    "improved_code": "mu.Lock()\ndefer mu.Unlock()",
                }
            ],
        }
    }
    fenced_yaml = "```yaml\n" + yaml.safe_dump(payload) + "```\n"
    report = adapter.extract(RawOutput(kind="yaml", text=fenced_yaml), task)
    assert report is not None
    assert "Security concerns: Token written" in report.summary
    assert len(report.findings) == 1
    finding = report.findings[0]
    assert finding.file == "internal/auth/verify.go"
    assert finding.line == 42
    assert finding.side == review_pb2.DIFF_SIDE_RIGHT
    assert finding.anchor == review_pb2.ANCHOR_KIND_DERIVED
    assert finding.severity == review_pb2.SEVERITY_HIGH
    assert finding.category == review_pb2.FINDING_CATEGORY_CONCURRENCY
    assert not finding.HasField("confidence")


@pytest.mark.parametrize(("max_requests", "want_max_size"), [(0, 240), (60, 240), (120, 480)])
def test_openhands_prepare_persists_agent_settings_with_budget_sized_condenser(
    tmp_path: Path, max_requests: int, want_max_size: int
) -> None:
    task = review_pb2.AgentTask(src_dir=str(tmp_path / "src"), out_dir=str(tmp_path / "out"), model_alias="qwen-coder")
    task.budget.max_requests = max_requests
    ctx = make_run_context(tmp_path)

    inv = OpenHandsAdapter().prepare(task, ctx)

    settings_path = Path(inv.env["OPENHANDS_PERSISTENCE_DIR"]) / "agent_settings.json"
    settings = json.loads(settings_path.read_text(encoding="utf-8"))
    assert settings["kind"] == "Agent"
    assert settings["llm"]["model"] == "openai/qwen-coder"
    assert settings["condenser"]["kind"] == "LLMSummarizingCondenser"
    assert settings["condenser"]["max_size"] == want_max_size
    assert settings["condenser"]["llm"]["model"] == "openai/qwen-coder"
    assert ctx.api_key not in settings_path.read_text(encoding="utf-8")


def test_openhands_prepare_collect_and_fallback(tmp_path: Path) -> None:
    adapter = OpenHandsAdapter()
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True)
    log_path = out_dir / "harness.log"
    log_path.write_text("Found bug in pkg/foo.go line 12: nil pointer dereference.", encoding="utf-8")

    task = review_pb2.AgentTask(
        src_dir=str(tmp_path / "src"),
        out_dir=str(out_dir),
        model_alias="qwen-coder",
    )
    ctx = make_run_context(tmp_path)
    inv = adapter.prepare(task, ctx)
    assert inv.argv[0] == "openhands"
    assert "--headless" in inv.argv
    assert "--always-approve" in inv.argv
    assert "--json" in inv.argv
    assert (ctx.home / "review_task.md").is_file()
    assert (ctx.home / ".hirc").is_file()
    assert inv.env["SANDBOX_TYPE"] == "local"
    assert inv.env["OPENHANDS_WORK_DIR"] == str(tmp_path / "src")
    assert inv.env["OPENHANDS_SUPPRESS_BANNER"] == "1"
    assert inv.env["__hi_imported"] == "true"

    raw = adapter.collect(task, ctx, ProcessResult(exit_code=0, timed_out=False, log_path=log_path))
    assert raw.kind == "markdown"
    assert "nil pointer dereference" in raw.text
    assert adapter.extract(raw, task) is None

    jsonl_log = out_dir / "jsonl.log"
    jsonl_log.write_text(
        "\n".join(
            [
                "Initializing agent...",
                json.dumps({"kind": "ActionEvent", "source": "agent", "tool_name": "file_editor"}),
                json.dumps(
                    {
                        "kind": "MessageEvent",
                        "source": "agent",
                        "llm_message": {
                            "role": "assistant",
                            "content": [{"type": "text", "text": "Bug in truncate.go line 9."}],
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    jsonl_raw = adapter.collect(task, ctx, ProcessResult(exit_code=0, timed_out=False, log_path=jsonl_log))
    assert jsonl_raw.kind == "markdown"
    assert jsonl_raw.text == "Bug in truncate.go line 9."


def test_codex_prepare_writes_secretless_config_and_extracts_json(tmp_path: Path) -> None:
    adapter = CodexAdapter()
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True)
    task = review_pb2.AgentTask(
        src_dir=str(tmp_path / "src"),
        out_dir=str(out_dir),
        model_alias="gpt-review",
    )
    ctx = make_run_context(tmp_path)
    inv = adapter.prepare(task, ctx)
    config_text = (ctx.home / ".codex" / "config.toml").read_text(encoding="utf-8")
    assert 'wire_api = "responses"' in config_text
    assert 'env_key = "OPENAI_API_KEY"' in config_text
    assert "eyJ.test.secret" not in config_text
    assert "--output-schema" in inv.argv
    assert inv.stdin_text == ctx.prompt

    (out_dir / "raw_report.json").write_text(
        json.dumps({"summary": "Clean PR.", "findings": []}),
        encoding="utf-8",
    )
    raw = adapter.collect(task, ctx, ProcessResult(exit_code=0, timed_out=False, log_path=out_dir / "harness.log"))
    assert raw.kind == "json"
    extracted = adapter.extract(raw, task)
    assert extracted is not None
    assert extracted.summary == "Clean PR."


def test_command_adapter_modes_and_custom_argv(tmp_path: Path) -> None:
    ctx = make_run_context(tmp_path)
    task = review_pb2.AgentTask(
        src_dir=str(tmp_path / "src"),
        out_dir=str(tmp_path / "out"),
        model_alias="qwen-coder",
    )
    sse_inv = MockLLMAdapter().prepare(task, ctx)
    assert "--mode" in sse_inv.argv
    assert sse_inv.argv[sse_inv.argv.index("--mode") + 1] == "sse"

    det_inv = SingleShotAdapter().prepare(task, ctx)
    assert det_inv.argv[det_inv.argv.index("--mode") + 1] == "deterministic"

    task.harness.options["argv"] = json.dumps(["echo", "custom-check"])
    custom_inv = CommandAdapter().prepare(task, ctx)
    assert custom_inv.argv == ["echo", "custom-check"]
