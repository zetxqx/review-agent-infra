from __future__ import annotations

from pathlib import Path

from review_harness_kit.contracts import HarnessInvocation, ProcessResult, RawOutput, RunContext
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import parse_report_json


def render_codex_config(model: str, base_url: str, context_window: int) -> str:
    return "\n".join(
        [
            f'model = "{model}"',
            'model_provider = "review"',
            'approval_policy = "never"',
            'sandbox_mode = "danger-full-access"',
            "project_doc_max_bytes = 0",
            f"model_context_window = {context_window}",
            "",
            "[model_providers.review]",
            'name = "review-adapter"',
            f'base_url = "{base_url}"',
            'env_key = "OPENAI_API_KEY"',
            'wire_api = "responses"',
            "stream_idle_timeout_ms = 400000",
            "",
        ]
    )


class CodexAdapter:
    name = "codex"
    cache_shape = "append_only"

    def prepare(self, task: review_pb2.AgentTask, ctx: RunContext) -> HarnessInvocation:
        codex_home = ctx.home / ".codex"
        codex_home.mkdir(parents=True, exist_ok=True)
        max_ctx = task.budget.max_context_tokens if task.budget.max_context_tokens > 0 else 32768
        (codex_home / "config.toml").write_text(
            render_codex_config(
                model=task.model_alias or "default",
                base_url=ctx.base_url,
                context_window=max_ctx,
            ),
            encoding="utf-8",
        )
        binary = task.harness.options.get("bin") or "codex"
        out_path = Path(task.out_dir or "/workspace/.review/out") / "raw_report.json"
        argv = [
            binary,
            "exec",
            "--skip-git-repo-check",
            "--output-schema",
            str(ctx.schema_path),
            "--output-last-message",
            str(out_path),
            "-",
        ]
        return HarnessInvocation(
            argv=argv,
            cwd=task.src_dir or "/workspace/src",
            env={"CODEX_HOME": str(codex_home)},
            stdin_text=ctx.prompt,
        )

    def collect(self, task: review_pb2.AgentTask, ctx: RunContext, result: ProcessResult) -> RawOutput:
        out_path = Path(task.out_dir or "/workspace/.review/out") / "raw_report.json"
        if out_path.is_file():
            content = out_path.read_text(encoding="utf-8", errors="replace")
            if content.strip():
                return RawOutput(kind="json", text=content)
        log_text = (
            result.log_path.read_text(encoding="utf-8", errors="replace")[-60_000:]
            if result.log_path.is_file()
            else ""
        )
        return RawOutput(kind="text", text=log_text)

    def extract(self, raw: RawOutput, task: review_pb2.AgentTask) -> review_pb2.ReviewReport | None:
        if raw.kind != "json" or not raw.text.strip():
            return None
        return parse_report_json(raw.text)
