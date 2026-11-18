from __future__ import annotations

from pathlib import Path

from review_harness_kit.contracts import HarnessInvocation, ProcessResult, RawOutput, RunContext
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import parse_report_json


class OpenHandsAdapter:
    name = "openhands"
    cache_shape = "append_only"

    def prepare(self, task: review_pb2.AgentTask, ctx: RunContext) -> HarnessInvocation:
        oh_dir = ctx.home / ".openhands"
        oh_dir.mkdir(parents=True, exist_ok=True)
        task_file = ctx.home / "review_task.md"
        task_file.write_text(ctx.prompt, encoding="utf-8")
        env = {
            "LLM_MODEL": f"openai/{task.model_alias or 'default'}",
            "LLM_BASE_URL": ctx.base_url,
            "LLM_API_KEY": ctx.api_key,
            "LLM_NUM_RETRIES": "0",
            "OPENHANDS_PERSISTENCE_DIR": str(oh_dir),
            "SANDBOX_TYPE": "local",
        }
        binary = task.harness.options.get("bin") or "openhands"
        argv = [binary, "--headless", "--override-with-envs", "-f", str(task_file)]
        return HarnessInvocation(argv=argv, cwd=task.src_dir or "/workspace/src", env=env)

    def collect(self, task: review_pb2.AgentTask, ctx: RunContext, result: ProcessResult) -> RawOutput:
        out = Path(task.out_dir or "/workspace/.review/out")
        json_path = out / "raw_report.json"
        if json_path.is_file():
            content = json_path.read_text(encoding="utf-8", errors="replace")
            if content.strip():
                return RawOutput(kind="json", text=content)
        md_path = out / "raw_report.md"
        if md_path.is_file():
            content = md_path.read_text(encoding="utf-8", errors="replace")
            if content.strip():
                return RawOutput(kind="markdown", text=content)
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
