from __future__ import annotations

import json
from pathlib import Path

from review_harness_kit.contracts import HarnessInvocation, ProcessResult, RawOutput, RunContext
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import parse_report_json


def _extract_jsonl_agent_messages(log_text: str) -> str:
    messages: list[str] = []
    for line in log_text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            ev = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if not isinstance(ev, dict):
            continue
        if ev.get("kind") != "MessageEvent" or ev.get("source") != "agent":
            continue
        llm_msg = ev.get("llm_message")
        if not isinstance(llm_msg, dict):
            continue
        content = llm_msg.get("content")
        if not isinstance(content, list):
            continue
        parts: list[str] = []
        for part in content:
            if isinstance(part, dict) and isinstance(part.get("text"), str) and part["text"].strip():
                parts.append(part["text"])
        if parts:
            messages.append("\n".join(parts))
    return "\n\n".join(messages)


class OpenHandsAdapter:
    name = "openhands"
    cache_shape = "append_only"

    def prepare(self, task: review_pb2.AgentTask, ctx: RunContext) -> HarnessInvocation:
        oh_dir = ctx.home / ".openhands"
        oh_dir.mkdir(parents=True, exist_ok=True)
        hirc_file = ctx.home / ".hirc"
        if not hirc_file.exists():
            hirc_file.write_text("user_consents_to_publishing_history: false\n", encoding="utf-8")
        task_file = ctx.home / "review_task.md"
        task_file.write_text(ctx.prompt, encoding="utf-8")
        cwd = task.src_dir or "/workspace/src"
        env = {
            "LLM_MODEL": f"openai/{task.model_alias or 'default'}",
            "LLM_BASE_URL": ctx.base_url,
            "LLM_API_KEY": ctx.api_key,
            "LLM_NUM_RETRIES": "0",
            "OPENHANDS_PERSISTENCE_DIR": str(oh_dir),
            "OPENHANDS_WORK_DIR": str(cwd),
            "OPENHANDS_SUPPRESS_BANNER": "1",
            "LITELLM_LOCAL_MODEL_COST_MAP": "True",
            "DO_NOT_TRACK": "1",
            "__hi_imported": "true",
            "SANDBOX_TYPE": "local",
        }
        binary = task.harness.options.get("bin") or "openhands"
        argv = [
            binary,
            "--headless",
            "--override-with-envs",
            "--always-approve",
            "--json",
            "-f",
            str(task_file),
        ]
        return HarnessInvocation(argv=argv, cwd=cwd, env=env)

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
            result.log_path.read_text(encoding="utf-8", errors="replace")
            if result.log_path.is_file()
            else ""
        )
        extracted_messages = _extract_jsonl_agent_messages(log_text)
        if extracted_messages:
            return RawOutput(kind="markdown", text=extracted_messages[-60_000:])
        return RawOutput(kind="markdown", text=log_text[-60_000:])

    def extract(self, raw: RawOutput, task: review_pb2.AgentTask) -> review_pb2.ReviewReport | None:
        if raw.kind != "json" or not raw.text.strip():
            return None
        return parse_report_json(raw.text)
