from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from review_harness_kit.contracts import HarnessInvocation, ProcessResult, RawOutput, RunContext
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import parse_report_json, strip_code_fence


def extract_trajectory_output(traj_text: str) -> RawOutput:
    try:
        data = json.loads(traj_text)
    except json.JSONDecodeError:
        return RawOutput(kind="markdown", text=traj_text[-60_000:])
    if not isinstance(data, dict):
        return RawOutput(kind="markdown", text=traj_text[-60_000:])

    if "findings" in data and isinstance(data.get("findings"), list):
        return RawOutput(kind="json", text=traj_text)

    candidates: list[str] = []
    info = data.get("info")
    if isinstance(info, dict):
        submission = info.get("submission")
        if isinstance(submission, str) and submission.strip():
            candidates.append(submission.strip())

    messages = data.get("messages")
    if isinstance(messages, list):
        for msg in reversed(messages):
            if not isinstance(msg, dict) or msg.get("role") != "assistant":
                continue
            content: Any = msg.get("content")
            if isinstance(content, str) and content.strip():
                candidates.append(content.strip())
                break

    for candidate in candidates:
        unwrapped = strip_code_fence(candidate)
        try:
            parsed = json.loads(unwrapped)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and isinstance(parsed.get("findings"), list):
            return RawOutput(kind="json", text=unwrapped)

    if candidates:
        return RawOutput(kind="markdown", text="\n\n".join(candidates)[-60_000:])
    return RawOutput(kind="markdown", text=traj_text[-60_000:])


class MiniSWEAgentAdapter:
    name = "mini-swe-agent"
    cache_shape = "append_only"

    def prepare(self, task: review_pb2.AgentTask, ctx: RunContext) -> HarnessInvocation:
        out = Path(task.out_dir or "/workspace/.review/out")
        mswea_dir = ctx.home / ".config" / "mini-swe-agent"
        mswea_dir.mkdir(parents=True, exist_ok=True)
        prompt_file = ctx.home / "review_task.md"
        prompt_file.write_text(ctx.prompt, encoding="utf-8")

        model_name = f"openai/{task.model_alias or 'default'}"
        traj_path = out / "mini.traj.json"
        env = {
            "MSWEA_CONFIGURED": "true",
            "MSWEA_COST_TRACKING": "ignore_errors",
            "MSWEA_MINI_CONFIG_PATH": str(mswea_dir),
            "MSWEA_ENV_TYPE": "local",
            "LITELLM_API_BASE": ctx.base_url,
            "OPENAI_API_BASE": ctx.base_url,
            "OPENAI_API_KEY": ctx.api_key,
        }
        binary = task.harness.options.get("bin") or "mini"
        argv = [
            binary,
            "-t",
            ctx.prompt,
            "-m",
            model_name,
            "-y",
            "--exit-immediately",
            "-o",
            str(traj_path),
        ]
        return HarnessInvocation(argv=argv, cwd=task.src_dir or "/workspace/src", env=env)

    def collect(self, task: review_pb2.AgentTask, ctx: RunContext, result: ProcessResult) -> RawOutput:
        out = Path(task.out_dir or "/workspace/.review/out")
        raw_json = out / "raw_report.json"
        if raw_json.is_file():
            content = raw_json.read_text(encoding="utf-8", errors="replace")
            if content.strip():
                return RawOutput(kind="json", text=content)

        traj_path = out / "mini.traj.json"
        if traj_path.is_file():
            traj_text = traj_path.read_text(encoding="utf-8", errors="replace")
            if traj_text.strip():
                return extract_trajectory_output(traj_text)

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
