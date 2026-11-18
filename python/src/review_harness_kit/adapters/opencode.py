from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from review_harness_kit.contracts import HarnessInvocation, ProcessResult, RawOutput, RunContext
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import parse_report_json, strip_code_fence


def find_embedded_report_json(text: str) -> review_pb2.ReviewReport | None:
    stripped = strip_code_fence(text)
    candidates = [stripped]
    if "```" in text:
        segments = text.split("```")
        for idx in range(1, len(segments), 2):
            block = segments[idx].strip()
            if "\n" in block and block.splitlines()[0].strip().isalpha():
                block = "\n".join(block.splitlines()[1:]).strip()
            candidates.append(block)

    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if 0 <= first_brace < last_brace:
        candidates.append(text[first_brace : last_brace + 1])

    for candidate in candidates:
        if not candidate:
            continue
        try:
            obj = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and isinstance(obj.get("findings"), list):
            return parse_report_json(candidate)
    return None


def extract_text_from_opencode_event(event: dict[str, Any]) -> str:
    part = event.get("part")
    if isinstance(part, dict):
        part_text = part.get("text")
        if isinstance(part_text, str) and part_text:
            return part_text
    for key in ("text", "content", "output"):
        val = event.get(key)
        if isinstance(val, str) and val:
            return val
    message = event.get("message")
    if isinstance(message, dict):
        msg_content = message.get("content")
        if isinstance(msg_content, str) and msg_content:
            return msg_content
    return ""


def build_opencode_config(base_url: str, model_alias: str, context_window: int) -> dict[str, Any]:
    return {
        "$schema": "https://opencode.ai/config.json",
        "autoupdate": False,
        "share": "disabled",
        "permission": {
            "edit": "deny",
            "webfetch": "deny",
            "bash": "allow",
        },
        "provider": {
            "review": {
                "npm": "@ai-sdk/openai-compatible",
                "name": "review-adapter",
                "options": {
                    "baseURL": base_url,
                    "apiKey": "{env:OPENAI_API_KEY}",
                },
                "models": {
                    model_alias: {
                        "name": model_alias,
                        "limit": {
                            "context": context_window,
                            "output": 4096,
                        },
                    }
                },
            }
        },
    }


class OpenCodeAdapter:
    name = "opencode"
    cache_shape = "append_only"

    def prepare(self, task: review_pb2.AgentTask, ctx: RunContext) -> HarnessInvocation:
        config_dir = ctx.home / ".config" / "opencode"
        config_dir.mkdir(parents=True, exist_ok=True)
        model_alias = task.model_alias or "default"
        max_ctx = task.budget.max_context_tokens if task.budget.max_context_tokens > 0 else 32768
        config_path = config_dir / "opencode.json"
        config_path.write_text(
            json.dumps(build_opencode_config(ctx.base_url, model_alias, max_ctx), indent=2) + "\n",
            encoding="utf-8",
        )

        env = {
            "OPENCODE_CONFIG": str(config_path),
            "OPENCODE_CONFIG_DIR": str(config_dir),
            "OPENCODE_DISABLE_AUTOUPDATE": "true",
        }
        binary = task.harness.options.get("bin") or "opencode"
        argv = [
            binary,
            "run",
            "--format",
            "json",
            "--model",
            f"review/{model_alias}",
            ctx.prompt,
        ]
        return HarnessInvocation(argv=argv, cwd=task.src_dir or "/workspace/src", env=env)

    def collect(self, task: review_pb2.AgentTask, ctx: RunContext, result: ProcessResult) -> RawOutput:
        out = Path(task.out_dir or "/workspace/.review/out")
        raw_json = out / "raw_report.json"
        if raw_json.is_file():
            content = raw_json.read_text(encoding="utf-8", errors="replace")
            if content.strip():
                return RawOutput(kind="json", text=content)

        if not result.log_path.is_file():
            return RawOutput(kind="text", text="")
        log_text = result.log_path.read_text(encoding="utf-8", errors="replace")[-60_000:]
        if not log_text.strip():
            return RawOutput(kind="text", text="")
        return RawOutput(kind="json", text=log_text)

    def extract(self, raw: RawOutput, task: review_pb2.AgentTask) -> review_pb2.ReviewReport | None:
        if raw.kind != "json" or not raw.text.strip():
            return None

        direct = find_embedded_report_json(raw.text)
        if direct is not None:
            return direct

        fragments: list[str] = []
        for raw_line in raw.text.splitlines():
            line = raw_line.strip()
            if not line.startswith("{"):
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(event, dict):
                continue
            if isinstance(event.get("findings"), list):
                return parse_report_json(line)
            piece = extract_text_from_opencode_event(event)
            if piece:
                fragments.append(piece)

        if not fragments:
            return None
        return find_embedded_report_json("".join(fragments))
