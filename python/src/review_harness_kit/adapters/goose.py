from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit

import yaml

from review_harness_kit.adapters.opencode import find_embedded_report_json
from review_harness_kit.contracts import HarnessInvocation, ProcessResult, RawOutput, RunContext
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import load_report_schema


def split_goose_openai_endpoint(base_url: str) -> tuple[str, str]:
    parts = urlsplit(base_url.strip())
    if not parts.scheme or not parts.netloc:
        return base_url.strip(), "v1/chat/completions"
    host = f"{parts.scheme}://{parts.netloc}"
    prefix = parts.path.strip("/") or "v1"
    if prefix.endswith("chat/completions"):
        return host, prefix
    return host, f"{prefix}/chat/completions"


class GooseAdapter:
    name = "goose"
    cache_shape = "append_only"

    def prepare(self, task: review_pb2.AgentTask, ctx: RunContext) -> HarnessInvocation:
        goose_dir = ctx.home / ".config" / "goose"
        goose_dir.mkdir(parents=True, exist_ok=True)
        recipe_path = goose_dir / "review_recipe.yaml"
        schema_obj = load_report_schema(ctx.schema_path)
        recipe = {
            "version": "1.0.0",
            "title": "pull-request-code-review",
            "description": "Read-only pull request defect review.",
            "instructions": (
                "Inspect the pull request diff on the read-only source tree without modifying any files "
                "and emit findings strictly matching the response schema."
            ),
            "prompt": ctx.prompt,
            "response": {
                "json_schema": schema_obj,
            },
        }
        recipe_path.write_text(yaml.safe_dump(recipe, sort_keys=False), encoding="utf-8")

        openai_host, openai_base_path = split_goose_openai_endpoint(ctx.base_url)
        env = {
            "GOOSE_PROVIDER": "openai",
            "GOOSE_MODEL": task.model_alias or "default",
            "GOOSE_MODE": "auto",
            "GOOSE_DISABLE_KEYRING": "1",
            "GOOSE_TELEMETRY_OFF": "true",
            "OPENAI_HOST": openai_host,
            "OPENAI_BASE_PATH": openai_base_path,
            "OPENAI_API_KEY": ctx.api_key,
        }
        binary = task.harness.options.get("bin") or "goose"
        argv = [
            binary,
            "run",
            "--no-session",
            "--recipe",
            str(recipe_path),
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
        decoder = json.JSONDecoder()
        text = raw.text
        for idx, ch in enumerate(text):
            if ch != "{":
                continue
            try:
                obj, _ = decoder.raw_decode(text[idx:])
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict) and isinstance(obj.get("findings"), list):
                return find_embedded_report_json(json.dumps(obj))
        return None
