from __future__ import annotations

import json
import shlex
import sys
from pathlib import Path

from review_harness_kit.adapters.pr_agent import PRAgentAdapter
from review_harness_kit.contracts import (
    CacheShape,
    HarnessInvocation,
    ProcessResult,
    RawOutput,
    RunContext,
)
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import parse_report_json

OPENAI_PROBE_RUNNER = Path(__file__).resolve().parent.parent / "runners" / "openai_probe.py"


class CommandAdapter:
    name: str = "command"
    cache_shape: CacheShape = "append_only"
    default_mode: str = "sse"

    def prepare(self, task: review_pb2.AgentTask, ctx: RunContext) -> HarnessInvocation:
        prompt_file = ctx.home / "review_task.md"
        prompt_file.write_text(ctx.prompt, encoding="utf-8")
        out_dir = task.out_dir or "/workspace/.review/out"
        src_dir = task.src_dir or "/workspace/src"
        env = {
            "REVIEW_PROMPT_FILE": str(prompt_file),
            "REVIEW_DIFF_FILE": task.diff_file,
            "REVIEW_SRC_DIR": src_dir,
            "REVIEW_OUT_DIR": out_dir,
            "REVIEW_SCHEMA_PATH": str(ctx.schema_path),
            "REVIEW_MODEL": task.model_alias or "default",
        }
        options = dict(task.harness.options)
        if options.get("argv"):
            parsed_argv = json.loads(options["argv"])
            if not isinstance(parsed_argv, list) or not all(isinstance(x, str) for x in parsed_argv):
                raise ValueError("harness option 'argv' must be a JSON array of strings")
            argv = parsed_argv
        elif options.get("command"):
            argv = shlex.split(options["command"])
        else:
            mode = options.get("mode") or self.default_mode
            default_turns = "2" if mode == "sse" else "1"
            turns = options.get("turns") or default_turns
            argv = [
                sys.executable,
                str(OPENAI_PROBE_RUNNER),
                "--mode",
                mode,
                "--turns",
                turns,
                "--model",
                task.model_alias or "default",
                "--prompt-file",
                str(prompt_file),
                "--out-dir",
                out_dir,
                "--schema-path",
                str(ctx.schema_path),
            ]
        return HarnessInvocation(
            argv=argv,
            cwd=src_dir,
            env=env,
            stdin_text=options.get("stdin"),
        )

    def collect(self, task: review_pb2.AgentTask, ctx: RunContext, result: ProcessResult) -> RawOutput:
        out = Path(task.out_dir or "/workspace/.review/out")
        json_path = out / "raw_report.json"
        if json_path.is_file():
            content = json_path.read_text(encoding="utf-8", errors="replace")
            if content.strip():
                return RawOutput(kind="json", text=content)
        yaml_path = out / "pr_agent_prediction.yaml"
        if yaml_path.is_file():
            content = yaml_path.read_text(encoding="utf-8", errors="replace")
            if content.strip():
                return RawOutput(kind="yaml", text=content)
        log_text = (
            result.log_path.read_text(encoding="utf-8", errors="replace")[-60_000:]
            if result.log_path.is_file()
            else ""
        )
        return RawOutput(kind="markdown", text=log_text)

    def extract(self, raw: RawOutput, task: review_pb2.AgentTask) -> review_pb2.ReviewReport | None:
        if raw.kind == "json" and raw.text.strip():
            return parse_report_json(raw.text)
        if raw.kind == "yaml" and raw.text.strip():
            return PRAgentAdapter().extract(raw, task)
        return None


class MockLLMAdapter(CommandAdapter):
    name: str = "mock-llm"
    cache_shape: CacheShape = "append_only"
    default_mode: str = "sse"


class SingleShotAdapter(CommandAdapter):
    name: str = "single-shot"
    cache_shape: CacheShape = "single_shot"
    default_mode: str = "deterministic"
