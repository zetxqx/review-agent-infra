from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from openai import DefaultHttpxClient, OpenAI


def stream_chat_turn(client: OpenAI, model: str, messages: list[dict[str, str]]) -> str:
    stream = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0,
        stream=True,
    )
    chunks: list[str] = []
    for event in stream:
        choices = getattr(event, "choices", None) or []
        if not choices:
            continue
        delta = getattr(choices[0], "delta", None)
        piece = getattr(delta, "content", None) if delta is not None else None
        if piece:
            chunks.append(piece)
    return "".join(chunks)


def run_sse_mode(client: OpenAI, model: str, prompt_text: str, turns: int) -> None:
    messages: list[dict[str, str]] = [
        {
            "role": "system",
            "content": (
                "You are a read-only code review assistant. Inspect the diff and report concrete "
                "correctness, concurrency, or security defects with file paths, line numbers, and "
                "verbatim target lines."
            ),
        },
        {"role": "user", "content": prompt_text},
    ]
    latest_text = stream_chat_turn(client, model, messages)
    for _ in range(max(0, turns - 1)):
        messages.append({"role": "assistant", "content": latest_text})
        messages.append(
            {
                "role": "user",
                "content": (
                    "Double-check every reported defect against the diff context, drop speculative nits, "
                    "and present the verified findings in concise markdown with exact file, line, and quoted code."
                ),
            }
        )
        latest_text = stream_chat_turn(client, model, messages)
    sys.stdout.write(latest_text)
    if not latest_text.endswith("\n"):
        sys.stdout.write("\n")
    sys.stdout.flush()


def run_deterministic_mode(
    client: OpenAI,
    model: str,
    prompt_text: str,
    schema: dict[str, Any],
    out_dir: Path,
) -> None:
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": "Review the pull request diff and emit a JSON ReviewReport matching the strict schema.",
            },
            {"role": "user", "content": prompt_text},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "ReviewReport", "strict": True, "schema": schema},
        },
    )
    content = resp.choices[0].message.content if resp.choices else "{}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "raw_report.json").write_text(content or "{}", encoding="utf-8")


def run_pr_agent_yaml_mode(client: OpenAI, model: str, prompt_text: str, out_dir: Path) -> None:
    resp = client.chat.completions.create(
        model=model,
        temperature=0,
        messages=[
            {
                "role": "system",
                "content": (
                    "Review the pull request diff and emit YAML under top-level key 'review' containing "
                    "'security_concerns' and 'key_issues_to_review' (each with relevant_file, end_line, "
                    "issue_header, issue_content)."
                ),
            },
            {"role": "user", "content": prompt_text},
        ],
    )
    content = resp.choices[0].message.content if resp.choices else ""
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "pr_agent_prediction.yaml").write_text(content or "", encoding="utf-8")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="openai_probe")
    parser.add_argument("--mode", default="sse")
    parser.add_argument("--turns", type=int, default=2)
    parser.add_argument("--model", default="default")
    parser.add_argument("--prompt-file", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--schema-path", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    base_url = os.environ.get("OPENAI_BASE_URL", "")
    api_key = os.environ.get("OPENAI_API_KEY", "unused")
    if not base_url:
        raise RuntimeError("OPENAI_BASE_URL is required")

    prompt_text = Path(args.prompt_file).read_text(encoding="utf-8", errors="replace")
    client = OpenAI(
        base_url=base_url,
        api_key=api_key,
        max_retries=0,
        timeout=180,
        http_client=DefaultHttpxClient(trust_env=False),
    )
    out_dir = Path(args.out_dir)

    if args.mode in ("deterministic", "json", "single-shot"):
        schema = json.loads(Path(args.schema_path).read_text(encoding="utf-8"))
        run_deterministic_mode(client, args.model, prompt_text, schema, out_dir)
        return 0
    if args.mode in ("pr-agent-yaml", "pr_agent_yaml", "yaml"):
        run_pr_agent_yaml_mode(client, args.model, prompt_text, out_dir)
        return 0
    run_sse_mode(client, args.model, prompt_text, max(1, args.turns))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
