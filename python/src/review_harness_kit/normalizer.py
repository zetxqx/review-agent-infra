from __future__ import annotations

import json
import math
import os
import tempfile
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import yaml
from google.protobuf import json_format
from openai import OpenAI, OpenAIError

from review_harness_kit.contracts import HarnessAdapter, ProcessResult, RawOutput, RunContext
from review_harness_kit.gen.review.v1 import review_pb2

MAX_RAW_CHARS = 60_000
EMBEDDED_SCHEMA_PATH = Path(__file__).resolve().parent / "schema" / "llm_review_report.schema.json"
NORMALIZER_SYSTEM = (
    "You convert a code reviewer's free-form output into JSON. Extract only findings explicitly "
    "present in the text. Never add, merge, or reword findings. Return an empty findings list if none exist."
)

VALID_SEVERITIES = {
    review_pb2.SEVERITY_CRITICAL,
    review_pb2.SEVERITY_HIGH,
    review_pb2.SEVERITY_MEDIUM,
    review_pb2.SEVERITY_LOW,
}

VALID_CATEGORIES = {
    review_pb2.FINDING_CATEGORY_CORRECTNESS,
    review_pb2.FINDING_CATEGORY_CONCURRENCY,
    review_pb2.FINDING_CATEGORY_SECURITY,
    review_pb2.FINDING_CATEGORY_K8S,
    review_pb2.FINDING_CATEGORY_STYLE,
}

EXTRACT_ERRORS = (
    ValueError,
    TypeError,
    KeyError,
    IndexError,
    LookupError,
    OSError,
    RuntimeError,
    yaml.YAMLError,
    json_format.ParseError,
)
NORMALIZE_ERRORS = (*EXTRACT_ERRORS, OpenAIError)


def strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if len(lines) >= 2 and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1]).strip()
    return stripped


def parse_report_json(raw_json: str) -> review_pb2.ReviewReport:
    cleaned = strip_code_fence(raw_json)
    data = json.loads(cleaned or "{}")
    if not isinstance(data, dict):
        raise TypeError("expected JSON object for ReviewReport")
    findings = data.get("findings")
    if isinstance(findings, list):
        for item in findings:
            if isinstance(item, dict) and item.get("confidence") is None:
                item.pop("confidence", None)
    return json_format.ParseDict(data, review_pb2.ReviewReport(), ignore_unknown_fields=True)


def load_report_schema(schema_path: Path) -> dict[str, Any]:
    target = schema_path if schema_path.is_file() else EMBEDDED_SCHEMA_PATH
    return json.loads(target.read_text(encoding="utf-8"))


def changed_files_from_diff(task: review_pb2.AgentTask) -> str:
    if not task.diff_file:
        return "(unknown)"
    diff_path = Path(task.diff_file)
    if not diff_path.is_file():
        return "(unknown)"
    seen: list[str] = []
    for line in diff_path.read_text(encoding="utf-8", errors="replace").splitlines():
        candidate = line[6:].strip() if line.startswith(("+++ b/", "--- a/")) else ""
        if candidate and candidate != "/dev/null" and candidate not in seen:
            seen.append(candidate)
    return "\n".join(seen) if seen else "(none)"


def resolve_harness_version(adapter: HarnessAdapter) -> str:
    explicit = getattr(adapter, "version", "")
    if isinstance(explicit, str) and explicit:
        return explicit
    for dist_name in (adapter.name, "review-harness-kit"):
        try:
            return version(dist_name)
        except PackageNotFoundError:
            continue
    return "0.1.0"


def harness_exited_cleanly(result: ProcessResult) -> bool:
    return not result.timed_out and result.exit_code == 0


def normalize(
    adapter: HarnessAdapter,
    raw: RawOutput,
    task: review_pb2.AgentTask,
    ctx: RunContext,
    result: ProcessResult,
) -> review_pb2.ReviewReport:
    cand: review_pb2.ReviewReport | None = None
    path = review_pb2.NORMALIZER_PATH_NONE

    try:
        cand = adapter.extract(raw, task)
        if cand is not None:
            path = review_pb2.NORMALIZER_PATH_DETERMINISTIC
    except EXTRACT_ERRORS:
        cand = None

    if cand is None and harness_exited_cleanly(result) and raw.text.strip():
        try:
            cand = llm_normalize(raw.text, task, ctx)
            path = review_pb2.NORMALIZER_PATH_LLM
        except NORMALIZE_ERRORS:
            cand = None

    out = review_pb2.ReviewReport()
    out.provenance.profile = task.harness.profile or adapter.name
    out.provenance.harness_version = resolve_harness_version(adapter)
    out.provenance.normalizer = path
    if result.exit_code is not None:
        out.provenance.exit_code = result.exit_code

    if cand is None:
        out.provenance.status = review_pb2.REPORT_STATUS_HARNESS_FAILED
        return out

    out.provenance.status = review_pb2.REPORT_STATUS_OK
    out.summary = (cand.summary or "")[:8000]
    src = Path(task.src_dir) if task.src_dir else Path("/workspace/src")
    for finding in cand.findings[:100]:
        anchored = anchor_finding(finding, src)
        if anchored is not None:
            out.findings.add().CopyFrom(anchored)
    return out


def llm_normalize(raw_text: str, task: review_pb2.AgentTask, ctx: RunContext) -> review_pb2.ReviewReport:
    schema = load_report_schema(ctx.schema_path)
    client = OpenAI(
        base_url=ctx.base_url,
        api_key=ctx.api_key or "unused",
        max_retries=0,
        timeout=400,
        default_headers={"x-review-phase": "normalize"},
    )
    user_content = (
        f"Changed files:\n{changed_files_from_diff(task)}\n\n"
        f"Reviewer output:\n{raw_text[-MAX_RAW_CHARS:]}"
    )
    resp = client.chat.completions.create(
        model=task.model_alias or "default",
        temperature=0,
        messages=[
            {"role": "system", "content": NORMALIZER_SYSTEM},
            {"role": "user", "content": user_content},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "ReviewReport", "strict": True, "schema": schema},
        },
    )
    content = resp.choices[0].message.content if resp.choices else "{}"
    return parse_report_json(content or "{}")


def anchor_finding(finding: review_pb2.ReviewFinding, src: Path) -> review_pb2.ReviewFinding | None:
    file_rel = finding.file.strip()
    while file_rel.startswith("./"):
        file_rel = file_rel[2:]
    if not file_rel or not finding.title.strip() or Path(file_rel).is_absolute():
        return None

    root = src.resolve()
    if file_rel.startswith(("b/", "a/")) and not (root / file_rel).exists():
        trimmed = file_rel[2:]
        if trimmed and not Path(trimmed).is_absolute() and (root / trimmed).exists():
            file_rel = trimmed

    path = (root / file_rel).resolve()
    if not path.is_relative_to(root) or path == root:
        return None

    out = review_pb2.ReviewFinding()
    out.CopyFrom(finding)
    out.file = file_rel
    out.line = max(0, out.line)
    out.title = out.title.strip()[:200]
    out.explanation = out.explanation[:4000]
    out.suggestion = out.suggestion[:4000]
    out.evidence = out.evidence[:2000]

    if out.HasField("confidence") and (
        not math.isfinite(out.confidence) or out.confidence < 0.0 or out.confidence > 1.0
    ):
        out.ClearField("confidence")

    if out.side not in (review_pb2.DIFF_SIDE_RIGHT, review_pb2.DIFF_SIDE_LEFT):
        out.side = review_pb2.DIFF_SIDE_RIGHT
    if out.severity not in VALID_SEVERITIES:
        out.severity = review_pb2.SEVERITY_MEDIUM
    if out.category not in VALID_CATEGORIES:
        out.category = review_pb2.FINDING_CATEGORY_CORRECTNESS

    if out.side == review_pb2.DIFF_SIDE_LEFT:
        out.target_line_text = out.target_line_text.strip()[:500]
        if out.anchor not in (review_pb2.ANCHOR_KIND_QUOTED, review_pb2.ANCHOR_KIND_DERIVED):
            out.anchor = (
                review_pb2.ANCHOR_KIND_QUOTED
                if out.target_line_text
                else review_pb2.ANCHOR_KIND_DERIVED
            )
        return out

    if out.target_line_text.strip():
        out.target_line_text = out.target_line_text.strip()[:500]
        if out.anchor not in (review_pb2.ANCHOR_KIND_QUOTED, review_pb2.ANCHOR_KIND_DERIVED):
            out.anchor = review_pb2.ANCHOR_KIND_QUOTED
        return out

    if not path.is_file():
        return None
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if 1 <= out.line <= len(lines):
        out.target_line_text = lines[out.line - 1].strip()[:500]
        out.anchor = review_pb2.ANCHOR_KIND_DERIVED
    else:
        out.line = 0
        out.anchor = review_pb2.ANCHOR_KIND_DERIVED
    return out


def write_report(report: review_pb2.ReviewReport, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    serialized = json_format.MessageToJson(report, preserving_proto_field_name=False) + "\n"
    fd, tmp_name = tempfile.mkstemp(dir=output_file.parent, prefix=".report.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as tmp_file:
            tmp_file.write(serialized)
            tmp_file.flush()
        os.replace(tmp_name, output_file)
    except BaseException:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise
