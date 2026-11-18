from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import yaml

from review_harness_kit.contracts import (
    HarnessInvocation,
    HarnessUnsupportedError,
    ProcessResult,
    RawOutput,
    RunContext,
)
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import strip_code_fence

DEFAULT_PR_AGENT_PYTHON = "/opt/harness/bin/python"
DEFAULT_CAPTURE_SCRIPT = Path(__file__).resolve().parent.parent / "runners" / "pr_agent_capture.py"

SEVERITY_BY_NAME = {
    "critical": review_pb2.SEVERITY_CRITICAL,
    "high": review_pb2.SEVERITY_HIGH,
    "medium": review_pb2.SEVERITY_MEDIUM,
    "low": review_pb2.SEVERITY_LOW,
}


def map_pr_agent_severity(issue: dict[str, Any]) -> int:
    raw_sev = str(issue.get("severity") or "").strip().lower()
    if raw_sev in SEVERITY_BY_NAME:
        return SEVERITY_BY_NAME[raw_sev]
    score = issue.get("score")
    if isinstance(score, (int, float)):
        if score >= 9:
            return review_pb2.SEVERITY_CRITICAL
        if score >= 7:
            return review_pb2.SEVERITY_HIGH
        if score <= 3:
            return review_pb2.SEVERITY_LOW
    return review_pb2.SEVERITY_MEDIUM


def map_pr_agent_category(issue: dict[str, Any]) -> int:
    text = f"{issue.get('label', '')} {issue.get('issue_header', '')}".lower()
    if "security" in text or "secret" in text or "injection" in text or "auth" in text:
        return review_pb2.FINDING_CATEGORY_SECURITY
    if "race" in text or "concurren" in text or "deadlock" in text or "mutex" in text:
        return review_pb2.FINDING_CATEGORY_CONCURRENCY
    if "k8s" in text or "kubernetes" in text or "pod" in text or "rbac" in text:
        return review_pb2.FINDING_CATEGORY_K8S
    if "style" in text or "nit" in text or "naming" in text:
        return review_pb2.FINDING_CATEGORY_STYLE
    return review_pb2.FINDING_CATEGORY_CORRECTNESS


def coerce_line_number(issue: dict[str, Any]) -> int:
    for key in ("end_line", "start_line", "line"):
        val = issue.get(key)
        if val is not None and str(val).strip() != "":
            try:
                return max(0, int(val))
            except (TypeError, ValueError):
                continue
    return 0


class PRAgentAdapter:
    name = "pr-agent"
    cache_shape = "single_shot"

    def prepare(self, task: review_pb2.AgentTask, ctx: RunContext) -> HarnessInvocation:
        if not task.git.HasField("base_ref") or not task.git.base_ref:
            raise HarnessUnsupportedError("pr-agent requires a local base branch")
        out = Path(task.out_dir or "/workspace/.review/out")
        max_ctx = task.budget.max_context_tokens if task.budget.max_context_tokens > 0 else 32768
        env = {
            "CONFIG__GIT_PROVIDER": "local",
            "CONFIG__MODEL": f"openai/{task.model_alias or 'default'}",
            "CONFIG__FALLBACK_MODELS": "[]",
            "CONFIG__CUSTOM_MODEL_MAX_TOKENS": str(max_ctx),
            "CONFIG__AI_TIMEOUT": "400",
            "CONFIG__NUM_RETRIES": "0",
            "CONFIG__USE_REPO_SETTINGS_FILE": "false",
            "OPENAI__API_BASE": ctx.base_url,
            "OPENAI__KEY": ctx.api_key,
            "LOCAL__REVIEW_PATH": str(out / "pr_agent_review.md"),
            "PR_REVIEWER__EXTRA_INSTRUCTIONS": ctx.rules_text,
        }
        python_bin = task.harness.options.get("python_bin") or (
            DEFAULT_PR_AGENT_PYTHON if Path(DEFAULT_PR_AGENT_PYTHON).exists() else sys.executable
        )
        capture_script = task.harness.options.get("capture_script") or str(DEFAULT_CAPTURE_SCRIPT)
        argv = [python_bin, capture_script, task.git.base_ref, str(out / "pr_agent_prediction.yaml")]
        return HarnessInvocation(argv=argv, cwd=task.src_dir or "/workspace/src", env=env)

    def collect(self, task: review_pb2.AgentTask, ctx: RunContext, result: ProcessResult) -> RawOutput:
        out = Path(task.out_dir or "/workspace/.review/out")
        for candidate, kind in (
            (out / "pr_agent_prediction.yaml", "yaml"),
            (out / "pr_agent_prediction.json", "yaml"),
            (out / "pr_agent_review.md", "markdown"),
        ):
            if candidate.is_file():
                content = candidate.read_text(encoding="utf-8", errors="replace")
                if content.strip():
                    return RawOutput(kind=kind, text=content)
        if result.log_path.is_file():
            log_text = result.log_path.read_text(encoding="utf-8", errors="replace")[-60_000:]
            return RawOutput(kind="text", text=log_text)
        return RawOutput(kind="yaml", text="")

    def extract(self, raw: RawOutput, task: review_pb2.AgentTask) -> review_pb2.ReviewReport | None:
        if raw.kind not in ("yaml", "json") or not raw.text.strip():
            return None
        data = yaml.safe_load(strip_code_fence(raw.text))
        if not isinstance(data, dict):
            return None
        review = data.get("review") if isinstance(data.get("review"), dict) else data
        if "key_issues_to_review" not in review and "security_concerns" not in review:
            return None

        report = review_pb2.ReviewReport()
        for issue in review.get("key_issues_to_review") or []:
            if not isinstance(issue, dict):
                continue
            relevant_file = str(issue.get("relevant_file") or "").strip()
            header = str(issue.get("issue_header") or "").strip()
            content = str(issue.get("issue_content") or "").strip()
            if not relevant_file or not (header or content):
                continue
            finding = report.findings.add()
            finding.file = relevant_file
            finding.line = coerce_line_number(issue)
            side_str = str(issue.get("side") or "RIGHT").strip().upper()
            finding.side = (
                review_pb2.DIFF_SIDE_LEFT if side_str == "LEFT" else review_pb2.DIFF_SIDE_RIGHT
            )
            relevant_line = str(issue.get("relevant_line") or "").strip()
            if relevant_line:
                if relevant_line.startswith(("+", "-")) and not relevant_line.startswith(("+++", "---")):
                    relevant_line = relevant_line[1:].strip()
                finding.target_line_text = relevant_line[:500]
                finding.anchor = review_pb2.ANCHOR_KIND_QUOTED
            else:
                finding.anchor = review_pb2.ANCHOR_KIND_DERIVED
            finding.severity = map_pr_agent_severity(issue)
            finding.category = map_pr_agent_category(issue)
            finding.title = (header or content.splitlines()[0])[:200]
            finding.explanation = content[:4000]
            improved = str(issue.get("improved_code") or issue.get("suggestion") or "").strip()
            if improved:
                finding.suggestion = improved[:4000]
            raw_conf = issue.get("confidence")
            if isinstance(raw_conf, (int, float)) and not isinstance(raw_conf, bool) and 0.0 <= float(raw_conf) <= 1.0:
                finding.confidence = float(raw_conf)

        summary_parts: list[str] = []
        general = str(review.get("general_summary") or review.get("summary") or "").strip()
        if general:
            summary_parts.append(general)
        security = str(review.get("security_concerns") or "No").strip()
        if security and not security.lower().startswith("no"):
            summary_parts.append(f"Security concerns: {security}")
        report.summary = "\n\n".join(summary_parts)[:8000]
        return report
