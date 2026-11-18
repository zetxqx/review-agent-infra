from __future__ import annotations

import json
from pathlib import Path

import pytest
from google.protobuf import json_format

from review_harness_kit.contracts import HarnessAdapter, load_adapter
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import EMBEDDED_SCHEMA_PATH, parse_report_json


def test_protojson_confidence_presence_round_trip() -> None:
    report = review_pb2.ReviewReport(summary="Checked 2 files.")
    unset_finding = report.findings.add(
        file="pkg/router/router.go",
        line=14,
        side=review_pb2.DIFF_SIDE_RIGHT,
        target_line_text="return r.pick(ctx)",
        anchor=review_pb2.ANCHOR_KIND_QUOTED,
        severity=review_pb2.SEVERITY_HIGH,
        category=review_pb2.FINDING_CATEGORY_CORRECTNESS,
        title="Nil dereference on empty pool",
    )
    zero_finding = report.findings.add(
        file="pkg/router/router.go",
        line=20,
        side=review_pb2.DIFF_SIDE_RIGHT,
        target_line_text="pool.Close()",
        anchor=review_pb2.ANCHOR_KIND_QUOTED,
        severity=review_pb2.SEVERITY_LOW,
        confidence=0.0,
        category=review_pb2.FINDING_CATEGORY_CONCURRENCY,
        title="Low-confidence speculative issue",
    )
    high_finding = report.findings.add(
        file="pkg/router/router.go",
        line=35,
        side=review_pb2.DIFF_SIDE_RIGHT,
        target_line_text="go handle(conn)",
        anchor=review_pb2.ANCHOR_KIND_QUOTED,
        severity=review_pb2.SEVERITY_CRITICAL,
        confidence=0.92,
        category=review_pb2.FINDING_CATEGORY_CONCURRENCY,
        title="Unbounded goroutine spawn",
    )

    assert not unset_finding.HasField("confidence")
    assert zero_finding.HasField("confidence")
    assert high_finding.HasField("confidence")

    encoded = json_format.MessageToJson(report, preserving_proto_field_name=False)
    raw_dict = json.loads(encoded)
    assert "confidence" not in raw_dict["findings"][0]
    assert raw_dict["findings"][1]["confidence"] == 0.0
    assert raw_dict["findings"][2]["confidence"] == pytest.approx(0.92)

    decoded = json_format.Parse(encoded, review_pb2.ReviewReport())
    assert not decoded.findings[0].HasField("confidence")
    assert decoded.findings[1].HasField("confidence")
    assert decoded.findings[1].confidence == 0.0
    assert decoded.findings[2].HasField("confidence")
    assert decoded.findings[2].confidence == pytest.approx(0.92)


def test_agent_task_int64_protojson_round_trip() -> None:
    raw_task = json.dumps(
        {
            "runId": "run-123",
            "shardIndex": 0,
            "harness": {"profile": "pr-agent", "adapter": "pr-agent"},
            "modelAlias": "qwen-coder",
            "budget": {
                "maxRequests": 40,
                "maxContextTokens": 32768,
                "maxTotalTokens": "1500000",
                "normalizerReserveTokens": "50000",
                "wallClockSeconds": 180,
            },
        }
    )
    task = json_format.Parse(raw_task, review_pb2.AgentTask())
    assert task.budget.max_total_tokens == 1_500_000
    assert task.budget.normalizer_reserve_tokens == 50_000


def test_parse_report_json_handles_null_confidence() -> None:
    llm_json = json.dumps(
        {
            "summary": "Found 1 bug.",
            "findings": [
                {
                    "file": "internal/server.go",
                    "line": 10,
                    "side": "DIFF_SIDE_RIGHT",
                    "targetLineText": "defer mu.Unlock()",
                    "anchor": "ANCHOR_KIND_QUOTED",
                    "severity": "SEVERITY_HIGH",
                    "confidence": None,
                    "category": "FINDING_CATEGORY_CONCURRENCY",
                    "title": "Unlock without matching Lock",
                    "explanation": "Mutex is unlocked without being locked.",
                    "suggestion": "Acquire mu.Lock() first.",
                    "evidence": "Line 10 calls Unlock directly.",
                }
            ],
        }
    )
    parsed = parse_report_json(llm_json)
    assert len(parsed.findings) == 1
    assert not parsed.findings[0].HasField("confidence")
    assert parsed.findings[0].severity == review_pb2.SEVERITY_HIGH


def test_embedded_schema_matches_repo_generated_schema() -> None:
    repo_schema = Path(__file__).resolve().parents[2] / "gen" / "schema" / "llm_review_report.schema.json"
    assert repo_schema.is_file()
    assert EMBEDDED_SCHEMA_PATH.is_file()
    assert json.loads(EMBEDDED_SCHEMA_PATH.read_text(encoding="utf-8")) == json.loads(
        repo_schema.read_text(encoding="utf-8")
    )


@pytest.mark.parametrize(
    "adapter_name",
    [
        "pr-agent",
        "pr_agent",
        "openhands",
        "codex",
        "command",
        "external-cmd",
        "mock-llm",
        "single-shot",
    ],
)
def test_entry_point_adapter_discovery(adapter_name: str) -> None:
    adapter = load_adapter(adapter_name)
    assert isinstance(adapter, HarnessAdapter)


def test_unknown_adapter_raises_lookup_error() -> None:
    with pytest.raises(LookupError):
        load_adapter("nonexistent-harness-xyz")
