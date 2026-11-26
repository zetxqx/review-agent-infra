from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Self

import pytest

from review_harness_kit.adapters.openhands import OpenHandsAdapter
from review_harness_kit.adapters.pr_agent import PRAgentAdapter
from review_harness_kit.contracts import ProcessResult, RawOutput, RunContext
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import (
    EMBEDDED_SCHEMA_PATH,
    ReviewablePaths,
    anchor_finding,
    normalize,
    reviewable_paths_for_task,
)


class RecordedRequest:
    def __init__(self, path: str, headers: dict[str, str], body: dict[str, Any]) -> None:
        self.path = path
        self.headers = headers
        self.body = body


class MockNormalizeServer:
    def __init__(self, response_payload: dict[str, Any], status_code: int = 200) -> None:
        self.response_payload = response_payload
        self.status_code = status_code
        self.requests: list[RecordedRequest] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(length).decode("utf-8")
                hdrs = {k.lower(): v for k, v in self.headers.items()}
                outer.requests.append(RecordedRequest(self.path, hdrs, json.loads(raw_body)))

                self.send_response(outer.status_code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                if outer.status_code >= 400:
                    err_bytes = json.dumps({"error": {"message": "budget_exhausted"}}).encode("utf-8")
                    self.wfile.write(err_bytes)
                    return
                completion = {
                    "id": "chatcmpl-norm-1",
                    "object": "chat.completion",
                    "created": 1700000000,
                    "model": "qwen-coder",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": json.dumps(outer.response_payload),
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 120, "completion_tokens": 45, "total_tokens": 165},
                }
                self.wfile.write(json.dumps(completion).encode("utf-8"))

            def log_message(self, format: str, *args: object) -> None:
                return

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._httpd.server_address
        return f"http://{host}:{port}/v1"

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)


def test_deterministic_normalizer_derives_snapshot_line_without_http(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    (src_dir / "pkg").mkdir(parents=True)
    (src_dir / "pkg" / "handler.go").write_text(
        "package pkg\n\nfunc Serve() {\n\tpanic(\"unimplemented\")\n}\n",
        encoding="utf-8",
    )

    yaml_text = (
        "review:\n"
        "  security_concerns: No\n"
        "  key_issues_to_review:\n"
        "    - relevant_file: pkg/handler.go\n"
        "      end_line: 4\n"
        "      issue_header: Unhandled panic in Serve\n"
        "      issue_content: Serve panics unconditionally.\n"
    )
    task = review_pb2.AgentTask(
        src_dir=str(src_dir),
        out_dir=str(tmp_path / "out"),
        model_alias="qwen-coder",
    )
    task.harness.profile = "pr-agent"

    with MockNormalizeServer({"summary": "unused", "findings": []}) as srv:
        ctx = RunContext(
            base_url=srv.base_url,
            api_key="tok-1",
            home=tmp_path / "home",
            prompt="prompt",
            rules_text="rules",
            schema_path=EMBEDDED_SCHEMA_PATH,
        )
        res = ProcessResult(exit_code=0, timed_out=False, log_path=tmp_path / "harness.log")
        report = normalize(PRAgentAdapter(), RawOutput(kind="yaml", text=yaml_text), task, ctx, res)
        assert len(srv.requests) == 0

    assert report.provenance.status == review_pb2.REPORT_STATUS_OK
    assert report.provenance.normalizer == review_pb2.NORMALIZER_PATH_DETERMINISTIC
    assert report.provenance.exit_code == 0
    assert len(report.findings) == 1
    assert report.findings[0].target_line_text == 'panic("unimplemented")'
    assert report.findings[0].anchor == review_pb2.ANCHOR_KIND_DERIVED


def test_llm_normalizer_fallback_sends_normalize_header_and_filters_traversal(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    (src_dir / "internal").mkdir(parents=True)
    (src_dir / "internal" / "cache.go").write_text(
        "package internal\n\nfunc Get(k string) string {\n\treturn store[k]\n}\n",
        encoding="utf-8",
    )
    diff_file = tmp_path / "diff.patch"
    diff_file.write_text(
        "--- a/internal/cache.go\n+++ b/internal/cache.go\n@@ -1,3 +1,5 @@\n+return store[k]\n",
        encoding="utf-8",
    )

    llm_payload = {
        "summary": "Detected unguarded concurrent map read.",
        "findings": [
            {
                "file": "internal/cache.go",
                "line": 4,
                "side": "DIFF_SIDE_RIGHT",
                "targetLineText": "",
                "anchor": "ANCHOR_KIND_DERIVED",
                "severity": "SEVERITY_HIGH",
                "confidence": 0.88,
                "category": "FINDING_CATEGORY_CONCURRENCY",
                "title": "Concurrent map read without lock",
                "explanation": "store is read without holding mu.RLock.",
                "suggestion": "Guard access with RWMutex.",
                "evidence": "Line 4 reads store[k].",
            },
            {
                "file": "../../etc/passwd",
                "line": 1,
                "side": "DIFF_SIDE_RIGHT",
                "targetLineText": "root:x:0:0",
                "anchor": "ANCHOR_KIND_QUOTED",
                "severity": "SEVERITY_CRITICAL",
                "confidence": 0.99,
                "category": "FINDING_CATEGORY_SECURITY",
                "title": "Path traversal attempt",
                "explanation": "Should be rejected by snapshot containment.",
                "suggestion": "",
                "evidence": "",
            },
        ],
    }

    task = review_pb2.AgentTask(
        src_dir=str(src_dir),
        diff_file=str(diff_file),
        out_dir=str(tmp_path / "out"),
        model_alias="qwen-coder",
    )
    task.harness.profile = "openhands"

    with MockNormalizeServer(llm_payload) as srv:
        ctx = RunContext(
            base_url=srv.base_url,
            api_key="eyJ.jwt.norm",
            home=tmp_path / "home",
            prompt="prompt",
            rules_text="rules",
            schema_path=EMBEDDED_SCHEMA_PATH,
        )
        raw = RawOutput(kind="markdown", text="In internal/cache.go line 4, store[k] races.")
        res = ProcessResult(exit_code=0, timed_out=False, log_path=tmp_path / "harness.log")
        report = normalize(OpenHandsAdapter(), raw, task, ctx, res)

        assert len(srv.requests) == 1
        req = srv.requests[0]
        assert req.path == "/v1/chat/completions"
        assert req.headers.get("x-review-phase") == "normalize"
        assert req.headers.get("authorization") == "Bearer eyJ.jwt.norm"
        assert req.body["response_format"]["type"] == "json_schema"
        assert req.body["response_format"]["json_schema"]["strict"] is True
        assert "internal/cache.go" in req.body["messages"][1]["content"]

    assert report.provenance.status == review_pb2.REPORT_STATUS_OK
    assert report.provenance.normalizer == review_pb2.NORMALIZER_PATH_LLM
    assert len(report.findings) == 1
    assert report.findings[0].file == "internal/cache.go"
    assert report.findings[0].target_line_text == "return store[k]"
    assert report.findings[0].confidence == 0.88


def test_normalizer_marks_harness_failed_when_upstream_errors(tmp_path: Path) -> None:
    task = review_pb2.AgentTask(
        src_dir=str(tmp_path / "src"),
        out_dir=str(tmp_path / "out"),
        model_alias="qwen-coder",
    )
    task.harness.profile = "openhands"
    (tmp_path / "src").mkdir(parents=True)

    with MockNormalizeServer({}, status_code=403) as srv:
        ctx = RunContext(
            base_url=srv.base_url,
            api_key="eyJ.jwt.norm",
            home=tmp_path / "home",
            prompt="prompt",
            rules_text="rules",
            schema_path=EMBEDDED_SCHEMA_PATH,
        )
        raw = RawOutput(kind="markdown", text="Unstructured text that needs normalizer.")
        res = ProcessResult(exit_code=1, timed_out=False, log_path=tmp_path / "harness.log")
        report = normalize(OpenHandsAdapter(), raw, task, ctx, res)

    assert report.provenance.status == review_pb2.REPORT_STATUS_HARNESS_FAILED
    assert report.provenance.normalizer == review_pb2.NORMALIZER_PATH_NONE
    assert report.provenance.exit_code == 1


@pytest.mark.parametrize(
    ("exit_code", "timed_out"),
    [(-9, True), (1, False)],
    ids=["timed_out", "nonzero_exit"],
)
def test_unfinished_harness_without_report_file_skips_llm_normalizer(
    tmp_path: Path, exit_code: int, timed_out: bool
) -> None:
    task = review_pb2.AgentTask(
        src_dir=str(tmp_path / "src"),
        out_dir=str(tmp_path / "out"),
        model_alias="qwen-coder",
    )
    task.harness.profile = "openhands"
    (tmp_path / "src").mkdir(parents=True)

    with MockNormalizeServer({"summary": "Partial deliberation only.", "findings": []}) as srv:
        ctx = RunContext(
            base_url=srv.base_url,
            api_key="eyJ.jwt.norm",
            home=tmp_path / "home",
            prompt="prompt",
            rules_text="rules",
            schema_path=EMBEDDED_SCHEMA_PATH,
        )
        raw = RawOutput(kind="markdown", text="Still checking candidate A before finalizing.")
        res = ProcessResult(exit_code=exit_code, timed_out=timed_out, log_path=tmp_path / "harness.log")
        report = normalize(OpenHandsAdapter(), raw, task, ctx, res)
        assert len(srv.requests) == 0

    assert report.provenance.status == review_pb2.REPORT_STATUS_HARNESS_FAILED
    assert report.provenance.normalizer == review_pb2.NORMALIZER_PATH_NONE
    assert report.provenance.exit_code == exit_code
    assert report.summary == ""
    assert len(report.findings) == 0


def test_timed_out_harness_with_report_file_keeps_deterministic_report(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    (src_dir / "internal").mkdir(parents=True)
    (src_dir / "internal" / "cache.go").write_text(
        "package internal\n\nfunc Get(k string) string {\n\treturn store[k]\n}\n",
        encoding="utf-8",
    )
    written_report = {
        "summary": "Report written before the wall clock expired.",
        "findings": [
            {
                "file": "internal/cache.go",
                "line": 4,
                "side": "DIFF_SIDE_RIGHT",
                "targetLineText": "return store[k]",
                "anchor": "ANCHOR_KIND_QUOTED",
                "severity": "SEVERITY_HIGH",
                "confidence": 0.9,
                "category": "FINDING_CATEGORY_CONCURRENCY",
                "title": "Concurrent map read without lock",
                "explanation": "store is read without holding mu.RLock.",
                "suggestion": "Guard access with RWMutex.",
                "evidence": "Line 4 reads store[k].",
            }
        ],
    }
    task = review_pb2.AgentTask(
        src_dir=str(src_dir),
        out_dir=str(tmp_path / "out"),
        model_alias="qwen-coder",
    )
    task.harness.profile = "openhands"

    with MockNormalizeServer({"summary": "unused", "findings": []}) as srv:
        ctx = RunContext(
            base_url=srv.base_url,
            api_key="eyJ.jwt.norm",
            home=tmp_path / "home",
            prompt="prompt",
            rules_text="rules",
            schema_path=EMBEDDED_SCHEMA_PATH,
        )
        raw = RawOutput(kind="json", text=json.dumps(written_report))
        res = ProcessResult(exit_code=-9, timed_out=True, log_path=tmp_path / "harness.log")
        report = normalize(OpenHandsAdapter(), raw, task, ctx, res)
        assert len(srv.requests) == 0

    assert report.provenance.status == review_pb2.REPORT_STATUS_OK
    assert report.provenance.normalizer == review_pb2.NORMALIZER_PATH_DETERMINISTIC
    assert report.provenance.exit_code == -9
    assert len(report.findings) == 1
    assert report.findings[0].file == "internal/cache.go"


PROXY_ENV_NAMES = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")


def point_proxies_at_dead_port(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in PROXY_ENV_NAMES:
        monkeypatch.setenv(name, "http://127.0.0.1:9")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)


def test_llm_normalizer_ignores_ambient_proxy_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    point_proxies_at_dead_port(monkeypatch)
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    task = review_pb2.AgentTask(src_dir=str(src_dir), out_dir=str(tmp_path / "out"), model_alias="qwen-coder")
    task.harness.profile = "openhands"

    with MockNormalizeServer({"summary": "Direct route.", "findings": []}) as srv:
        ctx = RunContext(
            base_url=srv.base_url,
            api_key="tok-direct",
            home=tmp_path / "home",
            prompt="prompt",
            rules_text="rules",
            schema_path=EMBEDDED_SCHEMA_PATH,
        )
        raw = RawOutput(kind="markdown", text="No defects found.")
        res = ProcessResult(exit_code=0, timed_out=False, log_path=tmp_path / "harness.log")
        report = normalize(OpenHandsAdapter(), raw, task, ctx, res)
        assert len(srv.requests) == 1
        assert srv.requests[0].headers.get("authorization") == "Bearer tok-direct"

    assert report.provenance.status == review_pb2.REPORT_STATUS_OK
    assert report.provenance.normalizer == review_pb2.NORMALIZER_PATH_LLM
    assert report.summary == "Direct route."


def make_finding(file: str, line: int, side: int = review_pb2.DIFF_SIDE_RIGHT) -> review_pb2.ReviewFinding:
    return review_pb2.ReviewFinding(
        file=file,
        line=line,
        side=side,
        severity=review_pb2.SEVERITY_HIGH,
        category=review_pb2.FINDING_CATEGORY_CORRECTNESS,
        title=f"Issue in {file}",
        explanation="Explained.",
    )


def test_anchor_finding_keeps_deleted_file_in_diff_as_file_level(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    reviewable = ReviewablePaths(diff_paths=frozenset({"pkg/removed.go"}))

    for given in ("pkg/removed.go", "a/pkg/removed.go", "b/pkg/removed.go"):
        anchored = anchor_finding(make_finding(given, 7), src_dir, reviewable)
        assert anchored is not None, given
        assert anchored.file == "pkg/removed.go"
        assert anchored.line == 0
        assert anchored.anchor == review_pb2.ANCHOR_KIND_DERIVED
        assert anchored.target_line_text == ""


def test_anchor_finding_keeps_masked_files_and_globs_as_file_level(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    reviewable = ReviewablePaths(masked_patterns=("AGENTS.md", ".codex/*"))

    agents = anchor_finding(make_finding("AGENTS.md", 3), src_dir, reviewable)
    assert agents is not None
    assert agents.file == "AGENTS.md"
    assert agents.line == 0
    assert agents.anchor == review_pb2.ANCHOR_KIND_DERIVED

    codex = anchor_finding(make_finding("b/.codex/config.toml", 1), src_dir, reviewable)
    assert codex is not None
    assert codex.file == ".codex/config.toml"
    assert codex.line == 0


def test_anchor_finding_rejects_hallucinated_paths_outside_diff(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    (src_dir / "pkg").mkdir(parents=True)
    reviewable = ReviewablePaths(diff_paths=frozenset({"pkg/removed.go"}), masked_patterns=("AGENTS.md",))

    assert anchor_finding(make_finding("ghost.go", 2), src_dir, reviewable) is None
    assert anchor_finding(make_finding("b/ghost.go", 2), src_dir, reviewable) is None
    assert anchor_finding(make_finding("pkg", 1), src_dir, reviewable) is None
    assert anchor_finding(make_finding("pkg/removed.go", 2), src_dir) is None
    assert anchor_finding(make_finding("../pkg/removed.go", 2), src_dir, reviewable) is None


def test_reviewable_paths_for_task_reads_old_new_and_renamed_paths(tmp_path: Path) -> None:
    diff_file = tmp_path / "diff.patch"
    diff_file.write_text(
        "diff --git a/gone.go b/gone.go\n"
        "deleted file mode 100644\n"
        "--- a/gone.go\n"
        "+++ /dev/null\n"
        "@@ -1 +0,0 @@\n"
        "-package gone\n"
        "diff --git a/old/name.go b/new/name.go\n"
        "similarity index 100%\n"
        "rename from old/name.go\n"
        "rename to new/name.go\n"
        "diff --git a/added.go b/added.go\n"
        "--- /dev/null\n"
        "+++ b/added.go\t2024-01-01 00:00:00\n"
        "@@ -0,0 +1 @@\n"
        "+package added\n",
        encoding="utf-8",
    )
    task = review_pb2.AgentTask(diff_file=str(diff_file), masked_files=["AGENTS.md", ".codex/*"])
    reviewable = reviewable_paths_for_task(task)

    assert reviewable.diff_paths == frozenset({"gone.go", "old/name.go", "new/name.go", "added.go"})
    assert "AGENTS.md" in reviewable
    assert ".codex/sub/x.toml" in reviewable
    assert "/dev/null" not in reviewable
    assert "other.go" not in reviewable


def test_normalize_keeps_findings_on_deleted_and_masked_files(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    (src_dir / "pkg").mkdir(parents=True)
    (src_dir / "pkg" / "live.go").write_text("package pkg\n\nvar x = 1\n", encoding="utf-8")
    diff_file = tmp_path / "diff.patch"
    diff_file.write_text(
        "diff --git a/pkg/removed.go b/pkg/removed.go\n"
        "--- a/pkg/removed.go\n"
        "+++ /dev/null\n"
        "@@ -1,2 +0,0 @@\n"
        "-package pkg\n"
        "-var lock sync.Mutex\n",
        encoding="utf-8",
    )
    written_report = {
        "summary": "Mixed findings.",
        "findings": [
            {"file": "a/pkg/removed.go", "line": 2, "side": "DIFF_SIDE_RIGHT", "title": "Deleted lock still used"},
            {"file": "AGENTS.md", "line": 4, "side": "DIFF_SIDE_RIGHT", "title": "Masked instructions changed"},
            {"file": "pkg/live.go", "line": 3, "side": "DIFF_SIDE_RIGHT", "title": "Live file issue"},
            {"file": "pkg/ghost.go", "line": 1, "side": "DIFF_SIDE_RIGHT", "title": "Hallucinated file"},
        ],
    }
    task = review_pb2.AgentTask(
        src_dir=str(src_dir),
        diff_file=str(diff_file),
        out_dir=str(tmp_path / "out"),
        model_alias="qwen-coder",
        masked_files=["AGENTS.md"],
    )
    task.harness.profile = "openhands"
    ctx = RunContext(
        base_url="http://127.0.0.1:9/v1",
        api_key="unused",
        home=tmp_path / "home",
        prompt="prompt",
        rules_text="rules",
        schema_path=EMBEDDED_SCHEMA_PATH,
    )
    raw = RawOutput(kind="json", text=json.dumps(written_report))
    res = ProcessResult(exit_code=0, timed_out=False, log_path=tmp_path / "harness.log")
    report = normalize(OpenHandsAdapter(), raw, task, ctx, res)

    assert report.provenance.normalizer == review_pb2.NORMALIZER_PATH_DETERMINISTIC
    by_file = {f.file: f for f in report.findings}
    assert set(by_file) == {"pkg/removed.go", "AGENTS.md", "pkg/live.go"}
    assert by_file["pkg/removed.go"].line == 0
    assert by_file["AGENTS.md"].line == 0
    assert by_file["pkg/live.go"].line == 3
    assert by_file["pkg/live.go"].target_line_text == "var x = 1"
