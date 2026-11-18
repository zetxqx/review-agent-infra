from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Self

import pytest
from google.protobuf import json_format

from review_harness_kit.entrypoint import run_command
from review_harness_kit.gen.review.v1 import review_pb2


class MockAdapterHTTPServer:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                phase = self.headers.get("x-review-phase", "harness")
                stream = bool(body.get("stream"))
                outer.calls.append(
                    {
                        "path": self.path,
                        "phase": phase,
                        "auth": self.headers.get("Authorization", ""),
                        "stream": stream,
                        "messages": body.get("messages", []),
                        "response_format": body.get("response_format"),
                    }
                )

                if stream:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.end_headers()
                    chunks = [
                        {
                            "id": "chatcmpl-sse",
                            "object": "chat.completion.chunk",
                            "created": 1700000000,
                            "model": body.get("model", "default"),
                            "choices": [
                                {
                                    "index": 0,
                                    "delta": {
                                        "role": "assistant",
                                        "content": "Defect in pkg/worker.go line 5: `ch <- item` blocks forever.",
                                    },
                                    "finish_reason": None,
                                }
                            ],
                        },
                        {
                            "id": "chatcmpl-sse",
                            "object": "chat.completion.chunk",
                            "created": 1700000000,
                            "model": body.get("model", "default"),
                            "choices": [],
                            "usage": {
                                "prompt_tokens": 200,
                                "completion_tokens": 25,
                                "total_tokens": 225,
                            },
                        },
                    ]
                    for ch in chunks:
                        self.wfile.write(f"data: {json.dumps(ch)}\n\n".encode())
                    self.wfile.write(b"data: [DONE]\n\n")
                    self.wfile.flush()
                    return

                report_json = {
                    "summary": "Found unbuffered channel send leak in pkg/worker.go.",
                    "findings": [
                        {
                            "file": "pkg/worker.go",
                            "line": 5,
                            "side": "DIFF_SIDE_RIGHT",
                            "targetLineText": "ch <- item",
                            "anchor": "ANCHOR_KIND_QUOTED",
                            "severity": "SEVERITY_HIGH",
                            "confidence": 0.91,
                            "category": "FINDING_CATEGORY_CONCURRENCY",
                            "title": "Goroutine leak on unbuffered channel send",
                            "explanation": "Context cancellation is not selected alongside channel send.",
                            "suggestion": "Use select with <-ctx.Done().",
                            "evidence": "Line 5 sends to unbuffered channel ch without select.",
                        }
                    ],
                }
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                resp = {
                    "id": "chatcmpl-json",
                    "object": "chat.completion",
                    "created": 1700000000,
                    "model": body.get("model", "default"),
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": json.dumps(report_json)},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 180, "completion_tokens": 60, "total_tokens": 240},
                }
                self.wfile.write(json.dumps(resp).encode("utf-8"))

            def log_message(self, format: str, *args: object) -> None:
                return

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def host_url_without_v1(self) -> str:
        host, port = self._httpd.server_address
        return f"http://{host}:{port}"

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)


def setup_workspace(root: Path, adapter_name: str, profile_name: str, wall_clock: int = 30) -> tuple[Path, Path]:
    src_dir = root / "src"
    in_dir = root / ".review" / "in"
    out_dir = root / ".review" / "out"
    (src_dir / "pkg").mkdir(parents=True)
    in_dir.mkdir(parents=True)
    out_dir.mkdir(parents=True)

    (src_dir / "pkg" / "worker.go").write_text(
        "package pkg\n\nfunc Send(ch chan int, item int) {\n\tif item > 0 {\n\t\tch <- item\n\t}\n}\n",
        encoding="utf-8",
    )
    (in_dir / "diff.patch").write_text(
        "--- a/pkg/worker.go\n+++ b/pkg/worker.go\n@@ -1,3 +1,7 @@\n+\t\tch <- item\n",
        encoding="utf-8",
    )
    (in_dir / "rules.md").write_text("Always guard channel sends with context cancellation.", encoding="utf-8")

    task = review_pb2.AgentTask(
        run_id="run-e2e-1",
        shard_index=0,
        model_alias="qwen-coder",
        pr_title="Add worker channel sender",
        pr_body="Implements Send helper.",
        base_branch="main",
        src_dir=str(src_dir),
        diff_file=str(in_dir / "diff.patch"),
        rules_dir=str(in_dir / "rules.md"),
        out_dir=str(out_dir),
    )
    task.harness.profile = profile_name
    task.harness.adapter = adapter_name
    task.git.base_ref = "base"
    task.git.head_ref = "pr"
    task.budget.max_requests = 20
    task.budget.max_context_tokens = 32768
    task.budget.max_total_tokens = 500_000
    task.budget.normalizer_reserve_tokens = 50_000
    task.budget.wall_clock_seconds = wall_clock

    task_path = in_dir / "task.json"
    task_path.write_text(json_format.MessageToJson(task), encoding="utf-8")
    return task_path, out_dir


def test_timeout_kills_entire_process_group(tmp_path: Path) -> None:
    task_path, out_dir = setup_workspace(tmp_path, "command", "slow-cmd", wall_clock=1)
    pid_file = tmp_path / "grandchild.pid"
    child_code = (
        "import subprocess, sys, time; "
        "gc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
        f"open({str(pid_file)!r}, 'w').write(str(gc.pid)); "
        "time.sleep(60)"
    )

    task = json_format.Parse(task_path.read_text(encoding="utf-8"), review_pb2.AgentTask())
    task.harness.options["argv"] = json.dumps([sys.executable, "-c", child_code])
    task_path.write_text(json_format.MessageToJson(task), encoding="utf-8")

    stdin_bytes = io.BytesIO(
        json.dumps({"openai_base_url": "http://127.0.0.1:9", "openai_api_key": "eyJ.secret"}).encode("utf-8")
    )
    started = time.monotonic()
    rc = run_command(stdin_bytes, ["--task", str(task_path), "--scratch-dir", str(tmp_path / "scratch")])
    elapsed = time.monotonic() - started

    assert rc == 0
    assert elapsed < 8.0
    assert pid_file.is_file()
    grandchild_pid = int(pid_file.read_text(encoding="utf-8").strip())
    with pytest.raises(ProcessLookupError):
        os.kill(grandchild_pid, 0)

    report = json_format.Parse((out_dir / "report.json").read_text(encoding="utf-8"), review_pb2.ReviewReport())
    assert report.provenance.status == review_pb2.REPORT_STATUS_HARNESS_FAILED


def test_review_kit_cli_end_to_end_sse_and_deterministic_profiles(tmp_path: Path) -> None:
    secret_token = "eyJhbGciOiJIUzI1NiJ9.supersecret.signature"

    with MockAdapterHTTPServer() as srv:
        sse_root = tmp_path / "sse_run"
        sse_task, sse_out = setup_workspace(sse_root, "mock-llm", "openhands-sim")
        stdin_payload = json.dumps(
            {
                "openai_base_url": srv.host_url_without_v1,
                "openai_api_key": secret_token,
            }
        ).encode("utf-8")

        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "review_harness_kit.entrypoint",
                "run",
                "--task",
                str(sse_task),
                "--scratch-dir",
                str(sse_root / "scratch"),
            ],
            input=stdin_payload,
            capture_output=True,
            timeout=30,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr.decode("utf-8", errors="replace")

        assert len(srv.calls) == 3
        assert srv.calls[0]["path"] == "/v1/chat/completions"
        assert srv.calls[0]["stream"] is True
        assert srv.calls[0]["phase"] == "harness"
        assert len(srv.calls[0]["messages"]) == 2
        assert srv.calls[1]["stream"] is True
        assert srv.calls[1]["phase"] == "harness"
        assert len(srv.calls[1]["messages"]) == 4
        assert srv.calls[2]["stream"] is False
        assert srv.calls[2]["phase"] == "normalize"
        assert srv.calls[2]["response_format"]["type"] == "json_schema"

        sse_report = json_format.Parse(
            (sse_out / "report.json").read_text(encoding="utf-8"),
            review_pb2.ReviewReport(),
        )
        assert sse_report.provenance.status == review_pb2.REPORT_STATUS_OK
        assert sse_report.provenance.normalizer == review_pb2.NORMALIZER_PATH_LLM
        assert sse_report.provenance.exit_code == 0
        assert len(sse_report.findings) == 1
        assert sse_report.findings[0].file == "pkg/worker.go"
        assert sse_report.findings[0].line == 5
        assert sse_report.findings[0].target_line_text == "ch <- item"

        for fpath in sse_root.rglob("*"):
            if fpath.is_file():
                assert secret_token not in fpath.read_text(encoding="utf-8", errors="replace")

        srv.calls.clear()
        det_root = tmp_path / "det_run"
        det_task, det_out = setup_workspace(det_root, "single-shot", "pr-agent-sim")
        rc = run_command(
            io.BytesIO(stdin_payload),
            ["--task", str(det_task), "--scratch-dir", str(det_root / "scratch")],
        )
        assert rc == 0
        assert len(srv.calls) == 1
        assert srv.calls[0]["phase"] == "harness"
        assert srv.calls[0]["stream"] is False

        det_report = json_format.Parse(
            (det_out / "report.json").read_text(encoding="utf-8"),
            review_pb2.ReviewReport(),
        )
        assert det_report.provenance.status == review_pb2.REPORT_STATUS_OK
        assert det_report.provenance.normalizer == review_pb2.NORMALIZER_PATH_DETERMINISTIC
        assert len(det_report.findings) == 1
