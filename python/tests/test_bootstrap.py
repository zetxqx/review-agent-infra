from __future__ import annotations

import io
import json
import os
import stat
import subprocess
import sys
import tarfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Self

import pytest
from google.protobuf import json_format

from review_harness_kit.bootstrap import (
    BootstrapConfig,
    download_and_extract_bundle,
    init_local_git_repo,
    upload_report,
)
from review_harness_kit.entrypoint import main, run_command
from review_harness_kit.gen.review.v1 import review_pb2
from review_harness_kit.normalizer import write_report


def build_bundle_tgz(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(data)
            info.mode = 0o644
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def restore_writable(root: Path) -> None:
    for cur_root, dirs, files in os.walk(root):
        cur = Path(cur_root)
        os.chmod(cur, 0o700)
        for d in dirs:
            os.chmod(cur / d, 0o700)
        for f in files:
            p = cur / f
            if not p.is_symlink():
                os.chmod(p, 0o600)


class MockControllerServer:
    def __init__(self, bundle_bytes: bytes, expected_token: str) -> None:
        self.bundle_bytes = bundle_bytes
        self.expected_token = expected_token
        self.bundle_fetches = 0
        self.uploaded_reports: list[bytes] = []
        self.report_failures_remaining = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.headers.get("Authorization") != f"Bearer {outer.expected_token}":
                    self.send_response(401)
                    self.end_headers()
                    return
                if self.path == "/v1/sandbox/bundle":
                    outer.bundle_fetches += 1
                    if outer.bundle_fetches > 1:
                        self.send_response(410)
                        self.end_headers()
                        return
                    self.send_response(200)
                    self.send_header("Content-Type", "application/gzip")
                    self.end_headers()
                    self.wfile.write(outer.bundle_bytes)
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self) -> None:
                if self.headers.get("Authorization") != f"Bearer {outer.expected_token}":
                    self.send_response(401)
                    self.end_headers()
                    return
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                if self.path == "/v1/sandbox/report":
                    if outer.report_failures_remaining > 0:
                        outer.report_failures_remaining -= 1
                        self.send_response(503)
                        self.end_headers()
                        return
                    outer.uploaded_reports.append(body)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"status":"ok"}')
                    return
                self.send_response(404)
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                return

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        host, port = self._httpd.server_address
        return f"http://{host}:{port}"

    def __enter__(self) -> Self:
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()
        self._thread.join(timeout=5)


def test_wait_subcommand_removed() -> None:
    with pytest.raises(SystemExit):
        main(["wait"])


def test_bootstrap_downloads_bundle_inits_git_locks_ro_and_uploads_report(tmp_path: Path) -> None:
    token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    probe_script = tmp_path / "probe.py"
    probe_script.write_text(
        "import json, os, pathlib, subprocess\n"
        "src = pathlib.Path(os.environ['REVIEW_SRC_DIR'])\n"
        "out = pathlib.Path(os.environ['REVIEW_OUT_DIR'])\n"
        "diff_out = subprocess.check_output(['git', '-C', str(src), 'diff', 'base...pr'], text=True)\n"
        "assert '+func f() int { return 2 }' in diff_out, diff_out\n"
        "try:\n"
        "    (src / 'tamper.txt').write_text('no')\n"
        "    raise SystemExit('src was writable')\n"
        "except PermissionError:\n"
        "    pass\n"
        "report = {'summary': 'Bootstrap OK', 'findings': []}\n"
        "(out / 'raw_report.json').write_text(json.dumps(report))\n",
        encoding="utf-8",
    )

    task = review_pb2.AgentTask(
        run_id="run-boot-1",
        model_alias="review-coder",
        masked_files=["AGENTS.md"],
    )
    task.harness.profile = "boot-probe"
    task.harness.adapter = "command"
    task.harness.options["argv"] = json.dumps([sys.executable, str(probe_script)])
    task.budget.wall_clock_seconds = 30

    diff_patch = (
        "diff --git a/AGENTS.md b/AGENTS.md\n"
        "--- a/AGENTS.md\n"
        "+++ b/AGENTS.md\n"
        "@@ -1 +1 @@\n"
        "-base\n"
        "+evil\n"
        "diff --git a/main.go b/main.go\n"
        "--- a/main.go\n"
        "+++ b/main.go\n"
        "@@ -1,2 +1,2 @@\n"
        " package main\n"
        "-func f() int { return 1 }\n"
        "+func f() int { return 2 }\n"
    )
    bundle = build_bundle_tgz(
        {
            "src/main.go": b"package main\nfunc f() int { return 2 }\n",
            ".review/in/diff.patch": diff_patch.encode("utf-8"),
            ".review/in/rules/AGENTS.md": b"trusted rule\n",
            ".review/in/task.json": json_format.MessageToJson(task).encode("utf-8"),
        }
    )

    workspace = tmp_path / "ws"
    scratch = tmp_path / "scratch"
    bootstrap_file = tmp_path / "bootstrap.json"

    try:
        with MockControllerServer(bundle, token) as ctrl:
            ctrl.report_failures_remaining = 1
            bootstrap_file.write_text(
                json.dumps({"controller_url": ctrl.url, "task_token": token}),
                encoding="utf-8",
            )
            rc = run_command(
                io.BytesIO(b""),
                [
                    "--bootstrap",
                    str(bootstrap_file),
                    "--workspace",
                    str(workspace),
                    "--scratch-dir",
                    str(scratch),
                ],
            )
            assert rc == 0
            assert ctrl.bundle_fetches == 1
            assert len(ctrl.uploaded_reports) == 1

            uploaded = json_format.Parse(ctrl.uploaded_reports[0].decode("utf-8"), review_pb2.ReviewReport())
            assert uploaded.summary == "Bootstrap OK"
            assert uploaded.provenance.status == review_pb2.REPORT_STATUS_OK

        src_mode = stat.S_IMODE((workspace / "src").stat().st_mode)
        assert src_mode == 0o555
        updated_task = json_format.Parse(
            (workspace / ".review" / "in" / "task.json").read_text(encoding="utf-8"),
            review_pb2.AgentTask(),
        )
        assert updated_task.git.base_ref == "base"
        assert updated_task.git.head_ref == "pr"
        for fpath in workspace.rglob("*"):
            if fpath.is_file():
                assert token not in fpath.read_text(encoding="utf-8", errors="replace")
    finally:
        if workspace.exists():
            restore_writable(workspace)


def test_init_local_git_repo_fallback_when_patch_cannot_apply(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    (src / "main.go").write_text("package main\n", encoding="utf-8")
    bad_patch = tmp_path / "diff.patch"
    bad_patch.write_text(
        "diff --git a/missing.go b/missing.go\n--- a/missing.go\n+++ b/missing.go\n@@ -1 +1 @@\n-old\n+new\n",
        encoding="utf-8",
    )
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    base_ref = init_local_git_repo(src, bad_patch, [], scratch)
    assert base_ref == ""
    branch = subprocess.check_output(["git", "-C", str(src), "rev-parse", "--abbrev-ref", "HEAD"], text=True).strip()
    assert branch == "pr"


def test_write_report_replaces_symlink_atomically(tmp_path: Path) -> None:
    outside = tmp_path / "outside.txt"
    outside.write_text("KEEP_ME", encoding="utf-8")
    report_path = tmp_path / "out" / "report.json"
    report_path.parent.mkdir()
    os.symlink(outside, report_path)

    report = review_pb2.ReviewReport(summary="safe write")
    write_report(report, report_path)

    assert not report_path.is_symlink()
    assert outside.read_text(encoding="utf-8") == "KEEP_ME"
    parsed = json_format.Parse(report_path.read_text(encoding="utf-8"), review_pb2.ReviewReport())
    assert parsed.summary == "safe write"


def test_upload_report_rejects_symlink_fifo_and_oversized_file(tmp_path: Path) -> None:
    cfg = BootstrapConfig(controller_url="http://127.0.0.1:9", task_token="tok")

    target = tmp_path / "real.json"
    target.write_text('{"summary":"ok"}', encoding="utf-8")
    sym = tmp_path / "sym_report.json"
    os.symlink(target, sym)
    with pytest.raises(OSError):
        upload_report(cfg, sym, attempts=1)

    fifo = tmp_path / "fifo_report.json"
    os.mkfifo(fifo)
    with pytest.raises(ValueError, match="regular file"):
        upload_report(cfg, fifo, attempts=1)

    big = tmp_path / "big_report.json"
    big.write_bytes(b"a" * (2 * 1024 * 1024 + 1))
    with pytest.raises(ValueError, match="byte cap"):
        upload_report(cfg, big, attempts=1)


def test_download_bundle_enforces_stream_size_cap(tmp_path: Path) -> None:
    token = "tok-cap"
    payload = b"x" * 1024
    with MockControllerServer(payload, token) as ctrl:
        cfg = BootstrapConfig(controller_url=ctrl.url, task_token=token)
        ws = tmp_path / "ws"
        with pytest.raises(ValueError, match="byte cap"):
            download_and_extract_bundle(cfg, ws, max_bundle_bytes=128)
        assert not (ws / ".bundle.tar.gz").exists()
