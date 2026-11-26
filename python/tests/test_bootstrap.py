from __future__ import annotations

import io
import json
import os
import stat
import subprocess
import sys
import tarfile
import threading
import urllib.request
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
from review_harness_kit.contracts import HarnessInvocation, RunContext
from review_harness_kit.entrypoint import STATIC_OFFLINE_ENV, main, run_command, run_harness
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


PROXY_ENV_NAMES = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")


def point_proxies_at_dead_port(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in PROXY_ENV_NAMES:
        monkeypatch.setenv(name, "http://127.0.0.1:9")
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.setattr(urllib.request, "_opener", None)


def test_bundle_download_and_report_upload_ignore_ambient_proxy_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    point_proxies_at_dead_port(monkeypatch)
    token = "tok-direct"
    bundle = build_bundle_tgz({"src/main.go": b"package main\n"})
    with MockControllerServer(bundle, token) as ctrl:
        cfg = BootstrapConfig(controller_url=ctrl.url, task_token=token)
        ws = tmp_path / "ws"
        download_and_extract_bundle(cfg, ws)
        assert (ws / "src" / "main.go").read_text(encoding="utf-8") == "package main\n"
        assert ctrl.bundle_fetches == 1

        report_path = tmp_path / "report.json"
        report_path.write_text('{"summary":"direct"}', encoding="utf-8")
        upload_report(cfg, report_path, attempts=1)
        assert ctrl.uploaded_reports == [b'{"summary":"direct"}']


def test_init_local_git_repo_discards_embedded_git_metadata(tmp_path: Path) -> None:
    marker = tmp_path / "filter_ran"
    src = tmp_path / "src"
    (src / ".git" / "hooks").mkdir(parents=True)
    (src / ".git" / "config").write_text(
        f'[core]\n\tfsmonitor = "touch {marker}"\n[filter "evil"]\n\tclean = "touch {marker}; cat"\n',
        encoding="utf-8",
    )
    (src / ".git" / "HEAD").write_text("ref: refs/heads/evil\n", encoding="utf-8")
    (src / ".gitattributes").write_text("* filter=evil\n", encoding="utf-8")
    (src / "main.go").write_text("package main\n", encoding="utf-8")
    (src / "vendor" / "lib" / ".git").mkdir(parents=True)
    (src / "vendor" / "lib" / ".git" / "config").write_text("[core]\n\tbare = false\n", encoding="utf-8")
    (src / "vendor" / "lib" / "lib.go").write_text("package lib\n", encoding="utf-8")
    (src / "sub").mkdir()
    (src / "sub" / ".git").write_text("gitdir: /nonexistent\n", encoding="utf-8")
    (src / "sub" / "sub.go").write_text("package sub\n", encoding="utf-8")
    scratch = tmp_path / "scratch"
    scratch.mkdir()

    assert init_local_git_repo(src, tmp_path / "missing.patch", [], scratch) == ""

    assert not marker.exists()
    assert not (src / "vendor" / "lib" / ".git").exists()
    assert not (src / "sub" / ".git").exists()
    config_text = (src / ".git" / "config").read_text(encoding="utf-8")
    assert "evil" not in config_text
    assert "fsmonitor" not in config_text
    tracked = subprocess.check_output(["git", "-C", str(src), "ls-files"], text=True).split()
    assert sorted(tracked) == [".gitattributes", "main.go", "sub/sub.go", "vendor/lib/lib.go"]


def test_run_harness_scrubs_proxy_env_and_hardens_git_config(tmp_path: Path) -> None:
    dump_script = tmp_path / "dump_env.py"
    env_out = tmp_path / "env.json"
    dump_script.write_text(
        "import json, os, sys\n"
        "with open(sys.argv[1], 'w') as fh:\n"
        "    json.dump(dict(os.environ), fh)\n",
        encoding="utf-8",
    )
    ctx = RunContext(
        base_url="http://127.0.0.1:9/v1",
        api_key="tok-harness",
        home=tmp_path / "home",
        prompt="prompt",
        rules_text="rules",
        schema_path=tmp_path / "schema.json",
    )
    invocation = HarnessInvocation(
        argv=[sys.executable, str(dump_script), str(env_out)],
        cwd=str(tmp_path),
        env={name: "http://proxy.invalid:3128" for name in PROXY_ENV_NAMES} | {"KEEP_ME": "1"},
    )
    result = run_harness(invocation, ctx, out_dir=tmp_path / "out", timeout_s=30)
    assert result.exit_code == 0

    child_env = json.loads(env_out.read_text(encoding="utf-8"))
    assert not set(PROXY_ENV_NAMES) & set(child_env)
    assert child_env["KEEP_ME"] == "1"
    assert child_env["OPENAI_API_KEY"] == "tok-harness"
    for key, val in STATIC_OFFLINE_ENV.items():
        assert child_env[key] == val
    assert child_env["GIT_CONFIG_NOSYSTEM"] == "1"
    assert child_env["GIT_CONFIG_GLOBAL"] == "/dev/null"
    count = int(child_env["GIT_CONFIG_COUNT"])
    git_config = {child_env[f"GIT_CONFIG_KEY_{i}"]: child_env[f"GIT_CONFIG_VALUE_{i}"] for i in range(count)}
    assert git_config == {"safe.directory": "*", "core.hooksPath": "/dev/null", "core.fsmonitor": "false"}

    probe_env = {key: val for key, val in child_env.items() if key in STATIC_OFFLINE_ENV or key in ("PATH", "HOME")}
    hooks_path = subprocess.check_output(
        ["git", "config", "--get", "core.hooksPath"], env=probe_env, cwd=tmp_path, text=True
    ).strip()
    assert hooks_path == "/dev/null"
