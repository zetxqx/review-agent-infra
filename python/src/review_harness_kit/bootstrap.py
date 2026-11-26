from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import tarfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from google.protobuf import json_format

from review_harness_kit.gen.review.v1 import review_pb2

DEFAULT_BOOTSTRAP_PATH = Path("/var/run/review-task/bootstrap.json")
DEFAULT_WORKSPACE_DIR = Path("/workspace")
MAX_BUNDLE_BYTES = 2 * 1024 * 1024 * 1024
MAX_REPORT_BYTES = 2 * 1024 * 1024
DIRECT_HTTP_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


@dataclass(frozen=True)
class BootstrapConfig:
    controller_url: str
    task_token: str


def parse_bootstrap_payload(raw: dict[str, object]) -> BootstrapConfig | None:
    controller_url = str(raw.get("controller_url") or "").strip().rstrip("/")
    task_token = str(raw.get("task_token") or "").strip()
    if controller_url and task_token:
        return BootstrapConfig(controller_url=controller_url, task_token=task_token)
    return None


def read_bootstrap_file(path: Path) -> BootstrapConfig:
    decoded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(decoded, dict):
        raise TypeError("bootstrap payload must be a JSON object")
    cfg = parse_bootstrap_payload(decoded)
    if cfg is None:
        raise ValueError("bootstrap payload requires controller_url and task_token")
    return cfg


def download_and_extract_bundle(
    cfg: BootstrapConfig,
    workspace_dir: Path,
    max_bundle_bytes: int = MAX_BUNDLE_BYTES,
) -> None:
    workspace_dir.mkdir(parents=True, exist_ok=True)
    bundle_tmp = workspace_dir / ".bundle.tar.gz"
    req = urllib.request.Request(
        f"{cfg.controller_url}/v1/sandbox/bundle",
        headers={"Authorization": f"Bearer {cfg.task_token}"},
        method="GET",
    )
    try:
        total_bytes = 0
        with DIRECT_HTTP_OPENER.open(req, timeout=120) as resp, bundle_tmp.open("wb") as dst:
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > max_bundle_bytes:
                    raise ValueError(f"sandbox bundle stream exceeds {max_bundle_bytes} byte cap")
                dst.write(chunk)
        with tarfile.open(bundle_tmp, mode="r:gz") as tf:
            tf.extractall(path=workspace_dir, filter="data")
    finally:
        if bundle_tmp.exists():
            bundle_tmp.unlink()


def _run_git(src_dir: Path, args: list[str], scratch_home: Path) -> subprocess.CompletedProcess[str]:
    env = {
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "HOME": str(scratch_home),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_AUTHOR_NAME": "review-kit",
        "GIT_AUTHOR_EMAIL": "kit@review.invalid",
        "GIT_COMMITTER_NAME": "review-kit",
        "GIT_COMMITTER_EMAIL": "kit@review.invalid",
        "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z",
        "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
    }
    cmd = [
        "git",
        "-c",
        "safe.directory=*",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        *args,
    ]
    return subprocess.run(cmd, cwd=src_dir, env=env, capture_output=True, text=True, check=False)


def _must_git(src_dir: Path, args: list[str], scratch_home: Path) -> str:
    proc = _run_git(src_dir, args, scratch_home)
    if proc.returncode != 0:
        raise RuntimeError(f"git {args} failed ({proc.returncode}): {proc.stderr.strip()}")
    return proc.stdout.strip()


def remove_embedded_git_metadata(src_dir: Path) -> None:
    for cur_root, dirnames, filenames in os.walk(src_dir, topdown=True, followlinks=False):
        cur_path = Path(cur_root)
        if ".git" in dirnames:
            dirnames.remove(".git")
            git_dir = cur_path / ".git"
            if git_dir.is_symlink():
                git_dir.unlink()
            else:
                shutil.rmtree(git_dir)
        if ".git" in filenames:
            (cur_path / ".git").unlink()


def init_local_git_repo(src_dir: Path, diff_file: Path, masked_files: list[str], scratch_home: Path) -> str:
    remove_embedded_git_metadata(src_dir)
    _must_git(src_dir, ["init", "-q", "-b", "pr"], scratch_home)
    _must_git(src_dir, ["add", "-A", "-f"], scratch_home)
    head_tree = _must_git(src_dir, ["write-tree"], scratch_home)

    base_commit = ""
    if diff_file.is_file() and diff_file.read_text(encoding="utf-8", errors="replace").strip():
        apply_args = ["apply", "-R", "--index", "--whitespace=nowarn"]
        for pat in masked_files:
            apply_args.append(f"--exclude={pat}")
        apply_args.append(str(diff_file))
        applied = _run_git(src_dir, apply_args, scratch_home)
        if applied.returncode == 0:
            base_tree_proc = _run_git(src_dir, ["write-tree"], scratch_home)
            if base_tree_proc.returncode == 0 and base_tree_proc.stdout.strip():
                base_commit_proc = _run_git(
                    src_dir,
                    ["commit-tree", base_tree_proc.stdout.strip(), "-m", "base"],
                    scratch_home,
                )
                if base_commit_proc.returncode == 0 and base_commit_proc.stdout.strip():
                    base_commit = base_commit_proc.stdout.strip()
                    _run_git(src_dir, ["update-ref", "refs/heads/base", base_commit], scratch_home)

    commit_args = ["commit-tree", head_tree]
    if base_commit:
        commit_args.extend(["-p", base_commit])
    commit_args.extend(["-m", "pr head"])
    head_commit = _must_git(src_dir, commit_args, scratch_home)
    _must_git(src_dir, ["update-ref", "refs/heads/pr", head_commit], scratch_home)
    _must_git(src_dir, ["read-tree", "--reset", "-u", head_commit], scratch_home)
    return "base" if base_commit else ""


def lock_readonly_recursive(root: Path) -> None:
    if not root.exists():
        return
    dirs: list[Path] = []
    for cur_root, _, filenames in os.walk(root, followlinks=False):
        cur_path = Path(cur_root)
        if not cur_path.is_symlink():
            dirs.append(cur_path)
        for name in filenames:
            fpath = cur_path / name
            if fpath.is_symlink():
                continue
            mode = fpath.lstat().st_mode
            perm = 0o555 if (mode & 0o111) else 0o444
            os.chmod(fpath, perm)
    for dpath in reversed(dirs):
        if stat.S_ISDIR(dpath.lstat().st_mode):
            os.chmod(dpath, 0o555)


def prepare_bootstrapped_workspace(
    cfg: BootstrapConfig,
    workspace_dir: Path,
    scratch_home: Path,
) -> tuple[Path, review_pb2.AgentTask]:
    download_and_extract_bundle(cfg, workspace_dir)

    src_dir = workspace_dir / "src"
    in_dir = workspace_dir / ".review" / "in"
    out_dir = workspace_dir / ".review" / "out"
    src_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    task_file = in_dir / "task.json"
    task = json_format.Parse(
        task_file.read_text(encoding="utf-8"),
        review_pb2.AgentTask(),
        ignore_unknown_fields=True,
    )
    task.src_dir = str(src_dir)
    task.diff_file = str(in_dir / "diff.patch")
    task.out_dir = str(out_dir)
    if (in_dir / "rules").exists():
        task.rules_dir = str(in_dir / "rules")
    elif (in_dir / "rules.md").is_file():
        task.rules_dir = str(in_dir / "rules.md")
    else:
        task.rules_dir = str(in_dir / "rules")
    if (in_dir / "ci_failure.log").is_file():
        task.ci_logs_file = str(in_dir / "ci_failure.log")

    base_ref = init_local_git_repo(src_dir, Path(task.diff_file), list(task.masked_files), scratch_home)
    task.git.head_ref = "pr"
    if base_ref:
        task.git.base_ref = base_ref
    else:
        task.git.ClearField("base_ref")

    task_file.write_text(json_format.MessageToJson(task, indent=2) + "\n", encoding="utf-8")

    lock_readonly_recursive(src_dir)
    lock_readonly_recursive(in_dir)
    return task_file, task


def read_report_safely(report_path: Path, max_report_bytes: int = MAX_REPORT_BYTES) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(report_path, flags)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            raise ValueError("report.json is not a regular file")
        if st.st_size > max_report_bytes:
            raise ValueError(f"report.json size {st.st_size} exceeds {max_report_bytes} byte cap")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(fd, min(65536, max_report_bytes - total + 1))
            if not chunk:
                break
            total += len(chunk)
            if total > max_report_bytes:
                raise ValueError(f"report.json read exceeds {max_report_bytes} byte cap")
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def upload_report(
    cfg: BootstrapConfig,
    report_path: Path,
    attempts: int = 3,
    max_report_bytes: int = MAX_REPORT_BYTES,
) -> None:
    payload = read_report_safely(report_path, max_report_bytes=max_report_bytes)
    url = f"{cfg.controller_url}/v1/sandbox/report"
    last_err: Exception | None = None
    for attempt in range(1, max(1, attempts) + 1):
        req = urllib.request.Request(
            url,
            data=payload,
            headers={
                "Authorization": f"Bearer {cfg.task_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with DIRECT_HTTP_OPENER.open(req, timeout=30) as resp:
                resp.read()
                if 200 <= resp.status < 300:
                    return
                last_err = RuntimeError(f"report upload returned status {resp.status}")
        except urllib.error.HTTPError as exc:
            if 400 <= exc.code < 500:
                raise
            last_err = exc
        except urllib.error.URLError as exc:
            last_err = exc
        if attempt < attempts:
            time.sleep(0.3 * attempt)
    if last_err is not None:
        raise last_err
