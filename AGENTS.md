# AGENTS.md

`review-agent-infra`. Self-hosted PR review platform for `llm-d` built from Protobuf contracts (`proto/`), Go infrastructure (`cmd/`, `internal/`, `pkg/contract/`), and an in-sandbox Python adapter kit (`python/`) around unmodified coding harnesses. Repository text and commit messages are plain English only (`rg '\p{Han}'` must return zero matches).

## Commenting Policy (Strict)

Write comments **only on contracts**, never inside implementation code.

### 1. Zero Comments in Implementation Code

Never write inline comments, block comments, docstrings, or step-by-step narrative comments inside Go (`.go`) or Python (`.py`) implementation files (`cmd/`, `internal/`, `python/src/`, or tests). Express intent with clear identifiers, small functions, and explicit types. The only allowed non-contract comments are compiler or linter directives when strictly required (such as `//go:embed` or `//nolint:<linter> // <reason>`).

### 2. Comments Belong Exclusively in Contracts

Write short, factual comments strictly on contract boundaries:

* **Protobuf schemas (`proto/**/*.proto`)**: Messages, fields, enums, invariants, and numeric/length bounds.
* **Cross-module plugin interfaces (`pkg/contract/*.go`, `python/src/review_harness_kit/contracts.py`)**: Interface/Protocol contracts and method pre/postconditions.

### 3. Mandatory Go `map[string]...` Exception

Every Go `map` keyed by `string` (or a `string`-derived type)—in struct fields, type definitions, package vars, function signatures, or non-trivial local maps—**must** carry a one-line comment stating what the key represents and what the value represents:

```go
// Bad: undocumented string-keyed map and narrative comment in implementation code
type Router struct {
	images map[string]string
}

func (r *Router) Resolve(profile string) string {
	// Look up the image from the profile map and fall back to default.
	if img, ok := r.images[profile]; ok {
		return img
	}
	return r.images["default"]
}

// Good: string-keyed map documents key and value; implementation body has zero comments
type Router struct {
	// key: harness profile name (e.g. "openhands"); value: container image reference
	images map[string]string
}

func (r *Router) Resolve(profile string) string {
	if img, ok := r.images[profile]; ok {
		return img
	}
	return r.images["default"]
}
```

In Python implementation files, avoid comments and docstrings completely:

```python
# Bad: module docstring and function docstring inside implementation code
"""Normalize harness output."""

def load_report(path: Path) -> ReviewReport:
    """Read and parse report JSON."""
    return json_format.Parse(path.read_text(), ReviewReport())

# Good: self-explanatory types and naming with zero comments or docstrings
def load_report(path: Path) -> ReviewReport:
    return json_format.Parse(path.read_text(), ReviewReport())
```

## Architecture and Ownership Boundaries

* **Three Build Artifacts**: `bin/controller` (sole cluster Deployment exposing `:8080` webhooks/healthz/metrics and `:8081` sandbox bundle/report/inference API), `review-kit` (sole in-sandbox Python CLI), and `bin/reviewctl` (local evaluation CLI). `cmd/schemagen` is a build-time generator invoked via `go run ./cmd/schemagen`.
* **Contracts (`proto/review/v1/review.proto`)**: Single source of truth for cross-language messages (`ReviewEvent`, `AgentTask`, `ReviewReport`, `TaskClaims`, `SandboxUsage`). Never hand-edit `gen/` or `python/src/review_harness_kit/gen/`; edit `.proto` and run `make proto && make schema`.
* **Go infrastructure (`cmd/*`, `internal/*`, `pkg/contract/`)**: Owns webhooks, admission, stateless in-memory queue (`InMemoryStore`), host-side safe bundle building (`internal/loader`), embedded `:8081` streaming proxy and opaque-token `SessionLedger` (`internal/inference`), single-container gVisor/Docker/local sandbox runners (`internal/sandbox`), denoising/publishing with GitHub PR marker deduplication (`internal/publisher`, `internal/vcs/github`), and `reviewctl`. Keep packages strictly acyclic.
* **Python harness kit (`python/review-harness-kit`)**: Runs solely inside the disposable sandbox (`review-kit run`) to download the clean `bundle.tar.gz`, initialize offline `base`/`pr` git branches, lock `/workspace/src` and `/workspace/.review/in` read-only, wrap existing open-source harnesses (`OpenHands`, `PR-Agent`, `Codex CLI`, `mini-SWE-agent`, `OpenCode`, `Goose`, `Aider`), normalize output to `ReviewReport`, and upload `report.json` to the controller `:8081` endpoint. Do not implement a custom agent loop and do not pull orchestration or Kubernetes logic into Python.

## Security and Contract Invariants

* Host-side `internal/loader` sanitizes untrusted repository tarballs before packaging `bundle.tar.gz`: strips top-level wrapper directories, rejects path traversal, escaping symlinks, chained symlink escapes, escaping hardlinks, device nodes, and oversized files (`64 MiB` per file, `2 GiB` total), and strips untrusted instruction files (`AGENTS.md`, `AGENTS.override.md`, `.codex/`, etc.) while recording `masked_files`.
* Sandbox Pods run a single `sandbox` container under gVisor (`RestartPolicy: Never`, `DNSPolicy: None` with `127.0.0.1`, `ReadOnlyRootFilesystem: true`, `Capabilities: Drop ALL`, zero `pods/exec` RBAC). Per-Pod `v1.Secret` credentials (`controller_url` resolved to ClusterIP + 32-byte random opaque `task_token`) are mounted `0400` at `/var/run/review-task/bootstrap.json`, linked via `OwnerReferences` to the Pod UID, and deleted immediately on completion or cancellation; Docker and local runners pipe the same bootstrap JSON over stdin.
* `GET /v1/sandbox/bundle` enforces atomic one-time read (`410 Gone` on replay), `POST /v1/sandbox/report` enforces a `2 MiB` body cap, and `review-kit` locks `/workspace/src` and `/workspace/.review/in` read-only before launching any harness subprocess. Never write `task_token` to `task.json` or workspace files.
* Remember `protojson` serializes `int64` fields (`maxTotalTokens`, `normalizerReserveTokens`, `forceRerunSeq`) as JSON strings: parse `task.json` strictly with `protojson` / `google.protobuf.json_format`, and keep `int64` out of `llm_review_report.schema.json`.
* Enforce `ValidateReport` bounds, finite `confidence` in `[0, 1]`, `line >= 0`, and closed `proto3` enums before publishing.

## Git and Verification Hygiene

* Write one-sentence English commit messages only (no body paragraphs, no bullet lists, no AI attribution trailers).
* Before claiming a change is complete, run the checks matching the files you touched and report observed results:
  * `proto/`: `make proto && make schema`
  * Go: `make build` and `GO111MODULE=on go test -race ./...`
  * Python (`python/`): `uv run pytest -v` and `uv run ruff check`
  * Smoke: `make smoke`
  * Docs/repo text: `rg '\p{Han}'` (must output zero matches)
