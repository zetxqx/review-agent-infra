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
* **SQL schema migrations (`migrations/*.sql`)**: Table, column, partial unique index, and lock invariants.

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

* **Contracts (`proto/review/v1/review.proto` + `buf`)**: Single source of truth for cross-language messages (`ReviewEvent`, `AgentTask`, `ReviewReport`, `TaskClaims`, `SandboxUsage`). Never hand-edit `gen/` or `python/src/review_harness_kit/gen/`; edit `.proto` and run `buf generate` plus `go run ./cmd/schemagen gen/schema/llm_review_report.schema.json`.
* **Go infrastructure (`cmd/*`, `internal/*`, `pkg/contract/`)**: Owns webhooks, admission, PostgreSQL `pgx/v5` state machine, Kubernetes `client-go` Exec sandboxes, `CGO_ENABLED=0` `cmd/loader`, streaming `cmd/adapter`, denoising/publishing, and `reviewctl`. Keep packages strictly acyclic per [implementation-plan.md](docs/design/implementation-plan.md).
* **Python harness kit (`python/review-harness-kit`)**: Runs solely inside the read-only `harness` container to wrap existing open-source harnesses (`OpenHands`, `PR-Agent`, `Codex CLI`, `mini-SWE-agent`, `OpenCode`, `Goose`, `Aider`) and normalize output to `ReviewReport`. Do not implement a custom agent loop and do not pull orchestration, K8s, or PG logic into Python.

## Security and Contract Invariants

* Keep sandbox Pod mounts `readOnly: true` for the `harness` container; only Go `cmd/loader` writes `/workspace/src` via `os.OpenRoot`, masks untrusted instruction files (`AGENTS.md`, `AGENTS.override.md`, `.codex/`, etc.), and reads `/workspace/.review/out/report.json` via `syscall.O_NOFOLLOW` with a 2 MiB cap.
* Pass task tokens over Exec stdin into harness process RAM only; never write them to `task.json` or Pod specs.
* Remember `protojson` serializes `int64` fields (`maxTotalTokens`, `normalizerReserveTokens`, `forceRerunSeq`) as JSON strings: parse `task.json` strictly with `protojson` / `google.protobuf.json_format`, keep `TaskClaims` JWTs on plain JSON with numeric `exp`, and keep `int64` out of `llm_review_report.schema.json`.
* Enforce `ValidateReport` bounds, finite `confidence` in `[0, 1]`, `line >= 0`, and closed `proto3` enums before publishing.

## Git and Verification Hygiene

* Write one-sentence English commit messages only (no body paragraphs, no bullet lists, no AI attribution trailers).
* Before claiming a change is complete, run the checks matching the files you touched and report observed results:
  * `proto/`: `buf lint`, `buf generate`, and `go run ./cmd/schemagen gen/schema/llm_review_report.schema.json`
  * Go: `go build ./...` and `go test -race ./...`
  * Python (`python/`): `uv run pytest` and `uv run ruff check`
  * Docs/repo text: `rg '\p{Han}'` (must output zero matches)
