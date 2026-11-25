# review-agent-infra

A **100% self-hosted, stateless, end-to-end pluggable coding review agent** for `llm-d`, consolidated into **three build artifacts**: **`bin/controller`** (sole Kubernetes Deployment), **`review-kit`** (sole in-sandbox Python CLI), and **`bin/reviewctl`** (local evaluation CLI).

It uses **Protobuf (`proto/review/v1/review.proto`)** as the single source of truth for cross-language contracts, **Go (`cmd/controller`, `cmd/reviewctl`, `internal/*`)** for all control-plane, host-side archive sanitization, embedded `:8081` streaming proxy and session transfer, single-container gVisor/Docker/local sandbox orchestration, and CLI infrastructure, and **Python (`python/review-harness-kit`)** strictly inside the isolated sandbox to wrap **existing open-source coding harnesses** (`OpenHands`, `PR-Agent`, `Codex CLI`, `mini-SWE-agent`, `OpenCode`, `Goose`, `Aider`) unmodified. Inference runs on a self-hosted `llm-d` cluster with fallback to standalone vLLM or Ollama.

## How It Fits Together

```mermaid
flowchart LR
    Proto["proto/review/v1/review.proto (make proto + make schema)"] -->|"Go & Python Types + JSON Schema"| Contracts["Shared Typed Contracts"]
    GH["GitHub PR / Local CLI"] --> Ctrl["bin/controller (:8080 Webhooks + :8081 Sandbox API + InMemoryStore)"]
    Ctrl -->|"1. Host internal/loader builds clean bundle.tar.gz"| Bundle["One-Time Bundle + Opaque Session Token"]
    Bundle -->|"2. GET /v1/sandbox/bundle (Bearer token)"| Kit["1-Container gVisor Sandbox (review-kit + Unmodified Harness)"]
    Kit <-->|"3. /v1/chat/completions or /v1/responses"| Ctrl
    Ctrl <-->|"4. Routed Inference (x-session-id + Batch Objective)"| LLMD["llm-d (Envoy + EPP + vLLM)"]
    Kit -->|"5. POST /v1/sandbox/report (2 MiB cap)"| Ctrl
    Ctrl -->|"6. Validate, Align, Marker Dedup, Sanitize, Publish"| GH
```

## Five Plugin Boundaries

| Boundary | Language | Default | Alternatives |
| :--- | :--- | :--- | :--- |
| VCS Trigger & Publisher | Go (`VCSPlugin`) | `github-app`, `local-cli` | `gitlab`, `gitea` |
| Sandbox Runner | Go (`SandboxRunner`) | `k8s-gvisor-pod` (1 container, zero `pods/exec`) | `k8s-kata`, local `docker`, `local` |
| Agent Harness Adapter | Python (`HarnessAdapter`) | `openhands`, `pr-agent` | `codex`, `mini-swe-agent`, `opencode`, `goose`, `aider`, `single-shot`, `mock-llm` |
| Context & Tooling | Go + Container `PATH` | `base_sha` rules, `diff.patch`, `rg` / `git` / `ast-grep` / `ctags` | `FetchCheckLogs` |
| Inference Endpoint Adapter | Go (`UpstreamProvider`) | `llmd-gateway` | `vllm-direct`, `sglang`, `ollama` |

## Build, Test, CLI, and Control-Plane Usage

### 1. Build Binaries & Regenerate Contracts

```bash
# Build bin/controller (CGO_ENABLED=0) and bin/reviewctl
make build

# Regenerate Go/Python Protobuf bindings and strict ReviewReport JSON Schema
make proto
make schema
```

### 2. Run Unit & End-to-End Integration Suites

```bash
# Run all Go unit + full-stack controller/CLI E2E tests (-race) and Python adapter/bootstrap/normalizer tests
make test

# Or run Go and Python suites individually
GO111MODULE=on go test -race ./...
cd python && uv run pytest -v && uv run ruff check

# Run self-contained smoke comparison
make smoke
```

### 3. Local Review & Multi-Harness Comparison (`reviewctl`)

```bash
# Side-by-side multi-harness comparison against any local git repo (self-contained mock llm-d SSE upstream)
./bin/reviewctl compare \
  --repo /path/to/llm-d-router \
  --commit HEAD \
  --harnesses single-shot,mock-llm \
  --mock-upstream

# Single-profile review against a live llm-d gateway or vLLM endpoint
./bin/reviewctl review \
  --repo /path/to/llm-d-router \
  --base main --head HEAD \
  --harness openhands \
  --upstream-kind llmd-gateway \
  --upstream-url http://127.0.0.1:8000 \
  --upstream-model Qwen/Qwen3-Coder-30B-A3B-Instruct
```

### 4. Production Control Plane & Kubernetes Deployment (`bin/controller`)

```bash
# Run the stateless controller (:8080 GitHub webhook/metrics, :8081 sandbox bundle/report/proxy)
./bin/controller \
  --listen-addr :8080 \
  --sandbox-listen-addr :8081 \
  --sandbox-runner k8s \
  --sandbox-namespace review-sandbox \
  --runtime-class gvisor \
  --controller-sandbox-url http://review-controller-sandbox.review-system.svc.cluster.local:8081 \
  --upstream-kind llmd-gateway \
  --upstream-url http://llmd-gateway.llm-d.svc:8000 \
  --upstream-model Qwen/Qwen3-Coder-30B-A3B-Instruct \
  --github-app-id "$GITHUB_APP_ID" \
  --github-private-key-file /etc/review/github-app.pem \
  --github-webhook-secret "$GITHUB_WEBHOOK_SECRET"

# Build images and apply dual-namespace hardened Kubernetes manifests (Namespaces, RBAC, NetworkPolicies, Controller, Ingress)
make docker-build docker-build-control-plane
make k8s-apply
```

## Documentation

* [Architecture Overview](docs/architecture.md): High-level pipeline, 3-artifact consolidation, end-to-end sequence with Protobuf wire formats, and trust zones.
* [Agent & Contributor Rules (AGENTS.md)](AGENTS.md): Strict contract-only commenting policy, Go `map[string]...` documentation rule, language boundaries, security invariants, and verification commands.
* [High-Level Design (HLD)](docs/design/hld.md): Architecture and trust zones, Protobuf codegen boundary, five plugin boundaries, off-the-shelf harness sandboxing, output normalization, prefix-cache matrix, `llm-d` EPP checklist, and rollout notes.

## Repository Layout

```text
review-agent-infra/
├── AGENTS.md                         # Repository engineering & commenting rules
├── Makefile                          # Build (controller + reviewctl), test, schema codegen, Docker, K8s, and smoke targets
├── proto/review/v1/review.proto      # Single source of truth for all cross-boundary contracts
├── scripts/gen-proto.sh              # Deterministic Go & Python Protobuf codegen script
├── gen/                              # Generated Go bindings and LLM ReviewReport JSON Schema
├── go.mod                            # Single Go module (controller, reviewctl, build-time schemagen)
├── cmd/                              # Go entrypoints (controller, reviewctl, build-time schemagen)
├── pkg/contract/                     # Go plugin interfaces and report bounds validator
├── internal/                         # Acyclic Go packages (store, telemetry, loader, inference, vcs, sandbox, publisher, orchestrator)
├── python/                           # Standalone uv package strictly for in-sandbox review-harness-kit
│   ├── pyproject.toml
│   ├── src/review_harness_kit/       # Bootstrapper, entrypoint, 7 HarnessAdapters, Normalizer, generated proto classes
│   └── tests/                        # Contract, bootstrap, adapter, normalizer, and CLI E2E pytest suites
├── deploy/                           # Dockerfiles (sandbox, controller) & hardened K8s YAML manifests
└── docs/                             # Architecture overview and design docs
```
