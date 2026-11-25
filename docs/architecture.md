# Architecture Overview

`review-agent-infra` turns untrusted pull request diffs into aligned, deduplicated review comments on self-hosted infrastructure. One Protobuf file (`proto/review/v1/review.proto`) defines the contracts across the Go control plane (`bin/controller` / `bin/reviewctl`) and the in-sandbox Python harness kit (`review-kit`).

## 1. High-Level Pipeline and Component I/O

Two runtime components move a pull request from a webhook trigger to published review comments: the stateless Go **Controller** (with embedded host bundle builder and `:8081` sandbox/inference proxy) and the disposable single-container **Sandbox** running **`review-kit`**.

```mermaid
flowchart LR
    GH["GitHub PR or Local CLI"]
    Ctrl["1. Controller (Go :8080 Webhooks + :8081 Bundle/Report/Inference Proxy)"]
    Kit["2. Sandbox (1-Container gVisor Pod: review-kit + OSS Harness)"]
    LLM["llm-d or vLLM (Model Serving)"]

    GH -->|"1. PR event, source archive, diff"| Ctrl
    Ctrl -->|"2. Pod + 0400 Secret (controller_url, task_token)"| Kit
    Kit -->|"3. GET /v1/sandbox/bundle (one-time)"| Ctrl
    Kit <-->|"4. OpenAI /v1/chat/completions or /v1/responses"| Ctrl
    Ctrl <-->|"5. Routed inference traffic"| LLM
    Kit -->|"6. POST /v1/sandbox/report (2 MiB cap)"| Ctrl
    Ctrl -->|"7. Aligned, marker-deduped PR comments"| GH
```

| Component | Entrypoint | Primary Responsibility | Inputs | Outputs |
| :--- | :--- | :--- | :--- | :--- |
| **1. Controller** | `cmd/controller` (or `cmd/reviewctl` locally) | Queues runs in memory, sanitizes & packs `bundle.tar.gz` via `internal/loader`, registers opaque random session tokens, serves `:8081` bundle/report/OpenAI proxy, denoises findings, deduplicates via GitHub PR review markers, and posts reviews | Signed GitHub webhooks (`ReviewEvent`), source tarball, `diff.patch`, `base_sha` rules, existing PR review markers, uploaded `report.json` | Clean `bundle.tar.gz`, in-memory `SessionLedger` usage, atomic GitHub PR `COMMENT` review with embedded fingerprint marker |
| **2. Harness Kit** | `review-kit` (`python/src/review_harness_kit`) | Bootstraps workspace from `GET /v1/sandbox/bundle`, builds offline `base`/`pr` Git repo, locks `/workspace/src` and `/workspace/.review/in` read-only, runs unmodified OSS coding agent under process-group timeout, normalizes output, and uploads `report.json` | Bootstrap JSON (`controller_url`, `task_token`) via `0400` Secret file or stdin, plus clean `bundle.tar.gz` | Canonical `protojson` `report.json` (`ReviewReport`) uploaded via `POST /v1/sandbox/report` |

---

## 2. Three Build Artifacts and Runtime Deployment

The repository builds two standalone Go binaries (`make build`) and one in-sandbox Python CLI (`review-kit`). `cmd/schemagen` runs at build time via `go run ./cmd/schemagen` during `make schema`.

| Artifact | Language | Execution Context | Role |
| :--- | :--- | :--- | :--- |
| `bin/controller` | Go (`CGO_ENABLED=0`) | Sole Kubernetes `Deployment` (`review-system`) | `:8080` public webhook/healthz/metrics server + `:8081` sandbox-only bundle/report/OpenAI proxy with lock-free CAS budgets, `InMemoryStore` queue, Pod orchestrator, orphan reaper, and stateless PR publisher |
| `review-kit` | Python 3.12 (`uv`) | Ephemeral 1-container gVisor Pod (`sandbox` container, `review-sandbox`) | Self-bootstrapping `CMD ["review-kit", "run"]` wrapper around `openhands`, `pr-agent`, `codex`, `mini-swe-agent`, `opencode`, `goose`, and `aider` |
| `bin/reviewctl` | Go | Developer workstation or CI job | Standalone CLI for single-commit reviews (`review`) and side-by-side multi-harness evaluation (`compare`) reusing the same `internal/loader` and `internal/inference` proxy on a loopback port |

---

## 3. End-to-End Sequence and Wire Formats

Every cross-process step uses an explicit Protobuf message from `proto/review/v1/review.proto` or a standard archive/HTTP payload.

```mermaid
sequenceDiagram
    autonumber
    participant GH as GitHub PR
    participant Ctrl as bin/controller (:8080 & :8081)
    participant K8s as Kubernetes API
    participant Kit as Sandbox Pod (review-kit + Agent CLI)
    participant LLM as llm-d / vLLM

    GH->>Ctrl: [Port 8080] Signed Webhook JSON -> review.v1.ReviewEvent
    Ctrl->>Ctrl: InMemoryStore debounce + claim lease
    GH-->>Ctrl: snapshot.tar.gz + diff.patch + base_sha rules
    Ctrl->>Ctrl: internal/loader builds clean bundle.tar.gz + registers 32-byte random task_token
    Ctrl->>K8s: Create 0400 Secret + 1-container gVisor Pod (RestartPolicy=Never), patch Secret OwnerReference
    K8s-->>Kit: Start review-kit run --bootstrap /var/run/review-task/bootstrap.json
    Kit->>Ctrl: [Port 8081] GET /v1/sandbox/bundle (Bearer task_token, one-time -> 410 Gone)
    Ctrl-->>Kit: Clean bundle.tar.gz (src/* + .review/in/task.json, diff.patch, rules/*)
    Kit->>Kit: Extract /workspace, git init base+pr, chmod a-w /workspace/src and /workspace/.review/in
    loop Multi-turn tool calls and optional 1-shot normalization
        Kit->>Ctrl: [Port 8081] POST /v1/chat/completions or /v1/responses (Bearer task_token)
        Ctrl->>LLM: Fresh request + x-session-id + fairness ID + batch objective
        LLM-->>Ctrl: SSE or JSON response with usage (cached_tokens)
        Ctrl-->>Kit: Relay response & settle atomic SessionLedger in memory
    end
    Kit->>Ctrl: [Port 8081] POST /v1/sandbox/report (Bearer task_token, 2 MiB cap)
    Kit-->>K8s: Exit 0 (PodSucceeded)
    K8s-->>Ctrl: Observe PodSucceeded, call FinishSession in memory, delete Pod & Secret
    Ctrl->>GH: GET /pulls/{pr}/reviews (extract fps marker) + GetHeadSHA + POST review with embedded fps marker
```

### Protobuf Contract Summary

| Step | Protobuf Message | Serialization and Transport | Security or Validation Invariant |
| :--- | :--- | :--- | :--- |
| **1. Trigger** | `review.v1.ReviewEvent` | In-memory Go struct parsed from verified GitHub Webhook JSON or CLI flags | Deduplicated per delivery ID and gated for untrusted first-time forks |
| **4-8. Task Input** | `review.v1.AgentTask` | `protojson` file at `/workspace/.review/in/task.json` (`int64` fields encoded as strings) | Never stores API keys, GitHub tokens, or task tokens on disk |
| **4-10. Session Policy** | `review.v1.TaskClaims` | Held strictly in controller RAM, indexed by constant-time lookup of 32-byte random `task_token` | Token expires with run and is unregistered when the sandbox finishes; no HS256 JWT signing |
| **14. Sandbox Output** | `review.v1.ReviewReport` | `protojson` uploaded via `POST /v1/sandbox/report` (plus `llm_review_report.schema.json` for 1-shot LLM fallback) | Enforced `2 MiB` HTTP body cap and validated by `contract.DecodeAndValidateReport` in Go |
| **16. Token Ledger** | `review.v1.SandboxUsage` | Returned directly in memory by `ProxyHandler.FinishSession` | Tracks prompt, cached, completion, and rejected requests outside sandbox control |

---

## 4. Trust Zones and Package Boundaries

Production deployments isolate untrusted PR code inside a disposable 1-container gVisor Pod in `review-sandbox`, separate from GitHub App credentials and upstream model routing in `review-system`.

```mermaid
flowchart TB
    subgraph Z1 ["Zone 1: External Trigger (Untrusted PR Inputs)"]
        Dev["GitHub Webhook or reviewctl CLI"]
    end

    subgraph Z2 ["Zone 2: Trusted Stateless Go Control Plane (ns: review-system)"]
        VCS["internal/vcs (github, localgit)"]
        Orch["internal/orchestrator + InMemoryStore + internal/loader"]
        Proxy["internal/inference (:8081 Bundle/Report/OpenAI Proxy + Atomic CAS Ledger)"]
        Pub["internal/publisher (Hunk Alignment, Marker Dedup, Sanitizer)"]
    end

    subgraph Z3 ["Zone 3: Disposable 1-Container gVisor Pod (ns: review-sandbox, No DNS, Zero pods/exec)"]
        Secret["0400 Secret (/var/run/review-task/bootstrap.json)"]
        Kit["review-kit + OSS Agent CLI (RO rootfs, chmod a-w /workspace/src)"]
    end

    subgraph Z4 ["Zone 4: Self-Hosted Model Serving"]
        Srv["llm-d Gateway (Session Affinity + KV Prefix Cache + Batch Priority)"]
    end

    Dev -->|"HTTPS :8080"| VCS --> Orch
    Orch -->|"Create Secret + Pod (pods + secrets RBAC only)"| Kit
    Secret --> Kit
    Kit <-->|"Sole Allowed TCP Egress (:8081)"| Proxy
    Proxy --> Srv
    Orch --> Pub --> Dev
```

For detailed design trade-offs and `llm-d` Endpoint Picker configuration, see [High-Level Design](design/hld.md).
