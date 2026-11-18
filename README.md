# review-agent-infra

A **100% self-hosted, end-to-end pluggable coding review agent** built in Go for `llm-d`.

## Documentation

* [High-Level Design (HLD)](docs/design/hld.md): Business goals, trust boundaries, five pluggable interfaces, self-hosted `llm-d` integration checklist, self-hosted tech stack, and rollout plan.
* [Concrete Go Implementation Plan](docs/design/implementation-plan.md): Go package layout and acyclic dependency graph, five plugin `interface` definitions, PostgreSQL concurrent state machine and fingerprint DDL, `client-go` gVisor Exec sandbox, Go `harness` and `adapter` pseudo-code, deduplication/publishing pipeline, and M1-M4 milestones.

## Planned Repository Layout

```text
review-agent-infra/
├── docs/design/              # High-level design (hld.md) and Go implementation plan (implementation-plan.md)
├── cmd/
│   ├── controller/           # Control plane monolith (webhooks, PG scheduler, K8s sandbox runner, publisher)
│   ├── adapter/              # External Inference Adapter (HMAC verification, budget enforcement, llm-d routing)
│   ├── harness/              # Lightweight two-stage Go agent in sandbox (static binary + offline read-only tools)
│   └── reviewctl/            # Local CLI reviewer and historical PR offline replay evaluation tool
├── pkg/contract/             # Shared pure data contracts and five plugin interfaces across binaries
├── internal/                 # Package implementations (vcs, store, sandbox, orchestrator, publisher, token, inference, llmclient, harness, tools, telemetry)
└── deploy/                   # Dockerfiles, K8s NetworkPolicy, and deployment manifests
```
