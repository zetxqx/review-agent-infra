SHELL := /bin/bash
GO ?= go
UV ?= $(shell command -v uv 2>/dev/null || echo $(HOME)/.local/bin/uv)
DOCKER ?= docker

export GO111MODULE := on

BIN_DIR := bin
SANDBOX_IMAGE ?= review-agent-infra/poc-sandbox:local
CONTROLLER_IMAGE ?= ghcr.io/zetxqx/review-controller:0.1.0
ADAPTER_IMAGE ?= ghcr.io/zetxqx/review-adapter:0.1.0
SMOKE_REPO ?= .
SMOKE_SCRIPT ?= testdata/smoke/findings.json
HARNESS ?= mock-llm

.PHONY: all build proto schema test test-go test-py docker-build docker-build-control-plane k8s-apply smoke smoke-pr clean

all: build

proto:
	./scripts/gen-proto.sh

schema:
	GO111MODULE=on $(GO) run ./cmd/schemagen gen/schema/llm_review_report.schema.json
	cp gen/schema/llm_review_report.schema.json python/src/review_harness_kit/schema/llm_review_report.schema.json

build:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GO111MODULE=on $(GO) build -trimpath -o $(BIN_DIR)/loader ./cmd/loader
	CGO_ENABLED=0 GO111MODULE=on $(GO) build -trimpath -o $(BIN_DIR)/controller ./cmd/controller
	CGO_ENABLED=0 GO111MODULE=on $(GO) build -trimpath -o $(BIN_DIR)/adapter ./cmd/adapter
	GO111MODULE=on $(GO) build -trimpath -o $(BIN_DIR)/reviewctl ./cmd/reviewctl
	GO111MODULE=on $(GO) build -trimpath -o $(BIN_DIR)/schemagen ./cmd/schemagen

test: test-go test-py

test-go:
	CGO_ENABLED=0 GO111MODULE=on $(GO) build -o /dev/null ./cmd/loader
	GO111MODULE=on $(GO) test -race ./...

test-py:
	cd python && $(UV) run pytest -v
	cd python && $(UV) run ruff check

docker-build:
	$(DOCKER) build -f deploy/docker/Dockerfile.poc-sandbox -t $(SANDBOX_IMAGE) .

docker-build-control-plane:
	$(DOCKER) build -f deploy/docker/Dockerfile.controller -t $(CONTROLLER_IMAGE) .
	$(DOCKER) build -f deploy/docker/Dockerfile.adapter -t $(ADAPTER_IMAGE) .

k8s-apply:
	kubectl apply -f deploy/k8s/namespaces.yaml
	kubectl apply -f deploy/k8s/rbac.yaml
	kubectl apply -f deploy/k8s/networkpolicy.yaml
	kubectl apply -f deploy/k8s/adapter.yaml
	kubectl apply -f deploy/k8s/controller.yaml

smoke: build
	./$(BIN_DIR)/reviewctl compare --repo $(SMOKE_REPO) --commit HEAD --harnesses single-shot,mock-llm --mock-upstream

smoke-pr: build
	@test -n "$(PR)" || (echo "usage: make smoke-pr PR=<number>" >&2; exit 2)
	./$(BIN_DIR)/reviewctl review --repo . --base origin/main --head pr-$(PR) --harness $(HARNESS) --mock-upstream --mock-script $(SMOKE_SCRIPT)

clean:
	rm -rf $(BIN_DIR)
