#!/usr/bin/env bash
set -euo pipefail

export GO111MODULE=on

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

mkdir -p gen/go gen/schema python/src/review_harness_kit/gen

tmp_bin_dir="$(mktemp -d "${ROOT_DIR}/.tmp-protoc-XXXXXX")"
trap 'rm -rf "$tmp_bin_dir"' EXIT

go build -o "${tmp_bin_dir}/protoc-gen-go" google.golang.org/protobuf/cmd/protoc-gen-go

protoc \
  -I proto \
  --plugin="protoc-gen-go=${tmp_bin_dir}/protoc-gen-go" \
  --go_out=gen/go \
  --go_opt=paths=source_relative \
  --python_out=python/src/review_harness_kit/gen \
  proto/review/v1/review.proto

touch \
  python/src/review_harness_kit/gen/__init__.py \
  python/src/review_harness_kit/gen/review/__init__.py \
  python/src/review_harness_kit/gen/review/v1/__init__.py

if [[ -f cmd/schemagen/main.go ]]; then
  go run ./cmd/schemagen gen/schema/llm_review_report.schema.json
fi
