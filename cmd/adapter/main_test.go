package main

import (
	"testing"

	"github.com/zetxqx/review-agent-infra/internal/inference"
)

func TestBuildUpstreamAndTokenKeyResolution(t *testing.T) {
	up, err := buildUpstream(adapterFlags{upstreamKind: "llmd-gateway", upstreamURL: "http://gw:8000"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := up.(*inference.LlmdGatewayUpstream); !ok {
		t.Fatalf("expected LlmdGatewayUpstream, got %T", up)
	}

	upDirect, err := buildUpstream(adapterFlags{upstreamKind: "vllm-direct", upstreamURL: "http://vllm:8000"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := upDirect.(*inference.VllmDirectUpstream); !ok {
		t.Fatalf("expected VllmDirectUpstream, got %T", upDirect)
	}

	if _, err := buildUpstream(adapterFlags{upstreamKind: "bad"}); err == nil {
		t.Fatal("expected error for unsupported upstream kind")
	}

	k, err := resolveTokenKey(adapterFlags{tokenKeyHex: "deadbeef"})
	if err != nil || len(k) != 4 {
		t.Fatalf("hex token key resolution failed: %v %x", err, k)
	}
	if _, err := resolveTokenKey(adapterFlags{}); err == nil {
		t.Fatal("expected error when no token key provided")
	}
}
