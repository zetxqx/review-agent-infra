package orchestrator

import (
	"context"
	"testing"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/store"
	"github.com/zetxqx/review-agent-infra/internal/token"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

func TestTaskTokenOutlivesSandboxStartupAndFinalization(t *testing.T) {
	ctx := context.Background()
	key := []byte("0123456789abcdef0123456789abcdef")
	vcs := &fakeVCS{
		liveHeadSHA:  "h-exp",
		diffPatch:    []byte("diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1,2 @@\n package a\n+func A() {}\n"),
		tarballBytes: []byte("t"),
	}

	var claims *reviewv1.TaskClaims
	var wallClock int32
	sbx := fakeSandboxFunc(func(_ context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
		parsed, err := token.Verify(key, "Bearer "+spec.TaskToken)
		if err != nil {
			return nil, err
		}
		claims = parsed
		wallClock = int32(spec.TimeoutSeconds)
		return &reviewv1.ReviewReport{
			Provenance: &reviewv1.Provenance{
				Profile:    spec.HarnessProfile,
				Normalizer: reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC,
				Status:     reviewv1.ReportStatus_REPORT_STATUS_OK,
			},
		}, nil
	})

	orch := New(Config{TokenKey: key, DebounceWindow: 0}, store.NewInMemoryStore(), vcs, sbx, nil)
	if _, err := orch.HandleEvent(ctx, &reviewv1.ReviewEvent{
		EventId:      "ev-token-exp",
		EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:         "llm-d/router",
		PrNumber:     82,
		BaseSha:      "b1",
		HeadSha:      "h-exp",
		Author:       "alice",
		IsMaintainer: true,
		TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}); err != nil {
		t.Fatal(err)
	}
	minted := time.Now()
	if ran, err := orch.RunOnce(ctx); err != nil || !ran {
		t.Fatalf("RunOnce = %v err=%v", ran, err)
	}
	if claims == nil || wallClock <= 0 {
		t.Fatalf("sandbox never received a verifiable task token (claims=%v, wall clock=%d)", claims, wallClock)
	}
	lifetime := claims.Exp - minted.Unix()
	want := int64(wallClock) + 600
	if lifetime < want-5 || lifetime > want+5 {
		t.Fatalf("task token lifetime = %d s, want about %d s (wall clock %d s + 600 s)", lifetime, want, wallClock)
	}
}
