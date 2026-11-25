package orchestrator

import (
	"context"
	"testing"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/store"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

func TestTaskTokenOutlivesSandboxStartupAndFinalization(t *testing.T) {
	ctx := context.Background()
	vcs := &fakeVCS{
		liveHeadSHA:  "h-exp",
		diffPatch:    []byte("diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1,2 @@\n package a\n+func A() {}\n"),
		tarballBytes: emptyTestTarGZ(t),
	}

	broker := newFakeBroker()
	var claims *reviewv1.TaskClaims
	var wallClock int32
	sbx := fakeSandboxFunc(func(_ context.Context, spec contract.SandboxSpec) error {
		claims = broker.Claims(spec.TaskToken)
		wallClock = int32(spec.TimeoutSeconds)
		return broker.SetReport(spec.TaskToken, &reviewv1.ReviewReport{
			Provenance: &reviewv1.Provenance{
				Profile:    spec.HarnessProfile,
				Normalizer: reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC,
				Status:     reviewv1.ReportStatus_REPORT_STATUS_OK,
			},
		})
	})

	orch := New(Config{DebounceWindow: 0}, store.NewInMemoryStore(), vcs, sbx, broker, nil)
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
		t.Fatalf("sandbox never received registered session claims (claims=%v, wall clock=%d)", claims, wallClock)
	}
	lifetime := claims.Exp - minted.Unix()
	want := int64(wallClock) + 600
	if lifetime < want-5 || lifetime > want+5 {
		t.Fatalf("task token lifetime = %d s, want about %d s (wall clock %d s + 600 s)", lifetime, want, wallClock)
	}
}
