package orchestrator

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/store"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

func TestRejectedHarnessRequestsFailShardsWithoutDeterministicReport(t *testing.T) {
	usageSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jti":"any","requests":3,"normalizeRequests":1,"rejectedRequests":2}`)
	}))
	defer usageSrv.Close()

	cases := []struct {
		name          string
		normalizer    reviewv1.NormalizerPath
		wantPublished int
	}{
		{name: "llm normalized partial output", normalizer: reviewv1.NormalizerPath_NORMALIZER_PATH_LLM, wantPublished: 0},
		{name: "deterministic report file", normalizer: reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC, wantPublished: 1},
	}
	for i, tc := range cases {
		ctx := context.Background()
		st := store.NewInMemoryStore()
		headSHA := fmt.Sprintf("h-rejected-%d", i)
		vcs := &fakeVCS{
			liveHeadSHA:  headSHA,
			diffPatch:    []byte("diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1,2 @@\n package a\n+func A() {}\n"),
			tarballBytes: []byte("t"),
		}
		sbx := fakeSandboxFunc(func(_ context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
			return &reviewv1.ReviewReport{
				Summary: "Summary produced after the request cap was reached.",
				Findings: []*reviewv1.ReviewFinding{
					{
						File:           "a.go",
						Line:           2,
						Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
						TargetLineText: "func A() {}",
						Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_QUOTED,
						Severity:       reviewv1.Severity_SEVERITY_HIGH,
						Category:       reviewv1.FindingCategory_FINDING_CATEGORY_CORRECTNESS,
						Title:          "Empty function body",
						Explanation:    "A does nothing.",
					},
				},
				Provenance: &reviewv1.Provenance{
					Profile:    spec.HarnessProfile,
					Normalizer: tc.normalizer,
					Status:     reviewv1.ReportStatus_REPORT_STATUS_OK,
				},
			}, nil
		})

		orch := New(Config{
			TokenKey:           []byte("0123456789abcdef0123456789abcdef"),
			DebounceWindow:     0,
			AdapterInternalURL: usageSrv.URL,
		}, st, vcs, sbx, nil)

		if _, err := orch.HandleEvent(ctx, &reviewv1.ReviewEvent{
			EventId:      fmt.Sprintf("ev-rejected-%d", i),
			EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
			Repo:         "llm-d/router",
			PrNumber:     int32(90 + i),
			BaseSha:      "b1",
			HeadSha:      headSHA,
			Author:       "alice",
			IsMaintainer: true,
			TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
		}); err != nil {
			t.Fatalf("%s: HandleEvent: %v", tc.name, err)
		}
		if ran, err := orch.RunOnce(ctx); err != nil || !ran {
			t.Fatalf("%s: RunOnce = %v err=%v", tc.name, ran, err)
		}
		if len(vcs.published) != tc.wantPublished {
			t.Fatalf("%s: published reviews = %d, want %d", tc.name, len(vcs.published), tc.wantPublished)
		}
	}
}
