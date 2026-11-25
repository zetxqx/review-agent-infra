package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/store"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

func TestRunLifecycleIsLoggedWithUsage(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	usageSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jti":"any","requests":3,"normalizeRequests":0,"promptTokens":"1200","cachedTokens":"1000","completionTokens":"300"}`)
	}))
	defer usageSrv.Close()

	ctx := context.Background()
	vcs := &fakeVCS{
		liveHeadSHA:  "h-logs",
		diffPatch:    []byte("diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1,2 @@\n package a\n+func A() {}\n"),
		tarballBytes: []byte("t"),
	}
	sbx := fakeSandboxFunc(func(_ context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
		return &reviewv1.ReviewReport{
			Summary: "One finding.",
			Findings: []*reviewv1.ReviewFinding{{
				File:           "a.go",
				Line:           2,
				Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
				TargetLineText: "func A() {}",
				Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_QUOTED,
				Severity:       reviewv1.Severity_SEVERITY_HIGH,
				Category:       reviewv1.FindingCategory_FINDING_CATEGORY_CORRECTNESS,
				Title:          "Empty function body",
				Explanation:    "A does nothing.",
			}},
			Provenance: &reviewv1.Provenance{
				Profile:    spec.HarnessProfile,
				Normalizer: reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC,
				Status:     reviewv1.ReportStatus_REPORT_STATUS_OK,
			},
		}, nil
	})
	orch := New(Config{
		TokenKey:           []byte("0123456789abcdef0123456789abcdef"),
		DebounceWindow:     0,
		AdapterInternalURL: usageSrv.URL,
	}, store.NewInMemoryStore(), vcs, sbx, nil)

	if _, err := orch.HandleEvent(ctx, &reviewv1.ReviewEvent{
		EventId:      "ev-logs",
		EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:         "llm-d/router",
		PrNumber:     83,
		BaseSha:      "b1",
		HeadSha:      "h-logs",
		Author:       "alice",
		IsMaintainer: true,
		TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}); err != nil {
		t.Fatal(err)
	}
	if ran, err := orch.RunOnce(ctx); err != nil || !ran {
		t.Fatalf("RunOnce = %v err=%v", ran, err)
	}
	if len(vcs.published) != 1 {
		t.Fatalf("expected one published review, got %d", len(vcs.published))
	}

	// key: log message; value: decoded JSON log record
	records := map[string]map[string]any{}
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		// key: JSON log field name; value: decoded field value
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		records[fmt.Sprint(rec["msg"])] = rec
	}
	for _, msg := range []string{"run claimed", "shard finished", "review published", "run finished"} {
		rec, ok := records[msg]
		if !ok {
			t.Fatalf("missing %q log line in:\n%s", msg, buf.String())
		}
		if rec["run_id"] == "" || rec["run_id"] == nil {
			t.Fatalf("%q log line has no run_id: %v", msg, rec)
		}
	}
	shard := records["shard finished"]
	if shard["status"] != "REPORT_STATUS_OK" || shard["requests"] != float64(3) || shard["prompt_tokens"] != float64(1200) || shard["cached_tokens"] != float64(1000) || shard["completion_tokens"] != float64(300) {
		t.Fatalf("unexpected shard finished fields: %v", shard)
	}
	if records["review published"]["pr"] != float64(83) {
		t.Fatalf("unexpected review published fields: %v", records["review published"])
	}
}
