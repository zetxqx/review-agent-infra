package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/internal/store"
	"github.com/zetxqx/review-agent-infra/internal/telemetry"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

type fakeVCS struct {
	mu           sync.Mutex
	liveHeadSHA  string
	diffPatch    []byte
	tarballBytes []byte
	published    []contract.PublishRequest
	feedback     []contract.FeedbackSample
}

func (f *fakeVCS) VerifyAndParseWebhook(_ context.Context, _ http.Header, _ []byte) (*reviewv1.ReviewEvent, error) {
	return nil, nil
}

func (f *fakeVCS) GetHeadSHA(_ context.Context, _ string, _ int) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.liveHeadSHA, nil
}

func (f *fakeVCS) FetchDiffAndTarball(_ context.Context, _ string, _ int, _, _ string) ([]byte, io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.diffPatch, io.NopCloser(bytes.NewReader(f.tarballBytes)), nil
}

// Return map key: repository-relative rule file path; value: raw UTF-8 rule bytes.
func (f *fakeVCS) FetchBaseRules(_ context.Context, _, _ string, _ []string) (map[string][]byte, error) {
	// key: repository-relative rule file path; value: raw UTF-8 rule bytes
	return map[string][]byte{"AGENTS.md": []byte("Rule 1")}, nil
}

func (f *fakeVCS) PublishReview(_ context.Context, req contract.PublishRequest) (*contract.PublishResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.published = append(f.published, req)
	var out []contract.PublishedComment
	for i, ic := range req.Inlines {
		out = append(out, contract.PublishedComment{
			Fingerprint: ic.Fingerprint,
			Category:    ic.Category,
			CommentID:   int64(700 + i),
		})
	}
	return &contract.PublishResult{ReviewID: int64(len(f.published)), Comments: out}, nil
}

func (f *fakeVCS) PollFeedback(_ context.Context, _ string, _ int, _ []int64) ([]contract.FeedbackSample, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.feedback, nil
}

type fakeSandboxFunc func(ctx context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error)

func (fn fakeSandboxFunc) Run(ctx context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
	return fn(ctx, spec)
}

func TestAdmissionRoutingAndRateLimiting(t *testing.T) {
	st := store.NewInMemoryStore()
	tel := telemetry.NewRegistry()
	now := time.Now()
	cfg := Config{
		TokenKey:               []byte("0123456789abcdef0123456789abcdef"),
		MaxRunsPerRepoWindow:   2,
		MaxRunsPerAuthorWindow: 2,
		RateLimitWindow:        time.Minute,
		Now:                    func() time.Time { return now },
	}
	orch := New(cfg, st, &fakeVCS{}, fakeSandboxFunc(func(context.Context, contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
		return nil, nil
	}), tel)

	forkAuto := &reviewv1.ReviewEvent{
		EventId:     "e1",
		EventKind:   reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:        "llm-d/router",
		PrNumber:    1,
		BaseSha:     "b1",
		HeadSha:     "h1",
		IsFork:      true,
		Author:      "stranger",
		TriggerType: reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}
	res, err := orch.HandleEvent(context.Background(), forkAuto)
	if err != nil || res.Allowed || res.Reason != "untrusted_fork_auto_skip" {
		t.Fatalf("expected fork auto skip, got %+v err=%v", res, err)
	}

	nonMaintainerCmd := &reviewv1.ReviewEvent{
		EventId:      "e2",
		EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:         "llm-d/router",
		PrNumber:     1,
		BaseSha:      "b1",
		HeadSha:      "h1",
		IsFork:       true,
		IsMaintainer: false,
		TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_MANUAL_COMMAND,
	}
	res, err = orch.HandleEvent(context.Background(), nonMaintainerCmd)
	if err != nil || res.Allowed || res.Reason != "maintainer_required" {
		t.Fatalf("expected maintainer_required rejection, got %+v err=%v", res, err)
	}

	badProfile := "nonexistent-harness"
	unknownProfCmd := &reviewv1.ReviewEvent{
		EventId:          "e3",
		EventKind:        reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:             "llm-d/router",
		PrNumber:         1,
		BaseSha:          "b1",
		HeadSha:          "h1",
		IsMaintainer:     true,
		RequestedHarness: &badProfile,
		TriggerType:      reviewv1.TriggerType_TRIGGER_TYPE_MANUAL_COMMAND,
	}
	res, err = orch.HandleEvent(context.Background(), unknownProfCmd)
	if err != nil || res.Allowed || res.Reason != "unknown_harness_profile" {
		t.Fatalf("expected unknown_harness_profile rejection, got %+v err=%v", res, err)
	}

	for i := 1; i <= 2; i++ {
		validEv := &reviewv1.ReviewEvent{
			EventId:      "ok-" + string(rune('0'+i)),
			EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
			Repo:         "llm-d/router",
			PrNumber:     int32(10 + i),
			BaseSha:      "b1",
			HeadSha:      "h1",
			Author:       "alice",
			TriggeredBy:  "alice",
			IsMaintainer: true,
			TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
		}
		res, err = orch.HandleEvent(context.Background(), validEv)
		if err != nil || !res.Allowed || !res.Enqueued {
			t.Fatalf("expected valid enqueue #%d, got %+v err=%v", i, res, err)
		}
	}

	thirdEv := &reviewv1.ReviewEvent{
		EventId:      "ok-3",
		EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:         "llm-d/router",
		PrNumber:     99,
		BaseSha:      "b1",
		HeadSha:      "h1",
		Author:       "bob",
		TriggeredBy:  "bob",
		IsMaintainer: true,
		TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}
	res, err = orch.HandleEvent(context.Background(), thirdEv)
	if err != nil || res.Allowed || res.Reason != "repo_rate_limit" {
		t.Fatalf("expected repo_rate_limit on 3rd run, got %+v err=%v", res, err)
	}
	if !strings.Contains(tel.Render(), `review_rate_limited_total`) {
		t.Fatalf("expected rate_limited_total metric recorded:\n%s", tel.Render())
	}
}

func TestShardingShadowExecutionPartialFailureAndFeedbackPolling(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	tel := telemetry.NewRegistry()

	diffPatch := []byte(strings.Join([]string{
		"diff --git a/go.sum b/go.sum",
		"--- a/go.sum",
		"+++ b/go.sum",
		"@@ -1 +1,2 @@",
		" mod1 h1:aaa",
		"+mod2 h1:bbb",
		"diff --git a/pkg/a.go b/pkg/a.go",
		"--- a/pkg/a.go",
		"+++ b/pkg/a.go",
		"@@ -1,2 +1,3 @@",
		" package pkg",
		"+func A() { panic(\"bugA\") }",
		"diff --git a/pkg/b.go b/pkg/b.go",
		"--- a/pkg/b.go",
		"+++ b/pkg/b.go",
		"@@ -1,2 +1,3 @@",
		" package pkg",
		"+func B() { panic(\"bugB\") }",
	}, "\n") + "\n")

	shards := SplitDiffIntoShards(diffPatch, 10)
	if len(shards) != 2 {
		t.Fatalf("expected go.sum stripped and 2 shards for a.go + b.go, got %d", len(shards))
	}

	vcs := &fakeVCS{
		liveHeadSHA:  "head-sha-1",
		diffPatch:    diffPatch,
		tarballBytes: []byte("fake-tar"),
		feedback: []contract.FeedbackSample{
			{CommentID: 700, ThumbsUp: 3, ThumbsDown: 0},
		},
	}

	hCfg := DefaultHarnessConfig()
	smallBudget := &reviewv1.TaskBudget{
		MaxRequests:             10,
		MaxContextTokens:        30,
		MaxTotalTokens:          100000,
		NormalizerReserveTokens: 10000,
		WallClockSeconds:        60,
	}
	pPrimary := hCfg.Profiles["openhands"]
	pPrimary.Budget = smallBudget
	hCfg.Profiles["openhands"] = pPrimary

	pShadow := hCfg.Profiles["codex"]
	pShadow.Budget = smallBudget
	hCfg.Profiles["codex"] = pShadow

	hCfg.Routing = []RoutingRule{
		{
			Repos:  []string{"llm-d/router"},
			Split:  []SplitWeight{{Name: "openhands", Weight: 100}},
			Shadow: []string{"codex"},
		},
	}

	var mu sync.Mutex
	var shadowCalls int
	sbx := fakeSandboxFunc(func(_ context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
		if spec.Role == "shadow" {
			mu.Lock()
			shadowCalls++
			mu.Unlock()
			return &reviewv1.ReviewReport{
				Summary: "Shadow report must never publish",
				Provenance: &reviewv1.Provenance{
					Profile:    spec.HarnessProfile,
					Normalizer: reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC,
					Status:     reviewv1.ReportStatus_REPORT_STATUS_OK,
				},
			}, nil
		}
		if spec.ShardIndex == 1 {
			return nil, errors.New("simulated shard-1 timeout")
		}
		conf := 0.95
		return &reviewv1.ReviewReport{
			Summary: "Primary shard 0 summary.",
			Findings: []*reviewv1.ReviewFinding{
				{
					File:           "pkg/a.go",
					Line:           2,
					Side:           reviewv1.DiffSide_DIFF_SIDE_RIGHT,
					TargetLineText: `func A() { panic("bugA") }`,
					Anchor:         reviewv1.AnchorKind_ANCHOR_KIND_QUOTED,
					Severity:       reviewv1.Severity_SEVERITY_HIGH,
					Confidence:     &conf,
					Category:       reviewv1.FindingCategory_FINDING_CATEGORY_CORRECTNESS,
					Title:          "Unrecovered panic in A",
					Explanation:    "Panics crash server.",
				},
			},
			Provenance: &reviewv1.Provenance{
				Profile:    spec.HarnessProfile,
				Normalizer: reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC,
				Status:     reviewv1.ReportStatus_REPORT_STATUS_OK,
			},
		}, nil
	})

	orch := New(Config{
		TokenKey:       []byte("0123456789abcdef0123456789abcdef"),
		DebounceWindow: 0,
		Harness:        hCfg,
	}, st, vcs, sbx, tel)

	ev := &reviewv1.ReviewEvent{
		EventId:      "ev-sharded",
		EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:         "llm-d/router",
		PrNumber:     55,
		BaseSha:      "base-sha-1",
		HeadSha:      "head-sha-1",
		Author:       "alice",
		IsMaintainer: true,
		TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}
	if _, err := orch.HandleEvent(ctx, ev); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	ran, err := orch.RunOnce(ctx)
	if err != nil || !ran {
		t.Fatalf("RunOnce = %v err=%v", ran, err)
	}

	if shadowCalls != 2 {
		t.Fatalf("expected 2 shadow shard executions, got %d", shadowCalls)
	}
	if len(vcs.published) != 1 {
		t.Fatalf("expected exactly 1 primary publish (shadow never publishes), got %d", len(vcs.published))
	}
	if !strings.Contains(vcs.published[0].Summary, "shard-1") || len(vcs.published[0].Inlines) != 1 {
		t.Fatalf("expected partial failure note + 1 inline finding, got %+v", vcs.published[0])
	}

	poller := NewFeedbackPoller(st, vcs, 24*time.Hour)
	updated, err := poller.PollOnce(ctx)
	if err != nil || updated != 1 {
		t.Fatalf("PollOnce = %d err=%v, want 1", updated, err)
	}

	scorecard, err := st.QueryHarnessScorecard(ctx, 24*time.Hour)
	if err != nil {
		t.Fatalf("QueryHarnessScorecard: %v", err)
	}
	if len(scorecard) != 1 || scorecard[0].HarnessProfile != "openhands" || scorecard[0].ThumbsUp != 3 {
		t.Fatalf("unexpected primary scorecard: %+v", scorecard)
	}
}

func TestGracefulSIGTERMReleasesLeaseToQueued(t *testing.T) {
	st := store.NewInMemoryStore()
	vcs := &fakeVCS{
		liveHeadSHA:  "h1",
		diffPatch:    []byte("diff --git a/x.go b/x.go\n--- a/x.go\n+++ b/x.go\n@@ -1 +1,2 @@\n package x\n+func X() {}\n"),
		tarballBytes: []byte("t"),
	}

	parentCtx, stopParent := context.WithCancel(context.Background())
	sbx := fakeSandboxFunc(func(ctx context.Context, _ contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
		stopParent()
		<-ctx.Done()
		return nil, ctx.Err()
	})

	orch := New(Config{
		WorkerID:       "w-sigterm",
		TokenKey:       []byte("0123456789abcdef0123456789abcdef"),
		DebounceWindow: 0,
	}, st, vcs, sbx, nil)

	_, err := orch.HandleEvent(context.Background(), &reviewv1.ReviewEvent{
		EventId:      "ev-sig",
		EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:         "llm-d/router",
		PrNumber:     77,
		BaseSha:      "b1",
		HeadSha:      "h1",
		Author:       "alice",
		IsMaintainer: true,
		TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	})
	if err != nil {
		t.Fatal(err)
	}

	rec, err := st.ClaimNextRun(context.Background(), "w-sigterm", time.Minute)
	if err != nil || rec == nil {
		t.Fatalf("initial claim failed: %v", err)
	}
	orch.processRun(parentCtx, rec)

	reclaimed, err := st.ClaimNextRun(context.Background(), "w-other", time.Minute)
	if err != nil || reclaimed == nil || reclaimed.Attempt != 0 {
		t.Fatalf("expected run released back to queued with attempt=0, got %+v err=%v", reclaimed, err)
	}
}

func TestAdapterHostnameResolvedToIPLiteralForDNSDisabledSandbox(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	vcs := &fakeVCS{
		liveHeadSHA:  "h-ip",
		diffPatch:    []byte("diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1,2 @@\n package a\n+func A() {}\n"),
		tarballBytes: []byte("t"),
	}

	var seenAdapterURL string
	sbx := fakeSandboxFunc(func(_ context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
		seenAdapterURL = spec.AdapterURL
		return &reviewv1.ReviewReport{
			Provenance: &reviewv1.Provenance{
				Profile:    spec.HarnessProfile,
				Normalizer: reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC,
				Status:     reviewv1.ReportStatus_REPORT_STATUS_OK,
			},
		}, nil
	})

	orch := New(Config{
		TokenKey:          []byte("0123456789abcdef0123456789abcdef"),
		DebounceWindow:    0,
		AdapterSandboxURL: "http://localhost:8080/v1",
	}, st, vcs, sbx, nil)

	if _, err := orch.HandleEvent(ctx, &reviewv1.ReviewEvent{
		EventId:      "ev-dns-resolve",
		EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:         "llm-d/router",
		PrNumber:     81,
		BaseSha:      "b1",
		HeadSha:      "h-ip",
		Author:       "alice",
		IsMaintainer: true,
		TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}); err != nil {
		t.Fatal(err)
	}

	if ran, err := orch.RunOnce(ctx); err != nil || !ran {
		t.Fatalf("RunOnce = %v err=%v", ran, err)
	}

	parsed, err := url.Parse(seenAdapterURL)
	if err != nil {
		t.Fatalf("parse seenAdapterURL %q: %v", seenAdapterURL, err)
	}
	if net.ParseIP(parsed.Hostname()) == nil {
		t.Fatalf("expected IP literal host in SandboxSpec.AdapterURL, got %q", seenAdapterURL)
	}
	if parsed.Port() != "8080" || parsed.Path != "/v1" {
		t.Fatalf("expected port 8080 and path /v1 preserved, got %q", seenAdapterURL)
	}
}

func TestCancelInFlightIgnoresDuplicateDeliveryAndCancelsSameSHARerun(t *testing.T) {
	ctx := context.Background()
	st := store.NewInMemoryStore()
	vcs := &fakeVCS{
		liveHeadSHA:  "sha-B",
		diffPatch:    []byte("diff --git a/a.go b/a.go\n--- a/a.go\n+++ b/a.go\n@@ -1 +1,2 @@\n package a\n+func B() {}\n"),
		tarballBytes: []byte("t"),
	}

	startedB := make(chan struct{})
	canceledB := make(chan struct{})
	releaseB := make(chan struct{})
	var startOnce, cancelOnce sync.Once

	sbx := fakeSandboxFunc(func(sbxCtx context.Context, spec contract.SandboxSpec) (*reviewv1.ReviewReport, error) {
		if spec.HarnessProfile == "openhands" {
			startOnce.Do(func() { close(startedB) })
			select {
			case <-sbxCtx.Done():
				cancelOnce.Do(func() { close(canceledB) })
				return nil, sbxCtx.Err()
			case <-releaseB:
			}
		}
		return &reviewv1.ReviewReport{
			Provenance: &reviewv1.Provenance{
				Profile:    spec.HarnessProfile,
				Normalizer: reviewv1.NormalizerPath_NORMALIZER_PATH_DETERMINISTIC,
				Status:     reviewv1.ReportStatus_REPORT_STATUS_OK,
			},
		}, nil
	})

	orch := New(Config{
		TokenKey:       []byte("0123456789abcdef0123456789abcdef"),
		DebounceWindow: 0,
		PollInterval:   15 * time.Millisecond,
	}, st, vcs, sbx, nil)

	evA := &reviewv1.ReviewEvent{
		EventId:      "deliv-commit-A",
		EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:         "llm-d/router",
		PrNumber:     90,
		BaseSha:      "base0",
		HeadSha:      "sha-A",
		Author:       "alice",
		IsMaintainer: true,
		TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}
	if _, err := orch.HandleEvent(ctx, evA); err != nil {
		t.Fatal(err)
	}

	evB := &reviewv1.ReviewEvent{
		EventId:      "deliv-commit-B",
		EventKind:    reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:         "llm-d/router",
		PrNumber:     90,
		BaseSha:      "base0",
		HeadSha:      "sha-B",
		Author:       "alice",
		IsMaintainer: true,
		TriggerType:  reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}
	if _, err := orch.HandleEvent(ctx, evB); err != nil {
		t.Fatal(err)
	}

	loopCtx, stopLoop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		orch.WorkerLoop(loopCtx)
	}()
	defer func() {
		close(releaseB)
		stopLoop()
		wg.Wait()
	}()

	select {
	case <-startedB:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for sha-B auto run to start")
	}

	dupRes, err := orch.HandleEvent(ctx, evA)
	if err != nil || dupRes.Enqueued {
		t.Fatalf("expected duplicate deliv-commit-A to be ignored (Enqueued=false), got %+v err=%v", dupRes, err)
	}
	select {
	case <-canceledB:
		t.Fatal("stale duplicate delivery for sha-A wrongly cancelled active sha-B run")
	case <-time.After(100 * time.Millisecond):
	}

	reqProf := "pr-agent"
	sameSHARerun := &reviewv1.ReviewEvent{
		EventId:          "deliv-manual-rerun",
		EventKind:        reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:             "llm-d/router",
		PrNumber:         90,
		BaseSha:          "base0",
		HeadSha:          "sha-B",
		Author:           "alice",
		TriggeredBy:      "alice",
		IsMaintainer:     true,
		TriggerType:      reviewv1.TriggerType_TRIGGER_TYPE_MANUAL_COMMAND,
		ForceRerunSeq:    9001,
		RequestedHarness: &reqProf,
	}
	rerunRes, err := orch.HandleEvent(ctx, sameSHARerun)
	if err != nil || !rerunRes.Enqueued {
		t.Fatalf("expected same-SHA manual /review rerun to enqueue, got %+v err=%v", rerunRes, err)
	}
	select {
	case <-canceledB:
	case <-time.After(3 * time.Second):
		t.Fatal("expected same-SHA manual /review rerun to immediately cancel superseded auto run")
	}
}
