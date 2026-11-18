package store

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

func runStoreSuite(t *testing.T, s Store, advanceClock func(time.Duration)) {
	t.Helper()
	ctx := context.Background()

	evA := &reviewv1.ReviewEvent{
		EventId:     "deliv-1",
		EventKind:   reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:        "llm-d/llm-d-router",
		PrNumber:    42,
		BaseSha:     "base1",
		HeadSha:     "shaA",
		TriggerType: reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}
	runA, inserted, err := s.EnqueueOrSupersede(ctx, evA, 20*time.Second)
	if err != nil || !inserted || runA == uuid.Nil {
		t.Fatalf("enqueue A failed: id=%v inserted=%v err=%v", runA, inserted, err)
	}

	if _, dupInserted, err := s.EnqueueOrSupersede(ctx, evA, 20*time.Second); err != nil || dupInserted {
		t.Fatalf("duplicate delivery should be ignored: inserted=%v err=%v", dupInserted, err)
	}

	evADupDiffDelivery := &reviewv1.ReviewEvent{
		EventId:     "deliv-2",
		EventKind:   reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:        "llm-d/llm-d-router",
		PrNumber:    42,
		BaseSha:     "base1",
		HeadSha:     "shaA",
		TriggerType: reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}
	if _, dup2, err := s.EnqueueOrSupersede(ctx, evADupDiffDelivery, 20*time.Second); err != nil || dup2 {
		t.Fatalf("duplicate active SHA should not insert second row: inserted=%v err=%v", dup2, err)
	}

	if rec, err := s.ClaimNextRun(ctx, "w1", 30*time.Second); err != nil || rec != nil {
		t.Fatalf("expected nil claim inside debounce window, got %+v err=%v", rec, err)
	}

	reqCodex := "codex"
	evB := &reviewv1.ReviewEvent{
		EventId:          "deliv-3",
		EventKind:        reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:             "llm-d/llm-d-router",
		PrNumber:         42,
		BaseSha:          "base1",
		HeadSha:          "shaB",
		TriggerType:      reviewv1.TriggerType_TRIGGER_TYPE_MANUAL_COMMAND,
		ForceRerunSeq:    9001,
		RequestedHarness: &reqCodex,
	}
	runB, insertedB, err := s.EnqueueOrSupersede(ctx, evB, 5*time.Second)
	if err != nil || !insertedB || runB == uuid.Nil {
		t.Fatalf("enqueue B failed: %v %v", insertedB, err)
	}

	advanceClock(6 * time.Second)
	claimed, err := s.ClaimNextRun(ctx, "w1", 30*time.Second)
	if err != nil || claimed == nil || claimed.ID != runB {
		t.Fatalf("expected run B claimed after debounce, got %+v err=%v", claimed, err)
	}
	if claimed.RequestedHarness == nil || *claimed.RequestedHarness != "codex" {
		t.Fatalf("expected requested harness codex, got %v", claimed.RequestedHarness)
	}

	if ok, err := s.SetHarnessProfile(ctx, runB, "w1", "codex"); err != nil || !ok {
		t.Fatalf("set harness profile failed: ok=%v err=%v", ok, err)
	}
	if ok, err := s.RenewLease(ctx, runB, "w1", 30*time.Second); err != nil || !ok {
		t.Fatalf("renew lease failed: ok=%v err=%v", ok, err)
	}
	if ok, err := s.RenewLease(ctx, runB, "other-worker", 30*time.Second); err != nil || ok {
		t.Fatalf("wrong worker should not renew lease: ok=%v err=%v", ok, err)
	}

	if ok, err := s.ReleaseLeaseToQueued(ctx, runB, "w1"); err != nil || !ok {
		t.Fatalf("release lease to queued failed: ok=%v err=%v", ok, err)
	}
	reclaimed, err := s.ClaimNextRun(ctx, "w2", 10*time.Second)
	if err != nil || reclaimed == nil || reclaimed.ID != runB || reclaimed.Attempt != 0 {
		t.Fatalf("graceful release must preserve attempt=0, got %+v err=%v", reclaimed, err)
	}

	advanceClock(15 * time.Second)
	expiredTakeover1, err := s.ClaimNextRun(ctx, "w3", 10*time.Second)
	if err != nil || expiredTakeover1 == nil || expiredTakeover1.ID != runB || expiredTakeover1.Attempt != 1 {
		t.Fatalf("expired lease claim expected attempt=1, got %+v err=%v", expiredTakeover1, err)
	}

	finalFailed, err := s.RecordErrorOrRequeue(ctx, runB, "w3", "transient err", time.Second)
	if err != nil || finalFailed {
		t.Fatalf("expected requeue at attempt 1, got finalFailed=%v err=%v", finalFailed, err)
	}

	advanceClock(2 * time.Second)
	attempt2, err := s.ClaimNextRun(ctx, "w4", 10*time.Second)
	if err != nil || attempt2 == nil || attempt2.ID != runB || attempt2.Attempt != 2 {
		t.Fatalf("expected attempt=2 claim, got %+v err=%v", attempt2, err)
	}
	if ok, err := s.MarkPublishing(ctx, runB, "w4"); err != nil || !ok {
		t.Fatalf("mark publishing failed: ok=%v err=%v", ok, err)
	}

	if err := s.SavePublishedFingerprints(ctx, "llm-d/llm-d-router", 42, runB, "codex", []PublishedItem{
		{Fingerprint: "fp-inline-1", Category: "concurrency", CommentID: 777},
		{Fingerprint: "fp-summary-1", Category: "security", CommentID: 0},
	}); err != nil {
		t.Fatalf("save published fingerprints failed: %v", err)
	}
	unseen, err := s.FilterUnseenFingerprints(ctx, "llm-d/llm-d-router", 42, []string{"fp-inline-1", "fp-summary-1", "fp-new", "fp-new"})
	if err != nil || !slices.Equal(unseen, []string{"fp-new"}) {
		t.Fatalf("unexpected unseen fingerprints: %v err=%v", unseen, err)
	}

	if ok, err := s.MarkDoneIfActive(ctx, runB, "w4"); err != nil || !ok {
		t.Fatalf("mark done failed: ok=%v err=%v", ok, err)
	}

	if err := s.RecordHarnessExecution(ctx, HarnessExecutionRecord{
		RunID:             runB,
		ShardIndex:        0,
		Attempt:           2,
		Role:              "primary",
		HarnessProfile:    "codex",
		HarnessVersion:    "1.0.0",
		Status:            "ok",
		NormalizerPath:    "deterministic",
		FindingsRaw:       2,
		FindingsPublished: 2,
		Requests:          5,
		PromptTokens:      1000,
		CachedTokens:      600,
		CompletionTokens:  200,
		DurationMs:        4000,
		Report:            &reviewv1.ReviewReport{Summary: "ok"},
	}); err != nil {
		t.Fatalf("record execution failed: %v", err)
	}

	targets, err := s.ListOpenPRCommentsForFeedback(ctx, 24*time.Hour)
	if err != nil || len(targets) != 1 || targets[0].CommentID != 777 {
		t.Fatalf("expected 1 open PR feedback comment target, got %+v err=%v", targets, err)
	}
	if err := s.UpsertFeedback(ctx, FeedbackUpdate{
		Repo:                 "llm-d/llm-d-router",
		PRNumber:             42,
		CommentID:            777,
		ThumbsUp:             2,
		ThumbsDown:           0,
		ResolvedInNextCommit: true,
	}); err != nil {
		t.Fatalf("upsert feedback failed: %v", err)
	}

	rows, err := s.QueryHarnessScorecard(ctx, 24*time.Hour)
	if err != nil || len(rows) != 1 || rows[0].HarnessProfile != "codex" || rows[0].Resolved != 1 || rows[0].CacheHitRatio != 0.6 {
		t.Fatalf("unexpected scorecard rows: %+v err=%v", rows, err)
	}

	evBackToA := &reviewv1.ReviewEvent{
		EventId:     "deliv-4",
		EventKind:   reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:        "llm-d/llm-d-router",
		PrNumber:    42,
		BaseSha:     "base1",
		HeadSha:     "shaA",
		TriggerType: reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}
	runA2, reinserted, err := s.EnqueueOrSupersede(ctx, evBackToA, 0)
	if err != nil || !reinserted || runA2 == runA {
		t.Fatalf("expected force-push back to superseded SHA allowed: id=%v ok=%v err=%v", runA2, reinserted, err)
	}
	if err := s.SupersedeAll(ctx, "llm-d/llm-d-router", 42, "deliv-close"); err != nil {
		t.Fatalf("supersede all failed: %v", err)
	}
	if rec, err := s.ClaimNextRun(ctx, "w5", 10*time.Second); err != nil || rec != nil {
		t.Fatalf("expected closed PR queue empty, got %+v err=%v", rec, err)
	}
}

func TestInMemoryStoreWorkflowAndConcurrency(t *testing.T) {
	s := NewInMemoryStore()
	now := time.Now().UTC()
	var mu sync.Mutex
	s.SetClock(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	})
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}

	runStoreSuite(t, s, advance)

	s2 := NewInMemoryStore()
	ctx := context.Background()
	for i := range 5 {
		_, _, err := s2.EnqueueOrSupersede(ctx, &reviewv1.ReviewEvent{
			EventId:     uuid.NewString(),
			EventKind:   reviewv1.EventKind_EVENT_KIND_REVIEW,
			Repo:        "llm-d/llm-d",
			PrNumber:    int32(100 + i),
			BaseSha:     "b",
			HeadSha:     "h",
			TriggerType: reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
		}, 0)
		if err != nil {
			t.Fatal(err)
		}
	}

	var claimMu sync.Mutex
	// key: claimed run UUID string; value: true when claimed
	claimedIDs := make(map[string]bool)
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for {
				rec, err := s2.ClaimNextRun(ctx, uuid.NewString(), time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if rec == nil {
					return
				}
				claimMu.Lock()
				if claimedIDs[rec.ID.String()] {
					t.Errorf("double claim on %s by worker %d", rec.ID, idx)
				}
				claimedIDs[rec.ID.String()] = true
				claimMu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(claimedIDs) != 5 {
		t.Fatalf("expected 5 unique claims, got %d", len(claimedIDs))
	}
}
