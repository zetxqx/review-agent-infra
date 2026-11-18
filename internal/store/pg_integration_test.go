package store

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

func startEphemeralPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker binary unavailable")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker daemon unavailable: %v", err)
	}

	out, err := exec.Command(
		"docker", "run", "-d", "--rm",
		"-p", "127.0.0.1::5432",
		"-e", "POSTGRES_USER=review",
		"-e", "POSTGRES_PASSWORD=review",
		"-e", "POSTGRES_DB=review",
		"postgres:16-alpine",
	).CombinedOutput()
	if err != nil {
		t.Skipf("cannot start postgres:16-alpine container: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	cid := strings.TrimSpace(string(out))
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", cid).Run()
	})

	portOut, err := exec.Command("docker", "port", cid, "5432/tcp").CombinedOutput()
	if err != nil {
		t.Fatalf("docker port failed: %v (%s)", err, portOut)
	}
	addr := strings.TrimSpace(strings.Split(string(portOut), "\n")[0])
	dsn := fmt.Sprintf("postgres://review:review@%s/review?sslmode=disable", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var pool *pgxpool.Pool
	for {
		pool, err = pgxpool.New(ctx, dsn)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				break
			}
			pool.Close()
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for postgres at %s: %v", addr, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestPGStoreLivePostgresLifecycleAndSkipLocked(t *testing.T) {
	pool := startEphemeralPostgres(t)
	s := NewPGStore(pool)
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate failed: %v", err)
	}

	ev := &reviewv1.ReviewEvent{
		EventId:     "pg-deliv-1",
		EventKind:   reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:        "llm-d/llm-d-router",
		PrNumber:    10,
		BaseSha:     "b1",
		HeadSha:     "h1",
		TriggerType: reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
	}
	runID, inserted, err := s.EnqueueOrSupersede(ctx, ev, 0)
	if err != nil || !inserted {
		t.Fatalf("enqueue failed: %v inserted=%v", err, inserted)
	}
	if _, dup, err := s.EnqueueOrSupersede(ctx, ev, 0); err != nil || dup {
		t.Fatalf("duplicate webhook delivery must be dropped: dup=%v err=%v", dup, err)
	}

	rec, err := s.ClaimNextRun(ctx, "worker-1", 1*time.Second)
	if err != nil || rec == nil || rec.ID != runID {
		t.Fatalf("claim failed: rec=%+v err=%v", rec, err)
	}
	if ok, err := s.SetHarnessProfile(ctx, runID, "worker-1", "openhands"); err != nil || !ok {
		t.Fatalf("set profile failed: ok=%v err=%v", ok, err)
	}
	if ok, err := s.RenewLease(ctx, runID, "worker-1", 1*time.Second); err != nil || !ok {
		t.Fatalf("renew lease failed: ok=%v err=%v", ok, err)
	}

	time.Sleep(1100 * time.Millisecond)
	reclaimed1, err := s.ClaimNextRun(ctx, "worker-2", 5*time.Second)
	if err != nil || reclaimed1 == nil || reclaimed1.ID != runID || reclaimed1.Attempt != 1 {
		t.Fatalf("expected expired lease reclaim attempt=1, got %+v err=%v", reclaimed1, err)
	}

	finalFailed, err := s.RecordErrorOrRequeue(ctx, runID, "worker-2", "sandbox err", 0)
	if err != nil || finalFailed {
		t.Fatalf("expected requeue at attempt 1, got finalFailed=%v err=%v", finalFailed, err)
	}
	attempt2, err := s.ClaimNextRun(ctx, "worker-3", 1*time.Second)
	if err != nil || attempt2 == nil || attempt2.Attempt != 2 {
		t.Fatalf("expected attempt=2 claim, got %+v err=%v", attempt2, err)
	}
	time.Sleep(1100 * time.Millisecond)
	if shouldBeNil, err := s.ClaimNextRun(ctx, "worker-4", time.Second); err != nil || shouldBeNil != nil {
		t.Fatalf("expected max-retry run marked failed and skipped, got %+v err=%v", shouldBeNil, err)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM review_runs WHERE id = $1`, runID).Scan(&status); err != nil || status != StatusFailed {
		t.Fatalf("expected status=failed after lease retries exhausted, got %q err=%v", status, err)
	}

	for i := range 6 {
		_, _, err := s.EnqueueOrSupersede(ctx, &reviewv1.ReviewEvent{
			EventId:     uuid.NewString(),
			EventKind:   reviewv1.EventKind_EVENT_KIND_REVIEW,
			Repo:        "llm-d/llm-d",
			PrNumber:    int32(500 + i),
			BaseSha:     "b",
			HeadSha:     "h",
			TriggerType: reviewv1.TriggerType_TRIGGER_TYPE_AUTO,
		}, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	// key: claimed run UUID string; value: true
	seen := make(map[string]bool)
	var wg sync.WaitGroup
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wid := uuid.NewString()
			for {
				r, err := s.ClaimNextRun(ctx, wid, 30*time.Second)
				if err != nil {
					t.Error(err)
					return
				}
				if r == nil {
					return
				}
				mu.Lock()
				if seen[r.ID.String()] {
					t.Errorf("concurrent SKIP LOCKED claimed duplicate run %s", r.ID)
				}
				seen[r.ID.String()] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != 6 {
		t.Fatalf("expected 6 concurrent SKIP LOCKED claims, got %d", len(seen))
	}

	reqProf := "pr-agent"
	doneRunID, ok, err := s.EnqueueOrSupersede(ctx, &reviewv1.ReviewEvent{
		EventId:          "pg-done-1",
		EventKind:        reviewv1.EventKind_EVENT_KIND_REVIEW,
		Repo:             "llm-d/llm-d-router",
		PrNumber:         77,
		BaseSha:          "b77",
		HeadSha:          "h77",
		TriggerType:      reviewv1.TriggerType_TRIGGER_TYPE_MANUAL_COMMAND,
		ForceRerunSeq:    88,
		RequestedHarness: &reqProf,
	}, 0)
	if err != nil || !ok {
		t.Fatalf("enqueue doneRun failed: %v %v", err, ok)
	}
	claimedDone, err := s.ClaimNextRun(ctx, "worker-pub", 30*time.Second)
	if err != nil || claimedDone == nil || claimedDone.ID != doneRunID {
		t.Fatalf("claim doneRun failed: %+v err=%v", claimedDone, err)
	}
	if ok, err := s.MarkPublishing(ctx, doneRunID, "worker-pub"); err != nil || !ok {
		t.Fatalf("mark publishing failed: ok=%v err=%v", ok, err)
	}
	if err := s.SavePublishedFingerprints(ctx, "llm-d/llm-d-router", 77, doneRunID, "pr-agent", []PublishedItem{
		{Fingerprint: "fp-pg-1", Category: "k8s", CommentID: 901},
		{Fingerprint: "fp-pg-2", Category: "style", CommentID: 0},
	}); err != nil {
		t.Fatalf("save fingerprints failed: %v", err)
	}
	unseen, err := s.FilterUnseenFingerprints(ctx, "llm-d/llm-d-router", 77, []string{"fp-pg-1", "fp-pg-2", "fp-pg-3"})
	if err != nil || len(unseen) != 1 || unseen[0] != "fp-pg-3" {
		t.Fatalf("unexpected unseen PG fingerprints: %v err=%v", unseen, err)
	}
	if ok, err := s.MarkDoneIfActive(ctx, doneRunID, "worker-pub"); err != nil || !ok {
		t.Fatalf("mark done failed: ok=%v err=%v", ok, err)
	}
	if err := s.RecordHarnessExecution(ctx, HarnessExecutionRecord{
		RunID:             doneRunID,
		ShardIndex:        0,
		Attempt:           0,
		Role:              "primary",
		HarnessProfile:    "pr-agent",
		HarnessVersion:    "0.50.0",
		Status:            "ok",
		NormalizerPath:    "deterministic",
		FindingsRaw:       2,
		FindingsPublished: 1,
		Requests:          2,
		PromptTokens:      500,
		CachedTokens:      250,
		CompletionTokens:  100,
		DurationMs:        1200,
		Report:            &reviewv1.ReviewReport{Summary: "done"},
	}); err != nil {
		t.Fatalf("record harness execution failed: %v", err)
	}
	targets, err := s.ListOpenPRCommentsForFeedback(ctx, 24*time.Hour)
	if err != nil || len(targets) != 1 || targets[0].CommentID != 901 {
		t.Fatalf("unexpected open PR comments: %+v err=%v", targets, err)
	}
	if err := s.UpsertFeedback(ctx, FeedbackUpdate{
		Repo:                 "llm-d/llm-d-router",
		PRNumber:             77,
		CommentID:            901,
		ThumbsUp:             3,
		ResolvedInNextCommit: true,
	}); err != nil {
		t.Fatalf("upsert feedback failed: %v", err)
	}
	scorecard, err := s.QueryHarnessScorecard(ctx, 24*time.Hour)
	if err != nil || len(scorecard) != 1 || scorecard[0].HarnessProfile != "pr-agent" || scorecard[0].CacheHitRatio != 0.5 || scorecard[0].Resolved != 1 {
		t.Fatalf("unexpected PG scorecard: %+v err=%v", scorecard, err)
	}
}
