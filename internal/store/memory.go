package store

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

var _ Store = (*InMemoryStore)(nil)

type execKey struct {
	runID   uuid.UUID
	shard   int
	attempt int
	role    string
	profile string
}

type storedExecution struct {
	rec       HarnessExecutionRecord
	createdAt time.Time
}

type storedFeedback struct {
	target    CommentTarget
	thumbsUp  int
	thumbsDn  int
	resolved  bool
	updatedAt time.Time
}

type InMemoryStore struct {
	mu  sync.Mutex
	now func() time.Time

	// key: webhook delivery_id; value: true when processed
	deliveries map[string]bool
	runs       map[uuid.UUID]*RunRecord
	runOrder   []uuid.UUID
	// key: "repo\x00pr\x00fingerprint"; value: true when published previously
	fingerprints map[string]bool
	// key: "repo\x00pr\x00commentID"; value: stored feedback row
	feedback   map[string]*storedFeedback
	executions map[execKey]*storedExecution
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		now: time.Now,
		// key: webhook delivery_id; value: true when processed
		deliveries: make(map[string]bool),
		runs:       make(map[uuid.UUID]*RunRecord),
		// key: "repo\x00pr\x00fingerprint"; value: true when published previously
		fingerprints: make(map[string]bool),
		// key: "repo\x00pr\x00commentID"; value: stored feedback row
		feedback:   make(map[string]*storedFeedback),
		executions: make(map[execKey]*storedExecution),
	}
}

func (s *InMemoryStore) SetClock(fn func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = fn
}

func fpKey(repo string, pr int, fp string) string {
	return fmt.Sprintf("%s\x00%d\x00%s", repo, pr, fp)
}

func fbKey(repo string, pr int, commentID int64) string {
	return fmt.Sprintf("%s\x00%d\x00%d", repo, pr, commentID)
}

func cloneRun(r *RunRecord) *RunRecord {
	cp := *r
	if r.RequestedHarness != nil {
		v := *r.RequestedHarness
		cp.RequestedHarness = &v
	}
	if r.HarnessProfile != nil {
		v := *r.HarnessProfile
		cp.HarnessProfile = &v
	}
	if r.WorkerID != nil {
		v := *r.WorkerID
		cp.WorkerID = &v
	}
	if r.LeaseExpiresAt != nil {
		v := *r.LeaseExpiresAt
		cp.LeaseExpiresAt = &v
	}
	if r.ErrorReason != nil {
		v := *r.ErrorReason
		cp.ErrorReason = &v
	}
	return &cp
}

func (s *InMemoryStore) SupersedeAll(_ context.Context, repo string, prNumber int, deliveryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if deliveryID != "" {
		if s.deliveries[deliveryID] {
			return nil
		}
		s.deliveries[deliveryID] = true
	}
	ts := s.now()
	for _, r := range s.runs {
		if r.Repo == repo && r.PRNumber == prNumber &&
			(r.Status == StatusQueued || r.Status == StatusRunning || r.Status == StatusPublishing) {
			r.Status = StatusSuperseded
			r.WorkerID = nil
			r.LeaseExpiresAt = nil
			r.UpdatedAt = ts
		}
	}
	return nil
}

func (s *InMemoryStore) EnqueueOrSupersede(_ context.Context, ev *reviewv1.ReviewEvent, debounce time.Duration) (uuid.UUID, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ev.EventId != "" {
		if s.deliveries[ev.EventId] {
			return uuid.Nil, false, nil
		}
		s.deliveries[ev.EventId] = true
	}
	ts := s.now()
	trig := triggerTypeString(ev.TriggerType)
	prNum := int(ev.PrNumber)

	for _, r := range s.runs {
		if r.Repo != ev.Repo || r.PRNumber != prNum {
			continue
		}
		if r.Status != StatusQueued && r.Status != StatusRunning && r.Status != StatusPublishing {
			continue
		}
		if r.BaseSHA == ev.BaseSha && r.HeadSHA == ev.HeadSha && r.TriggerType == trig && r.ForceRerunSeq == ev.ForceRerunSeq {
			continue
		}
		r.Status = StatusSuperseded
		r.WorkerID = nil
		r.LeaseExpiresAt = nil
		r.UpdatedAt = ts
	}

	for _, r := range s.runs {
		if r.Repo == ev.Repo && r.PRNumber == prNum && r.BaseSHA == ev.BaseSha &&
			r.HeadSHA == ev.HeadSha && r.TriggerType == trig && r.ForceRerunSeq == ev.ForceRerunSeq &&
			r.Status != StatusSuperseded && r.Status != StatusFailed {
			return uuid.Nil, false, nil
		}
	}

	var reqHarness *string
	if ev.RequestedHarness != nil && *ev.RequestedHarness != "" {
		v := *ev.RequestedHarness
		reqHarness = &v
	}
	id := uuid.New()
	s.runs[id] = &RunRecord{
		ID:               id,
		Repo:             ev.Repo,
		PRNumber:         prNum,
		BaseSHA:          ev.BaseSha,
		HeadSHA:          ev.HeadSha,
		TriggerType:      trig,
		ForceRerunSeq:    ev.ForceRerunSeq,
		IsFork:           ev.IsFork,
		IsMaintainer:     ev.IsMaintainer,
		RequestedHarness: reqHarness,
		Status:           StatusQueued,
		DebounceUntil:    ts.Add(debounce),
		MaxRetries:       2,
		CreatedAt:        ts,
		UpdatedAt:        ts,
	}
	s.runOrder = append(s.runOrder, id)
	return id, true, nil
}

func (s *InMemoryStore) hasActivePRRun(repo string, pr int) bool {
	for _, r := range s.runs {
		if r.Repo == repo && r.PRNumber == pr && (r.Status == StatusRunning || r.Status == StatusPublishing) {
			return true
		}
	}
	return false
}

func (s *InMemoryStore) ClaimNextRun(_ context.Context, workerID string, leaseTTL time.Duration) (*RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts := s.now()
	for _, id := range s.runOrder {
		r := s.runs[id]
		queuedReady := r.Status == StatusQueued && !r.DebounceUntil.After(ts) && !s.hasActivePRRun(r.Repo, r.PRNumber)
		expiredLease := (r.Status == StatusRunning || r.Status == StatusPublishing) && r.LeaseExpiresAt != nil && r.LeaseExpiresAt.Before(ts)
		if !queuedReady && !expiredLease {
			continue
		}
		if expiredLease && r.Attempt >= r.MaxRetries {
			reason := "lease expired max retries"
			r.Status = StatusFailed
			r.ErrorReason = &reason
			r.WorkerID = nil
			r.LeaseExpiresAt = nil
			r.UpdatedAt = ts
			continue
		}
		if expiredLease {
			r.Attempt++
		}
		w := workerID
		exp := ts.Add(leaseTTL)
		r.Status = StatusRunning
		r.WorkerID = &w
		r.LeaseExpiresAt = &exp
		r.UpdatedAt = ts
		return cloneRun(r), nil
	}
	return nil, nil
}

func isActiveWorkerRun(r *RunRecord, workerID string) bool {
	return r != nil && r.WorkerID != nil && *r.WorkerID == workerID &&
		(r.Status == StatusRunning || r.Status == StatusPublishing)
}

func (s *InMemoryStore) RenewLease(_ context.Context, runID uuid.UUID, workerID string, leaseTTL time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.runs[runID]
	if !isActiveWorkerRun(r, workerID) {
		return false, nil
	}
	ts := s.now()
	exp := ts.Add(leaseTTL)
	r.LeaseExpiresAt = &exp
	r.UpdatedAt = ts
	return true, nil
}

func (s *InMemoryStore) SetHarnessProfile(_ context.Context, runID uuid.UUID, workerID, profile string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.runs[runID]
	if !isActiveWorkerRun(r, workerID) {
		return false, nil
	}
	p := profile
	r.HarnessProfile = &p
	r.UpdatedAt = s.now()
	return true, nil
}

func (s *InMemoryStore) MarkPublishing(_ context.Context, runID uuid.UUID, workerID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.runs[runID]
	if !isActiveWorkerRun(r, workerID) {
		return false, nil
	}
	r.Status = StatusPublishing
	r.UpdatedAt = s.now()
	return true, nil
}

func (s *InMemoryStore) MarkSuperseded(_ context.Context, runID uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.runs[runID]
	if r != nil && (r.Status == StatusQueued || r.Status == StatusRunning || r.Status == StatusPublishing) {
		r.Status = StatusSuperseded
		r.WorkerID = nil
		r.LeaseExpiresAt = nil
		r.UpdatedAt = s.now()
	}
	return nil
}

func (s *InMemoryStore) MarkDoneIfActive(_ context.Context, runID uuid.UUID, workerID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.runs[runID]
	if !isActiveWorkerRun(r, workerID) {
		return false, nil
	}
	r.Status = StatusDone
	r.WorkerID = nil
	r.LeaseExpiresAt = nil
	r.UpdatedAt = s.now()
	return true, nil
}

func (s *InMemoryStore) ReleaseLeaseToQueued(_ context.Context, runID uuid.UUID, workerID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.runs[runID]
	if !isActiveWorkerRun(r, workerID) {
		return false, nil
	}
	ts := s.now()
	r.Status = StatusQueued
	r.WorkerID = nil
	r.LeaseExpiresAt = nil
	r.DebounceUntil = ts
	r.UpdatedAt = ts
	return true, nil
}

func (s *InMemoryStore) RecordErrorOrRequeue(_ context.Context, runID uuid.UUID, workerID, errReason string, requeueDelay time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.runs[runID]
	if !isActiveWorkerRun(r, workerID) {
		return false, nil
	}
	ts := s.now()
	reason := errReason
	r.ErrorReason = &reason
	r.WorkerID = nil
	r.LeaseExpiresAt = nil
	r.UpdatedAt = ts
	if r.Attempt >= r.MaxRetries {
		r.Status = StatusFailed
		return true, nil
	}
	r.Status = StatusQueued
	r.Attempt++
	r.DebounceUntil = ts.Add(requeueDelay)
	return false, nil
}

func (s *InMemoryStore) FilterUnseenFingerprints(_ context.Context, repo string, prNumber int, candidates []string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// key: fingerprint seen in this slice; value: true
	dedup := make(map[string]bool, len(candidates))
	out := make([]string, 0, len(candidates))
	for _, fp := range candidates {
		if fp == "" || dedup[fp] || s.fingerprints[fpKey(repo, prNumber, fp)] {
			continue
		}
		dedup[fp] = true
		out = append(out, fp)
	}
	return out, nil
}

func (s *InMemoryStore) SavePublishedFingerprints(_ context.Context, repo string, prNumber int, runID uuid.UUID, harnessProfile string, items []PublishedItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ts := s.now()
	for _, item := range items {
		k := fpKey(repo, prNumber, item.Fingerprint)
		if !s.fingerprints[k] {
			s.fingerprints[k] = true
		}
		if item.CommentID > 0 {
			fk := fbKey(repo, prNumber, item.CommentID)
			if _, exists := s.feedback[fk]; !exists {
				s.feedback[fk] = &storedFeedback{
					target: CommentTarget{
						Repo:           repo,
						PRNumber:       prNumber,
						CommentID:      item.CommentID,
						RunID:          runID,
						HarnessProfile: harnessProfile,
						Fingerprint:    item.Fingerprint,
						Category:       item.Category,
					},
					updatedAt: ts,
				}
			}
		}
	}
	return nil
}

func (s *InMemoryStore) RecordHarnessExecution(_ context.Context, exec HarnessExecutionRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := execKey{
		runID:   exec.RunID,
		shard:   exec.ShardIndex,
		attempt: exec.Attempt,
		role:    exec.Role,
		profile: exec.HarnessProfile,
	}
	s.executions[k] = &storedExecution{
		rec:       exec,
		createdAt: s.now(),
	}
	return nil
}

func (s *InMemoryStore) ListOpenPRCommentsForFeedback(_ context.Context, since time.Duration) ([]CommentTarget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.now().Add(-since)
	var out []CommentTarget
	for _, fb := range s.feedback {
		r := s.runs[fb.target.RunID]
		if fb.updatedAt.After(cutoff) && r != nil && r.Status == StatusDone {
			out = append(out, fb.target)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CommentID < out[j].CommentID
	})
	return out, nil
}

func (s *InMemoryStore) UpsertFeedback(_ context.Context, fb FeedbackUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.feedback[fbKey(fb.Repo, fb.PRNumber, fb.CommentID)]
	if !ok {
		return nil
	}
	row.thumbsUp = fb.ThumbsUp
	row.thumbsDn = fb.ThumbsDown
	row.resolved = row.resolved || fb.ResolvedInNextCommit
	row.updatedAt = s.now()
	return nil
}

func (s *InMemoryStore) QueryHarnessScorecard(_ context.Context, window time.Duration) ([]ScorecardRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.now().Add(-window)

	type aggEx struct {
		// key: run ID string; value: true when counted
		runs      map[string]bool
		published int64
		prompt    int64
		cached    int64
		totalTok  int64
		durations []int64
	}
	// key: harness profile name; value: execution accumulator
	byProfile := make(map[string]*aggEx)
	for _, ex := range s.executions {
		if ex.rec.Role != "primary" || !ex.createdAt.After(cutoff) {
			continue
		}
		a := byProfile[ex.rec.HarnessProfile]
		if a == nil {
			a = &aggEx{
				// key: run ID string; value: true when counted
				runs: make(map[string]bool),
			}
			byProfile[ex.rec.HarnessProfile] = a
		}
		a.runs[ex.rec.RunID.String()] = true
		a.published += int64(ex.rec.FindingsPublished)
		a.prompt += ex.rec.PromptTokens
		a.cached += ex.rec.CachedTokens
		a.totalTok += ex.rec.PromptTokens + ex.rec.CompletionTokens
		a.durations = append(a.durations, ex.rec.DurationMs)
	}

	type aggFB struct {
		up       int64
		down     int64
		resolved int64
	}
	// key: harness profile name; value: feedback accumulator
	fbByProfile := make(map[string]*aggFB)
	for _, fb := range s.feedback {
		if !fb.updatedAt.After(cutoff) {
			continue
		}
		f := fbByProfile[fb.target.HarnessProfile]
		if f == nil {
			f = &aggFB{}
			fbByProfile[fb.target.HarnessProfile] = f
		}
		f.up += int64(fb.thumbsUp)
		f.down += int64(fb.thumbsDn)
		if fb.resolved {
			f.resolved++
		}
	}

	rows := make([]ScorecardRow, 0, len(byProfile))
	for prof, ex := range byProfile {
		slices.Sort(ex.durations)
		var p50 float64
		if len(ex.durations) > 0 {
			mid := len(ex.durations) / 2
			if len(ex.durations)%2 == 1 {
				p50 = float64(ex.durations[mid])
			} else {
				p50 = float64(ex.durations[mid-1]+ex.durations[mid]) / 2.0
			}
		}
		var cacheRatio float64
		if ex.prompt > 0 {
			cacheRatio = float64(int64(float64(ex.cached)*1000/float64(ex.prompt)+0.5)) / 1000
		}
		fb := fbByProfile[prof]
		var up, dn, resolved int64
		if fb != nil {
			up, dn, resolved = fb.up, fb.down, fb.resolved
		}
		var rpm float64
		if ex.totalTok > 0 {
			rpm = float64(int64(1e6*float64(resolved)/float64(ex.totalTok)*100+0.5)) / 100
		}
		rows = append(rows, ScorecardRow{
			HarnessProfile:           prof,
			Runs:                     int64(len(ex.runs)),
			FindingsPublished:        ex.published,
			TotalTokens:              ex.totalTok,
			CacheHitRatio:            cacheRatio,
			P50DurationMs:            p50,
			ThumbsUp:                 up,
			ThumbsDown:               dn,
			Resolved:                 resolved,
			ResolvedPerMillionTokens: rpm,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ResolvedPerMillionTokens != rows[j].ResolvedPerMillionTokens {
			return rows[i].ResolvedPerMillionTokens > rows[j].ResolvedPerMillionTokens
		}
		if rows[i].FindingsPublished != rows[j].FindingsPublished {
			return rows[i].FindingsPublished > rows[j].FindingsPublished
		}
		return rows[i].HarnessProfile < rows[j].HarnessProfile
	})
	return rows, nil
}
