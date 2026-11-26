package store

import (
	"context"
	"time"

	"github.com/google/uuid"
	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

const (
	StatusQueued     = "queued"
	StatusRunning    = "running"
	StatusPublishing = "publishing"
	StatusDone       = "done"
	StatusSuperseded = "superseded"
	StatusFailed     = "failed"
)

type RunRecord struct {
	ID               uuid.UUID
	Repo             string
	PRNumber         int
	PRTitle          string
	PRBody           string
	BaseSHA          string
	HeadSHA          string
	TriggerType      string
	ForceRerunSeq    int64
	RequestedHarness *string
	HarnessProfile   *string
	Status           string
	DebounceUntil    time.Time
	Attempt          int
	MaxRetries       int
	WorkerID         *string
	LeaseExpiresAt   *time.Time
	ErrorReason      *string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type PublishedItem struct {
	Fingerprint string
	Category    string
	CommentID   int64
}

type HarnessExecutionRecord struct {
	RunID             uuid.UUID
	ShardIndex        int
	Attempt           int
	Role              string
	HarnessProfile    string
	HarnessVersion    string
	Status            string
	NormalizerPath    string
	FindingsRaw       int
	FindingsPublished int
	Requests          int
	PromptTokens      int64
	CachedTokens      int64
	CompletionTokens  int64
	DurationMs        int64
	Report            *reviewv1.ReviewReport
}

type CommentTarget struct {
	Repo           string
	PRNumber       int
	CommentID      int64
	RunID          uuid.UUID
	HarnessProfile string
	Fingerprint    string
	Category       string
}

type FeedbackUpdate struct {
	Repo                 string
	PRNumber             int
	CommentID            int64
	ThumbsUp             int
	ThumbsDown           int
	ResolvedInNextCommit bool
}

type ScorecardRow struct {
	HarnessProfile           string
	Runs                     int64
	FindingsPublished        int64
	TotalTokens              int64
	CacheHitRatio            float64
	P50DurationMs            float64
	ThumbsUp                 int64
	ThumbsDown               int64
	Resolved                 int64
	ResolvedPerMillionTokens float64
}

type Store interface {
	SupersedeAll(ctx context.Context, repo string, prNumber int, deliveryID string) error
	EnqueueOrSupersede(ctx context.Context, ev *reviewv1.ReviewEvent, debounce time.Duration) (uuid.UUID, bool, error)
	ClaimNextRun(ctx context.Context, workerID string, leaseTTL time.Duration) (*RunRecord, error)
	RenewLease(ctx context.Context, runID uuid.UUID, workerID string, leaseTTL time.Duration) (bool, error)
	SetHarnessProfile(ctx context.Context, runID uuid.UUID, workerID, profile string) (bool, error)
	MarkPublishing(ctx context.Context, runID uuid.UUID, workerID string) (bool, error)
	MarkSuperseded(ctx context.Context, runID uuid.UUID) error
	MarkDoneIfActive(ctx context.Context, runID uuid.UUID, workerID string) (bool, error)
	ReleaseLeaseToQueued(ctx context.Context, runID uuid.UUID, workerID string) (bool, error)
	RecordErrorOrRequeue(ctx context.Context, runID uuid.UUID, workerID, errReason string, requeueDelay time.Duration) (bool, error)
	FilterUnseenFingerprints(ctx context.Context, repo string, prNumber int, candidates []string) ([]string, error)
	SavePublishedFingerprints(ctx context.Context, repo string, prNumber int, runID uuid.UUID, harnessProfile string, items []PublishedItem) error
	RecordHarnessExecution(ctx context.Context, exec HarnessExecutionRecord) error
	ListOpenPRCommentsForFeedback(ctx context.Context, since time.Duration) ([]CommentTarget, error)
	UpsertFeedback(ctx context.Context, fb FeedbackUpdate) error
	QueryHarnessScorecard(ctx context.Context, window time.Duration) ([]ScorecardRow, error)
}

func triggerTypeString(t reviewv1.TriggerType) string {
	if t == reviewv1.TriggerType_TRIGGER_TYPE_MANUAL_COMMAND {
		return "manual_command"
	}
	return "auto"
}
