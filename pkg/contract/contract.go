package contract

import (
	"context"
	"io"
	"net/http"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

// SandboxSpec defines the controller endpoint, opaque session token, and execution constraints for one disposable sandbox run.
type SandboxSpec struct {
	ShortRunID     string
	ShardIndex     int
	Attempt        int
	Role           string
	HarnessProfile string
	HarnessImage   string
	ControllerURL  string
	TaskToken      string
	TimeoutSeconds int
}

// UpstreamRequest holds the sanitized HTTP destination, authoritative headers, and body emitted by UpstreamProvider.
type UpstreamRequest struct {
	URL     string
	Headers http.Header
	Body    []byte
}

// InlineComment represents one anchored review comment placed on a diff line or file.
type InlineComment struct {
	Fingerprint string
	Category    string
	File        string
	Line        int32
	Side        string
	Body        string
}

// PublishRequest bundles the atomic review payload submitted through VCSPlugin.PublishReview.
type PublishRequest struct {
	Repo         string
	PRNumber     int
	CommitID     string
	RunID        string
	Event        string
	Summary      string
	Inlines      []InlineComment
	FileComments []InlineComment
	Fingerprints []string
}

// PublishedComment records one posted comment ID mapped back to its dedup fingerprint.
type PublishedComment struct {
	Fingerprint string
	Category    string
	CommentID   int64
}

// PublishResult summarizes the review ID and posted comments returned by VCSPlugin.PublishReview.
type PublishResult struct {
	ReviewID int64
	Comments []PublishedComment
}

// FeedbackSample records reaction counts and resolution status polled for a previously posted comment.
type FeedbackSample struct {
	CommentID            int64
	ThumbsUp             int
	ThumbsDown           int
	ResolvedInNextCommit bool
}

// RunRecord identifies a claimed review run passed to ContextProviders.
type RunRecord struct {
	ID             string
	Repo           string
	PRNumber       int
	BaseSHA        string
	HeadSHA        string
	TriggerType    string
	HarnessProfile string
}

// VCSPlugin integrates webhook ingestion, repository reads, idempotent review publishing, PR marker dedup, and feedback polling.
type VCSPlugin interface {
	VerifyAndParseWebhook(ctx context.Context, headers http.Header, body []byte) (*reviewv1.ReviewEvent, error)
	GetHeadSHA(ctx context.Context, repo string, prNumber int) (string, error)
	FetchDiffAndTarball(ctx context.Context, repo string, prNumber int, baseSHA, headSHA string) (diffPatch []byte, tarball io.ReadCloser, err error)
	// FetchBaseRules returns review rule payloads keyed by repository-relative file path; value is raw rule bytes.
	FetchBaseRules(ctx context.Context, repo, baseSHA string, changedFiles []string) (map[string][]byte, error)
	ListPublishedFingerprints(ctx context.Context, repo string, prNumber int) ([]string, error)
	PublishReview(ctx context.Context, req PublishRequest) (*PublishResult, error)
	PollFeedback(ctx context.Context, repo string, prNumber int, commentIDs []int64) ([]FeedbackSample, error)
}

// CheckLogFetcher is an optional VCS extension probed by type assertion to retrieve truncated CI failure logs.
type CheckLogFetcher interface {
	FetchCheckLogs(ctx context.Context, repo string, prNumber int, headSHA string, maxBytes int) ([]byte, error)
}

// SandboxRunner launches a single isolated sandbox executing review-kit run against the controller sandbox API.
type SandboxRunner interface {
	Run(ctx context.Context, spec SandboxSpec) error
}

// ContextProvider contributes files into the sandbox context directory (/workspace/.review/in).
// inputs key: file path relative to /workspace/.review/in; value: raw file bytes.
type ContextProvider interface {
	Contribute(ctx context.Context, run *RunRecord, inputs map[string][]byte) error
}

// UpstreamProvider constructs authoritative upstream inference requests for OpenAI-compatible backends.
// payload key: allowlisted OpenAI request JSON field name; value: sanitized JSON value.
type UpstreamProvider interface {
	Build(claims *reviewv1.TaskClaims, api string, payload map[string]any) (UpstreamRequest, error)
}
