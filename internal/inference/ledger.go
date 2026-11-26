package inference

import (
	"slices"
	"sync"
	"sync/atomic"

	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

const MaxNormalizeRequests int32 = 2

type Usage struct {
	Prompt     int64
	Cached     int64
	Completion int64
}

func (u Usage) Total() int64 {
	prompt := max(0, u.Prompt)
	cached := max(0, u.Cached)
	uncachedPrompt := max(0, prompt-cached)
	completion := max(0, u.Completion)
	return uncachedPrompt + completion
}

type SessionLedger struct {
	jti               string
	expiresAtUnix     int64
	maxTotal          int64
	normalizerReserve int64
	maxRequests       int32

	committed         atomic.Int64
	requests          atomic.Int32
	normalizeRequests atomic.Int32
	rejectedRequests  atomic.Int32
	promptTokens      atomic.Int64
	cachedTokens      atomic.Int64
	completionTokens  atomic.Int64
}

func NewSessionLedger(claims *reviewv1.TaskClaims) *SessionLedger {
	return &SessionLedger{
		jti:               claims.Jti,
		expiresAtUnix:     claims.Exp,
		maxTotal:          claims.MaxTotalTokens,
		normalizerReserve: claims.NormalizerReserveTokens,
		maxRequests:       claims.MaxRequests,
	}
}

func casReserveCounter(counter *atomic.Int32, limit int32) bool {
	for {
		cur := counter.Load()
		if cur >= limit {
			return false
		}
		if counter.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (l *SessionLedger) TryReserve(estimate int64, phase string) string {
	normalizing := phase == "normalize"
	capLimit := l.maxTotal - l.normalizerReserve
	counter, reqLimit := &l.requests, l.maxRequests
	if normalizing {
		capLimit = l.maxTotal
		counter, reqLimit = &l.normalizeRequests, MaxNormalizeRequests
	}
	if !casReserveCounter(counter, reqLimit) {
		l.countRejection(normalizing)
		return "request_limit_exhausted"
	}
	for {
		cur := l.committed.Load()
		if cur+estimate > capLimit {
			counter.Add(-1)
			l.countRejection(normalizing)
			return "budget_exhausted"
		}
		if l.committed.CompareAndSwap(cur, cur+estimate) {
			return ""
		}
	}
}

func (l *SessionLedger) countRejection(normalizing bool) {
	if !normalizing {
		l.rejectedRequests.Add(1)
	}
}

func (l *SessionLedger) Settle(estimate int64, actual *Usage) {
	charge := estimate
	if actual != nil {
		charge = actual.Total()
		l.promptTokens.Add(actual.Prompt)
		l.cachedTokens.Add(actual.Cached)
		l.completionTokens.Add(actual.Completion)
	}
	l.committed.Add(charge - estimate)
}

func (l *SessionLedger) Snapshot() *reviewv1.SandboxUsage {
	return &reviewv1.SandboxUsage{
		Jti:               l.jti,
		Requests:          l.requests.Load(),
		NormalizeRequests: l.normalizeRequests.Load(),
		PromptTokens:      l.promptTokens.Load(),
		CachedTokens:      l.cachedTokens.Load(),
		CompletionTokens:  l.completionTokens.Load(),
		RejectedRequests:  l.rejectedRequests.Load(),
	}
}

type ActiveSession struct {
	token      string
	claims     *reviewv1.TaskClaims
	ledger     *SessionLedger
	bundlePath string
	bundleRead atomic.Bool

	reportMu sync.Mutex
	report   []byte
}

func (s *ActiveSession) SetReport(raw []byte) {
	s.reportMu.Lock()
	s.report = slices.Clone(raw)
	s.reportMu.Unlock()
}

func (s *ActiveSession) Report() []byte {
	s.reportMu.Lock()
	defer s.reportMu.Unlock()
	return slices.Clone(s.report)
}
