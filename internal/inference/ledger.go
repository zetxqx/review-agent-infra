package inference

import (
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

type LedgerStore struct {
	mu sync.RWMutex
	// key: task token JWT ID (jti); value: atomic per-token budget ledger
	entries map[string]*SessionLedger
}

func NewLedgerStore() *LedgerStore {
	return &LedgerStore{
		// key: task token JWT ID (jti); value: atomic per-token budget ledger
		entries: make(map[string]*SessionLedger),
	}
}

func (s *LedgerStore) GetOrCreate(claims *reviewv1.TaskClaims) *SessionLedger {
	s.mu.RLock()
	l, ok := s.entries[claims.Jti]
	s.mu.RUnlock()
	if ok {
		return l
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok = s.entries[claims.Jti]; ok {
		return l
	}
	l = NewSessionLedger(claims)
	s.entries[claims.Jti] = l
	return l
}

func (s *LedgerStore) Get(jti string) (*SessionLedger, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l, ok := s.entries[jti]
	return l, ok
}

func (s *LedgerStore) SweepExpired(nowUnix int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for k, l := range s.entries {
		if l.expiresAtUnix <= nowUnix {
			delete(s.entries, k)
			removed++
		}
	}
	return removed
}
