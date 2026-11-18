package telemetry

import (
	"cmp"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
)

var DefaultDurationBuckets = []float64{1, 5, 10, 30, 60, 120, 300, 600}

type runCounterKey struct {
	repo    string
	harness string
	status  string
}

type tokenCounterKey struct {
	repo    string
	harness string
	kind    string
}

type pairKey struct {
	repo    string
	harness string
}

type rateLimitKey struct {
	repo    string
	harness string
	reason  string
}

type histogramSample struct {
	buckets []uint64
	count   uint64
	sum     float64
}

type Registry struct {
	mu              sync.Mutex
	durationBuckets []float64
	runsTotal       map[runCounterKey]uint64
	tokensTotal     map[tokenCounterKey]uint64
	cachedTotal     map[pairKey]uint64
	durations       map[pairKey]*histogramSample
	rateLimited     map[rateLimitKey]uint64
}

func NewRegistry() *Registry {
	return &Registry{
		durationBuckets: slices.Clone(DefaultDurationBuckets),
		runsTotal:       make(map[runCounterKey]uint64),
		tokensTotal:     make(map[tokenCounterKey]uint64),
		cachedTotal:     make(map[pairKey]uint64),
		durations:       make(map[pairKey]*histogramSample),
		rateLimited:     make(map[rateLimitKey]uint64),
	}
}

func (r *Registry) IncRun(repo, harness, status string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.runsTotal[runCounterKey{repo: repo, harness: harness, status: status}]++
	r.mu.Unlock()
}

func (r *Registry) AddTokens(repo, harness string, prompt, completion, cached int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	if prompt > 0 {
		r.tokensTotal[tokenCounterKey{repo: repo, harness: harness, kind: "prompt"}] += uint64(prompt)
	}
	if completion > 0 {
		r.tokensTotal[tokenCounterKey{repo: repo, harness: harness, kind: "completion"}] += uint64(completion)
	}
	if cached > 0 {
		r.cachedTotal[pairKey{repo: repo, harness: harness}] += uint64(cached)
	}
	r.mu.Unlock()
}

func (r *Registry) ObserveRunDuration(repo, harness string, seconds float64) {
	if r == nil {
		return
	}
	if seconds < 0 {
		seconds = 0
	}
	r.mu.Lock()
	k := pairKey{repo: repo, harness: harness}
	h, ok := r.durations[k]
	if !ok {
		h = &histogramSample{buckets: make([]uint64, len(r.durationBuckets))}
		r.durations[k] = h
	}
	for i, bound := range r.durationBuckets {
		if seconds <= bound {
			h.buckets[i]++
		}
	}
	h.count++
	h.sum += seconds
	r.mu.Unlock()
}

func (r *Registry) IncRateLimited(repo, harness, reason string) {
	if r == nil {
		return
	}
	if harness == "" {
		harness = "unassigned"
	}
	if reason == "" {
		reason = "policy"
	}
	r.mu.Lock()
	r.rateLimited[rateLimitKey{repo: repo, harness: harness, reason: reason}]++
	r.mu.Unlock()
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.Render()))
	})
}

func (r *Registry) Render() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	var b strings.Builder

	b.WriteString("# HELP review_runs_total Completed or terminated review runs by repository, harness, and status.\n")
	b.WriteString("# TYPE review_runs_total counter\n")
	runKeys := make([]runCounterKey, 0, len(r.runsTotal))
	for k := range r.runsTotal {
		runKeys = append(runKeys, k)
	}
	slices.SortFunc(runKeys, func(a, b runCounterKey) int {
		if c := cmp.Compare(a.repo, b.repo); c != 0 {
			return c
		}
		if c := cmp.Compare(a.harness, b.harness); c != 0 {
			return c
		}
		return cmp.Compare(a.status, b.status)
	})
	for _, k := range runKeys {
		fmt.Fprintf(&b, "review_runs_total{repo=%q,harness=%q,status=%q} %d\n", k.repo, k.harness, k.status, r.runsTotal[k])
	}

	b.WriteString("# HELP review_tokens_total Inference tokens consumed by repository, harness, and kind.\n")
	b.WriteString("# TYPE review_tokens_total counter\n")
	tokKeys := make([]tokenCounterKey, 0, len(r.tokensTotal))
	for k := range r.tokensTotal {
		tokKeys = append(tokKeys, k)
	}
	slices.SortFunc(tokKeys, func(a, b tokenCounterKey) int {
		if c := cmp.Compare(a.repo, b.repo); c != 0 {
			return c
		}
		if c := cmp.Compare(a.harness, b.harness); c != 0 {
			return c
		}
		return cmp.Compare(a.kind, b.kind)
	})
	for _, k := range tokKeys {
		fmt.Fprintf(&b, "review_tokens_total{repo=%q,harness=%q,kind=%q} %d\n", k.repo, k.harness, k.kind, r.tokensTotal[k])
	}

	b.WriteString("# HELP review_cached_tokens_total Prefix-cached prompt tokens reported by upstream per repository and harness.\n")
	b.WriteString("# TYPE review_cached_tokens_total counter\n")
	cacheKeys := make([]pairKey, 0, len(r.cachedTotal))
	for k := range r.cachedTotal {
		cacheKeys = append(cacheKeys, k)
	}
	slices.SortFunc(cacheKeys, comparePairKey)
	for _, k := range cacheKeys {
		fmt.Fprintf(&b, "review_cached_tokens_total{repo=%q,harness=%q} %d\n", k.repo, k.harness, r.cachedTotal[k])
	}

	b.WriteString("# HELP review_run_duration_seconds End-to-end sandbox and publishing duration in seconds.\n")
	b.WriteString("# TYPE review_run_duration_seconds histogram\n")
	durKeys := make([]pairKey, 0, len(r.durations))
	for k := range r.durations {
		durKeys = append(durKeys, k)
	}
	slices.SortFunc(durKeys, comparePairKey)
	for _, k := range durKeys {
		h := r.durations[k]
		for i, bound := range r.durationBuckets {
			fmt.Fprintf(&b, "review_run_duration_seconds_bucket{repo=%q,harness=%q,le=\"%g\"} %d\n", k.repo, k.harness, bound, h.buckets[i])
		}
		fmt.Fprintf(&b, "review_run_duration_seconds_bucket{repo=%q,harness=%q,le=\"+Inf\"} %d\n", k.repo, k.harness, h.count)
		fmt.Fprintf(&b, "review_run_duration_seconds_sum{repo=%q,harness=%q} %g\n", k.repo, k.harness, h.sum)
		fmt.Fprintf(&b, "review_run_duration_seconds_count{repo=%q,harness=%q} %d\n", k.repo, k.harness, h.count)
	}

	b.WriteString("# HELP review_rate_limited_total Rejected review triggers due to admission or rate limits.\n")
	b.WriteString("# TYPE review_rate_limited_total counter\n")
	rlKeys := make([]rateLimitKey, 0, len(r.rateLimited))
	for k := range r.rateLimited {
		rlKeys = append(rlKeys, k)
	}
	slices.SortFunc(rlKeys, func(a, b rateLimitKey) int {
		if c := cmp.Compare(a.repo, b.repo); c != 0 {
			return c
		}
		if c := cmp.Compare(a.harness, b.harness); c != 0 {
			return c
		}
		return cmp.Compare(a.reason, b.reason)
	})
	for _, k := range rlKeys {
		fmt.Fprintf(&b, "review_rate_limited_total{repo=%q,harness=%q,reason=%q} %d\n", k.repo, k.harness, k.reason, r.rateLimited[k])
	}

	return b.String()
}

func comparePairKey(a, b pairKey) int {
	if c := cmp.Compare(a.repo, b.repo); c != 0 {
		return c
	}
	return cmp.Compare(a.harness, b.harness)
}
