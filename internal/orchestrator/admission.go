package orchestrator

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
)

type SlidingWindowLimiter struct {
	mu sync.Mutex
	// key: rate-limit bucket key (repo or author); value: accepted event timestamps within window
	hits map[string][]time.Time
}

func NewSlidingWindowLimiter() *SlidingWindowLimiter {
	return &SlidingWindowLimiter{
		// key: rate-limit bucket key (repo or author); value: accepted event timestamps within window
		hits: make(map[string][]time.Time),
	}
}

func (l *SlidingWindowLimiter) Allow(bucket string, now time.Time, window time.Duration, limit int) bool {
	if limit <= 0 || window <= 0 || bucket == "" {
		return true
	}
	cutoff := now.Add(-window)
	l.mu.Lock()
	defer l.mu.Unlock()

	prev := l.hits[bucket]
	kept := prev[:0]
	for _, ts := range prev {
		if ts.After(cutoff) {
			kept = append(kept, ts)
		}
	}
	if len(kept) >= limit {
		l.hits[bucket] = kept
		return false
	}
	kept = append(kept, now)
	l.hits[bucket] = kept
	return true
}

type AdmissionResult struct {
	Allowed  bool
	Action   string
	Reason   string
	RunID    uuid.UUID
	Enqueued bool
}

func (o *Orchestrator) AdmissionCheck(ev *reviewv1.ReviewEvent, now time.Time) (bool, string) {
	if ev == nil {
		return false, "nil_event"
	}
	if ev.EventKind != reviewv1.EventKind_EVENT_KIND_REVIEW {
		return true, ""
	}
	if ev.IsDraft {
		return false, "draft_pr"
	}
	if ev.IsBotAuthor {
		return false, "bot_author"
	}

	targetHarness := PickProfile(o.cfg.Harness, ev.Repo, int(ev.PrNumber), ev.RequestedHarness)

	if ev.TriggerType == reviewv1.TriggerType_TRIGGER_TYPE_MANUAL_COMMAND {
		if !ev.IsMaintainer {
			o.telemetry.IncRateLimited(ev.Repo, targetHarness, "maintainer_required")
			return false, "maintainer_required"
		}
		if ev.RequestedHarness != nil && *ev.RequestedHarness != "" {
			if _, ok := o.cfg.Harness.Profiles[*ev.RequestedHarness]; !ok {
				o.telemetry.IncRateLimited(ev.Repo, *ev.RequestedHarness, "unknown_harness_profile")
				return false, "unknown_harness_profile"
			}
		}
	} else {
		if ev.IsFork && !ev.IsMaintainer && !o.isTrustedContributor(ev.Author) {
			o.telemetry.IncRateLimited(ev.Repo, targetHarness, "untrusted_fork_auto_skip")
			return false, "untrusted_fork_auto_skip"
		}
	}

	if !o.limiter.Allow("repo:"+ev.Repo, now, o.cfg.RateLimitWindow, o.cfg.MaxRunsPerRepoWindow) {
		o.telemetry.IncRateLimited(ev.Repo, targetHarness, "repo_rate_limit")
		return false, "repo_rate_limit"
	}
	actor := ev.TriggeredBy
	if actor == "" {
		actor = ev.Author
	}
	if actor != "" && !o.limiter.Allow("actor:"+strings.ToLower(actor), now, o.cfg.RateLimitWindow, o.cfg.MaxRunsPerAuthorWindow) {
		o.telemetry.IncRateLimited(ev.Repo, targetHarness, "author_rate_limit")
		return false, "author_rate_limit"
	}
	return true, ""
}

func (o *Orchestrator) HandleEvent(ctx context.Context, ev *reviewv1.ReviewEvent) (AdmissionResult, error) {
	if ev == nil || ev.EventKind == reviewv1.EventKind_EVENT_KIND_IGNORE || ev.EventKind == reviewv1.EventKind_EVENT_KIND_UNSPECIFIED {
		return AdmissionResult{Allowed: true, Action: "ignored"}, nil
	}
	if ev.EventKind == reviewv1.EventKind_EVENT_KIND_CLOSE {
		if err := o.store.SupersedeAll(ctx, ev.Repo, int(ev.PrNumber), ev.EventId); err != nil {
			return AdmissionResult{}, err
		}
		o.cancelInFlightForPR(ev.Repo, int(ev.PrNumber), uuid.Nil)
		return AdmissionResult{Allowed: true, Action: "superseded_closed"}, nil
	}
	if ev.EventKind == reviewv1.EventKind_EVENT_KIND_FEEDBACK {
		return AdmissionResult{Allowed: true, Action: "feedback_noted"}, nil
	}

	ok, reason := o.AdmissionCheck(ev, o.cfg.Now())
	if !ok {
		return AdmissionResult{Allowed: false, Action: "rejected", Reason: reason}, nil
	}

	debounce := o.cfg.DebounceWindow
	if ev.TriggerType == reviewv1.TriggerType_TRIGGER_TYPE_MANUAL_COMMAND {
		debounce = 0
	}
	runID, inserted, err := o.store.EnqueueOrSupersede(ctx, ev, debounce)
	if err != nil {
		return AdmissionResult{}, err
	}
	if inserted {
		o.cancelInFlightForPR(ev.Repo, int(ev.PrNumber), runID)
		o.signalWake()
	}
	return AdmissionResult{
		Allowed:  true,
		Action:   "enqueued",
		RunID:    runID,
		Enqueued: inserted,
	}, nil
}

func (o *Orchestrator) isTrustedContributor(login string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(login))
	if trimmed == "" {
		return false
	}
	return slices.ContainsFunc(o.cfg.TrustedContributors, func(cand string) bool {
		return strings.EqualFold(strings.TrimSpace(cand), trimmed)
	})
}
