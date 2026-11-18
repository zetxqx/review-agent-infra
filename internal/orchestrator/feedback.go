package orchestrator

import (
	"context"
	"time"

	"github.com/zetxqx/review-agent-infra/internal/store"
	"github.com/zetxqx/review-agent-infra/pkg/contract"
)

type FeedbackPoller struct {
	store    store.Store
	vcs      contract.VCSPlugin
	lookback time.Duration
}

func NewFeedbackPoller(st store.Store, vcs contract.VCSPlugin, lookback time.Duration) *FeedbackPoller {
	if lookback <= 0 {
		lookback = 30 * 24 * time.Hour
	}
	return &FeedbackPoller{
		store:    st,
		vcs:      vcs,
		lookback: lookback,
	}
}

type prGroupKey struct {
	repo string
	pr   int
}

func (fp *FeedbackPoller) PollOnce(ctx context.Context) (int, error) {
	targets, err := fp.store.ListOpenPRCommentsForFeedback(ctx, fp.lookback)
	if err != nil {
		return 0, err
	}
	if len(targets) == 0 {
		return 0, nil
	}

	grouped := make(map[prGroupKey][]int64)
	var order []prGroupKey
	for _, t := range targets {
		if t.CommentID <= 0 {
			continue
		}
		k := prGroupKey{repo: t.Repo, pr: t.PRNumber}
		if _, exists := grouped[k]; !exists {
			order = append(order, k)
		}
		grouped[k] = append(grouped[k], t.CommentID)
	}

	updated := 0
	for _, k := range order {
		samples, pollErr := fp.vcs.PollFeedback(ctx, k.repo, k.pr, grouped[k])
		if pollErr != nil {
			continue
		}
		for _, s := range samples {
			if upErr := fp.store.UpsertFeedback(ctx, store.FeedbackUpdate{
				Repo:                 k.repo,
				PRNumber:             k.pr,
				CommentID:            s.CommentID,
				ThumbsUp:             s.ThumbsUp,
				ThumbsDown:           s.ThumbsDown,
				ResolvedInNextCommit: s.ResolvedInNextCommit,
			}); upErr == nil {
				updated++
			}
		}
	}
	return updated, nil
}

func (fp *FeedbackPoller) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = fp.PollOnce(ctx)
		}
	}
}
