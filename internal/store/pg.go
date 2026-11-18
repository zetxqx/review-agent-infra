package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	reviewv1 "github.com/zetxqx/review-agent-infra/gen/go/review/v1"
	"github.com/zetxqx/review-agent-infra/migrations"
	"google.golang.org/protobuf/encoding/protojson"
)

var _ Store = (*PGStore)(nil)

type PGStore struct {
	pool *pgxpool.Pool
}

func NewPGStore(pool *pgxpool.Pool) *PGStore {
	return &PGStore{pool: pool}
}

func (s *PGStore) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, migrations.InitSQL); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PGStore) SupersedeAll(ctx context.Context, repo string, prNumber int, deliveryID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if deliveryID != "" {
		tag, err := tx.Exec(ctx, `INSERT INTO webhook_deliveries (delivery_id) VALUES ($1) ON CONFLICT DO NOTHING`, deliveryID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return tx.Commit(ctx)
		}
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), $2)`, repo, prNumber); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE review_runs SET status = 'superseded', worker_id = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE repo = $1 AND pr_number = $2 AND status IN ('queued', 'running', 'publishing')
	`, repo, prNumber); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PGStore) EnqueueOrSupersede(ctx context.Context, ev *reviewv1.ReviewEvent, debounce time.Duration) (uuid.UUID, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, false, err
	}
	defer tx.Rollback(ctx)

	if ev.EventId != "" {
		tag, err := tx.Exec(ctx, `INSERT INTO webhook_deliveries (delivery_id) VALUES ($1) ON CONFLICT DO NOTHING`, ev.EventId)
		if err != nil {
			return uuid.Nil, false, err
		}
		if tag.RowsAffected() == 0 {
			return uuid.Nil, false, tx.Commit(ctx)
		}
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), $2)`, ev.Repo, ev.PrNumber); err != nil {
		return uuid.Nil, false, err
	}

	trig := triggerTypeString(ev.TriggerType)
	if _, err := tx.Exec(ctx, `
		UPDATE review_runs SET status = 'superseded', worker_id = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE repo = $1 AND pr_number = $2
		  AND status IN ('queued', 'running', 'publishing')
		  AND NOT (base_sha = $3 AND head_sha = $4 AND trigger_type = $5 AND force_rerun_seq = $6)
	`, ev.Repo, ev.PrNumber, ev.BaseSha, ev.HeadSha, trig, ev.ForceRerunSeq); err != nil {
		return uuid.Nil, false, err
	}

	var reqHarness *string
	if ev.RequestedHarness != nil && *ev.RequestedHarness != "" {
		v := *ev.RequestedHarness
		reqHarness = &v
	}

	runID := uuid.New()
	tag, err := tx.Exec(ctx, `
		INSERT INTO review_runs (id, repo, pr_number, base_sha, head_sha, trigger_type, force_rerun_seq, requested_harness, status, debounce_until)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'queued', now() + make_interval(secs => $9))
		ON CONFLICT (repo, pr_number, base_sha, head_sha, trigger_type, force_rerun_seq)
		WHERE status NOT IN ('superseded', 'failed') DO NOTHING
	`, runID, ev.Repo, ev.PrNumber, ev.BaseSha, ev.HeadSha, trig, ev.ForceRerunSeq, reqHarness, debounce.Seconds())
	if err != nil {
		return uuid.Nil, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, false, err
	}
	if tag.RowsAffected() == 0 {
		return uuid.Nil, false, nil
	}
	return runID, true, nil
}

func (s *PGStore) ClaimNextRun(ctx context.Context, workerID string, leaseTTL time.Duration) (*RunRecord, error) {
	for {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return nil, err
		}

		var rec RunRecord
		err = tx.QueryRow(ctx, `
			SELECT id, repo, pr_number, base_sha, head_sha, trigger_type, force_rerun_seq,
			       requested_harness, harness_profile, status, debounce_until, attempt, max_retries,
			       worker_id, lease_expires_at, error_reason, created_at, updated_at
			FROM review_runs r
			WHERE (
				r.status = 'queued' AND r.debounce_until <= now()
				AND NOT EXISTS (
					SELECT 1 FROM review_runs a
					WHERE a.repo = r.repo AND a.pr_number = r.pr_number AND a.status IN ('running', 'publishing')
				)
			) OR (
				r.status IN ('running', 'publishing') AND r.lease_expires_at < now()
			)
			ORDER BY r.created_at ASC
			LIMIT 1
			FOR UPDATE OF r SKIP LOCKED
		`).Scan(
			&rec.ID, &rec.Repo, &rec.PRNumber, &rec.BaseSHA, &rec.HeadSHA, &rec.TriggerType, &rec.ForceRerunSeq,
			&rec.RequestedHarness, &rec.HarnessProfile, &rec.Status, &rec.DebounceUntil, &rec.Attempt, &rec.MaxRetries,
			&rec.WorkerID, &rec.LeaseExpiresAt, &rec.ErrorReason, &rec.CreatedAt, &rec.UpdatedAt,
		)
		if err != nil {
			tx.Rollback(ctx)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, nil
			}
			return nil, err
		}

		reclaimed := rec.Status == StatusRunning || rec.Status == StatusPublishing
		if reclaimed && rec.Attempt >= rec.MaxRetries {
			_, updErr := tx.Exec(ctx, `
				UPDATE review_runs SET status = 'failed', error_reason = 'lease expired max retries', worker_id = NULL, lease_expires_at = NULL, updated_at = now()
				WHERE id = $1
			`, rec.ID)
			if updErr != nil {
				tx.Rollback(ctx)
				return nil, updErr
			}
			if cErr := tx.Commit(ctx); cErr != nil {
				return nil, cErr
			}
			continue
		}

		if reclaimed {
			rec.Attempt++
		}
		if err := tx.QueryRow(ctx, `
			UPDATE review_runs
			SET status = 'running', attempt = $2, worker_id = $3, lease_expires_at = now() + make_interval(secs => $4), updated_at = now()
			WHERE id = $1
			RETURNING lease_expires_at, updated_at
		`, rec.ID, rec.Attempt, workerID, leaseTTL.Seconds()).Scan(&rec.LeaseExpiresAt, &rec.UpdatedAt); err != nil {
			tx.Rollback(ctx)
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		rec.Status = StatusRunning
		w := workerID
		rec.WorkerID = &w
		return &rec, nil
	}
}

func (s *PGStore) RenewLease(ctx context.Context, runID uuid.UUID, workerID string, leaseTTL time.Duration) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE review_runs
		SET lease_expires_at = now() + make_interval(secs => $3), updated_at = now()
		WHERE id = $1 AND worker_id = $2 AND status IN ('running', 'publishing')
	`, runID, workerID, leaseTTL.Seconds())
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) SetHarnessProfile(ctx context.Context, runID uuid.UUID, workerID, profile string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE review_runs
		SET harness_profile = $3, updated_at = now()
		WHERE id = $1 AND worker_id = $2 AND status IN ('running', 'publishing')
	`, runID, workerID, profile)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) MarkPublishing(ctx context.Context, runID uuid.UUID, workerID string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE review_runs
		SET status = 'publishing', updated_at = now()
		WHERE id = $1 AND worker_id = $2 AND status IN ('running', 'publishing')
	`, runID, workerID)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) MarkSuperseded(ctx context.Context, runID uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE review_runs
		SET status = 'superseded', worker_id = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND status IN ('queued', 'running', 'publishing')
	`, runID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PGStore) MarkDoneIfActive(ctx context.Context, runID uuid.UUID, workerID string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE review_runs
		SET status = 'done', worker_id = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND worker_id = $2 AND status IN ('running', 'publishing')
	`, runID, workerID)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) ReleaseLeaseToQueued(ctx context.Context, runID uuid.UUID, workerID string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE review_runs
		SET status = 'queued', worker_id = NULL, lease_expires_at = NULL, debounce_until = now(), updated_at = now()
		WHERE id = $1 AND worker_id = $2 AND status IN ('running', 'publishing')
	`, runID, workerID)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGStore) RecordErrorOrRequeue(ctx context.Context, runID uuid.UUID, workerID, errReason string, requeueDelay time.Duration) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var attempt, maxRetries int
	err = tx.QueryRow(ctx, `
		SELECT attempt, max_retries
		FROM review_runs
		WHERE id = $1 AND worker_id = $2 AND status IN ('running', 'publishing')
		FOR UPDATE
	`, runID, workerID).Scan(&attempt, &maxRetries)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, tx.Commit(ctx)
		}
		return false, err
	}

	if attempt >= maxRetries {
		tag, err := tx.Exec(ctx, `
			UPDATE review_runs
			SET status = 'failed', error_reason = $3, worker_id = NULL, lease_expires_at = NULL, updated_at = now()
			WHERE id = $1 AND worker_id = $2 AND status IN ('running', 'publishing')
		`, runID, workerID, errReason)
		if err != nil {
			return false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return tag.RowsAffected() == 1, nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE review_runs
		SET status = 'queued', attempt = attempt + 1, debounce_until = now() + make_interval(secs => $3),
		    error_reason = $4, worker_id = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE id = $1 AND worker_id = $2 AND status IN ('running', 'publishing')
	`, runID, workerID, requeueDelay.Seconds(), errReason); err != nil {
		return false, err
	}
	return false, tx.Commit(ctx)
}

func (s *PGStore) FilterUnseenFingerprints(ctx context.Context, repo string, prNumber int, candidates []string) ([]string, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT fingerprint
		FROM finding_fingerprints
		WHERE repo = $1 AND pr_number = $2 AND fingerprint = ANY($3)
	`, repo, prNumber, candidates)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// key: already-posted finding fingerprint hex; value: true
	seen := make(map[string]bool, len(candidates))
	for rows.Next() {
		var fp string
		if err := rows.Scan(&fp); err != nil {
			return nil, err
		}
		seen[fp] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	unseen := make([]string, 0, len(candidates))
	for _, fp := range candidates {
		if fp == "" || seen[fp] {
			continue
		}
		seen[fp] = true
		unseen = append(unseen, fp)
	}
	return unseen, nil
}

func (s *PGStore) SavePublishedFingerprints(ctx context.Context, repo string, prNumber int, runID uuid.UUID, harnessProfile string, items []PublishedItem) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, item := range items {
		var commentID *int64
		if item.CommentID > 0 {
			id := item.CommentID
			commentID = &id
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO finding_fingerprints (repo, pr_number, fingerprint, first_run_id, comment_id)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (repo, pr_number, fingerprint) DO NOTHING
		`, repo, prNumber, item.Fingerprint, runID, commentID); err != nil {
			return err
		}
		if item.CommentID > 0 {
			if _, err := tx.Exec(ctx, `
				INSERT INTO review_feedback (repo, pr_number, comment_id, run_id, harness_profile, fingerprint, category)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				ON CONFLICT (repo, pr_number, comment_id) DO NOTHING
			`, repo, prNumber, item.CommentID, runID, harnessProfile, item.Fingerprint, item.Category); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func (s *PGStore) RecordHarnessExecution(ctx context.Context, exec HarnessExecutionRecord) error {
	var reportJSON []byte
	if exec.Report != nil {
		raw, err := protojson.Marshal(exec.Report)
		if err != nil {
			return err
		}
		reportJSON = raw
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO harness_executions (
			run_id, shard_index, attempt, role, harness_profile, harness_version,
			status, normalizer_path, findings_raw, findings_published,
			requests, prompt_tokens, cached_tokens, completion_tokens, duration_ms, report
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16::jsonb)
		ON CONFLICT (run_id, shard_index, attempt, role, harness_profile) DO UPDATE SET
			harness_version = EXCLUDED.harness_version,
			status = EXCLUDED.status,
			normalizer_path = EXCLUDED.normalizer_path,
			findings_raw = EXCLUDED.findings_raw,
			findings_published = EXCLUDED.findings_published,
			requests = EXCLUDED.requests,
			prompt_tokens = EXCLUDED.prompt_tokens,
			cached_tokens = EXCLUDED.cached_tokens,
			completion_tokens = EXCLUDED.completion_tokens,
			duration_ms = EXCLUDED.duration_ms,
			report = EXCLUDED.report
	`, exec.RunID, exec.ShardIndex, exec.Attempt, exec.Role, exec.HarnessProfile, exec.HarnessVersion,
		exec.Status, exec.NormalizerPath, exec.FindingsRaw, exec.FindingsPublished,
		exec.Requests, exec.PromptTokens, exec.CachedTokens, exec.CompletionTokens, exec.DurationMs, reportJSON,
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PGStore) ListOpenPRCommentsForFeedback(ctx context.Context, since time.Duration) ([]CommentTarget, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT rf.repo, rf.pr_number, rf.comment_id, rf.run_id, rf.harness_profile, rf.fingerprint, rf.category
		FROM review_feedback rf
		JOIN review_runs rr ON rr.id = rf.run_id
		WHERE rf.updated_at > now() - make_interval(secs => $1)
		  AND rr.status = 'done'
		ORDER BY rf.updated_at DESC
	`, since.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CommentTarget
	for rows.Next() {
		var item CommentTarget
		if err := rows.Scan(&item.Repo, &item.PRNumber, &item.CommentID, &item.RunID, &item.HarnessProfile, &item.Fingerprint, &item.Category); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, tx.Commit(ctx)
}

func (s *PGStore) UpsertFeedback(ctx context.Context, fb FeedbackUpdate) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE review_feedback
		SET thumbs_up = $4,
		    thumbs_down = $5,
		    resolved_in_next_commit = resolved_in_next_commit OR $6,
		    updated_at = now()
		WHERE repo = $1 AND pr_number = $2 AND comment_id = $3
	`, fb.Repo, fb.PRNumber, fb.CommentID, fb.ThumbsUp, fb.ThumbsDown, fb.ResolvedInNextCommit); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PGStore) QueryHarnessScorecard(ctx context.Context, window time.Duration) ([]ScorecardRow, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		WITH ex AS (
			SELECT harness_profile,
			       count(DISTINCT run_id)                                      AS runs,
			       coalesce(sum(findings_published), 0)                        AS findings_published,
			       coalesce(sum(prompt_tokens + completion_tokens), 0)         AS total_tokens,
			       sum(cached_tokens)::numeric / nullif(sum(prompt_tokens), 0) AS cache_hit_ratio,
			       percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms)    AS p50_duration_ms
			FROM harness_executions
			WHERE role = 'primary' AND created_at > now() - make_interval(secs => $1)
			GROUP BY harness_profile
		), fb AS (
			SELECT harness_profile,
			       coalesce(sum(thumbs_up), 0)                     AS thumbs_up,
			       coalesce(sum(thumbs_down), 0)                   AS thumbs_down,
			       count(*) FILTER (WHERE resolved_in_next_commit) AS resolved
			FROM review_feedback
			WHERE updated_at > now() - make_interval(secs => $1)
			GROUP BY harness_profile
		)
		SELECT ex.harness_profile,
		       ex.runs::bigint,
		       ex.findings_published::bigint,
		       ex.total_tokens::bigint,
		       coalesce(round(ex.cache_hit_ratio, 3)::double precision, 0),
		       coalesce(ex.p50_duration_ms, 0)::double precision,
		       coalesce(fb.thumbs_up, 0)::bigint,
		       coalesce(fb.thumbs_down, 0)::bigint,
		       coalesce(fb.resolved, 0)::bigint,
		       coalesce(round(1e6 * coalesce(fb.resolved, 0) / nullif(ex.total_tokens, 0), 2)::double precision, 0)
		FROM ex
		LEFT JOIN fb USING (harness_profile)
		ORDER BY 10 DESC, ex.findings_published DESC, ex.harness_profile ASC
	`, window.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ScorecardRow
	for rows.Next() {
		var row ScorecardRow
		if err := rows.Scan(
			&row.HarnessProfile,
			&row.Runs,
			&row.FindingsPublished,
			&row.TotalTokens,
			&row.CacheHitRatio,
			&row.P50DurationMs,
			&row.ThumbsUp,
			&row.ThumbsDown,
			&row.Resolved,
			&row.ResolvedPerMillionTokens,
		); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, tx.Commit(ctx)
}
