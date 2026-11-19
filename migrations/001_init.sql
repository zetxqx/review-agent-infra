-- Webhook delivery deduplication table preventing duplicate network retries from re-triggering runs.
CREATE TABLE IF NOT EXISTS webhook_deliveries (
    delivery_id TEXT PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Review run queue and lease table; at most one running or publishing job per PR is enforced below.
CREATE TABLE IF NOT EXISTS review_runs (
    id                UUID PRIMARY KEY,
    repo              TEXT   NOT NULL,
    pr_number         INT    NOT NULL,
    base_sha          TEXT   NOT NULL,
    head_sha          TEXT   NOT NULL,
    trigger_type      TEXT    NOT NULL, -- 'auto' | 'manual_command'
    force_rerun_seq   BIGINT  NOT NULL DEFAULT 0, -- 0 for auto; GitHub comment ID for manual /review
    is_fork           BOOLEAN NOT NULL DEFAULT FALSE,
    is_maintainer     BOOLEAN NOT NULL DEFAULT FALSE,
    requested_harness TEXT,            -- Explicit profile from /review <profile>; NULL selects routing policy
    harness_profile   TEXT,            -- Chosen once at claim time and kept across retries
    status            TEXT    NOT NULL, -- 'queued' | 'running' | 'publishing' | 'done' | 'superseded' | 'failed'
    debounce_until    TIMESTAMPTZ NOT NULL,
    attempt           INT     NOT NULL DEFAULT 0,
    max_retries       INT     NOT NULL DEFAULT 2,
    worker_id         TEXT,
    lease_expires_at  TIMESTAMPTZ,
    error_reason      TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Backfill trust-classification columns on existing databases running idempotent Migrate.
ALTER TABLE review_runs ADD COLUMN IF NOT EXISTS is_fork BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE review_runs ADD COLUMN IF NOT EXISTS is_maintainer BOOLEAN NOT NULL DEFAULT FALSE;

-- Partial unique idempotency key: permits re-review on force-push back to prior SHA or reopen without duplicating active/done runs.
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_or_done_run_idempotency
    ON review_runs (repo, pr_number, base_sha, head_sha, trigger_type, force_rerun_seq)
    WHERE status NOT IN ('superseded', 'failed');

-- Hard database invariant guaranteeing at most one active running or publishing run per repository PR.
CREATE UNIQUE INDEX IF NOT EXISTS uq_active_pr_run
    ON review_runs (repo, pr_number)
    WHERE status IN ('running', 'publishing');

-- Claim lookup index covering ready debounced jobs and expired worker leases.
CREATE INDEX IF NOT EXISTS idx_runs_claimable
    ON review_runs (status, debounce_until, lease_expires_at);

-- Cross-round deduplication fingerprints keyed by file + prev_line + target_line (harness- and category-agnostic).
CREATE TABLE IF NOT EXISTS finding_fingerprints (
    repo         TEXT NOT NULL,
    pr_number    INT  NOT NULL,
    fingerprint  TEXT NOT NULL,
    first_run_id UUID NOT NULL REFERENCES review_runs(id),
    comment_id   BIGINT, -- NULL when finding is folded into the PR summary body
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo, pr_number, fingerprint)
);

-- Quality feedback aggregates updated by webhooks and periodic reaction polling for inline/file comments (comment_id > 0).
CREATE TABLE IF NOT EXISTS review_feedback (
    repo                    TEXT NOT NULL,
    pr_number               INT  NOT NULL,
    comment_id              BIGINT NOT NULL,
    run_id                  UUID NOT NULL REFERENCES review_runs(id),
    harness_profile         TEXT NOT NULL,
    fingerprint             TEXT NOT NULL,
    category                TEXT NOT NULL,
    thumbs_up               INT  NOT NULL DEFAULT 0,
    thumbs_down             INT  NOT NULL DEFAULT 0,
    resolved_in_next_commit BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (repo, pr_number, comment_id)
);

-- Per-shard, per-attempt, per-role execution telemetry for A/B comparison across harnesses; token counts come from cmd/adapter.
CREATE TABLE IF NOT EXISTS harness_executions (
    run_id             UUID   NOT NULL REFERENCES review_runs(id),
    shard_index        INT    NOT NULL,
    attempt            INT    NOT NULL,
    role               TEXT   NOT NULL, -- 'primary' | 'shadow'
    harness_profile    TEXT   NOT NULL,
    harness_version    TEXT,
    status             TEXT   NOT NULL, -- 'ok' | 'harness_failed' | 'sandbox_error'
    normalizer_path    TEXT,            -- 'deterministic' | 'llm' | 'none'
    findings_raw       INT    NOT NULL DEFAULT 0,
    findings_published INT    NOT NULL DEFAULT 0,
    requests           INT    NOT NULL DEFAULT 0,
    prompt_tokens      BIGINT NOT NULL DEFAULT 0,
    cached_tokens      BIGINT NOT NULL DEFAULT 0,
    completion_tokens  BIGINT NOT NULL DEFAULT 0,
    duration_ms        BIGINT NOT NULL DEFAULT 0,
    report             JSONB,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, shard_index, attempt, role, harness_profile)
);
