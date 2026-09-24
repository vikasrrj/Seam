CREATE TABLE IF NOT EXISTS accounts (
    id BIGINT PRIMARY KEY,
    owner TEXT NOT NULL,
    balance_cents BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS widgets (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    qty INTEGER,
    price NUMERIC(12,2),
    created TIMESTAMPTZ,
    tags JSONB,
    data BYTEA,
    active BOOLEAN,
    note VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS seam_jobs (
    job_id TEXT PRIMARY KEY,
    source_dsn TEXT NOT NULL,
    source_dsn_sha256 TEXT NOT NULL DEFAULT '',
    source_slot TEXT NOT NULL,
    source_publication TEXT NOT NULL,
    source_system_id TEXT NOT NULL DEFAULT '',
    table_schema TEXT NOT NULL,
    table_name TEXT NOT NULL,
    destination_table TEXT NOT NULL DEFAULT 'accounts',
    kafka_topic TEXT NOT NULL,
    kafka_topic_id TEXT NOT NULL DEFAULT '',
    generation TEXT NOT NULL,
    scan_upper_bound BIGINT NOT NULL,
    discovery_cursor BIGINT,
    discovery_complete BOOLEAN NOT NULL DEFAULT FALSE,
    schema_fingerprint TEXT NOT NULL,
    source_schema JSONB NOT NULL DEFAULT '{}',
    source_schema_fingerprint TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS seam_checkpoints (
    job_id TEXT PRIMARY KEY REFERENCES seam_jobs(job_id),
    generation TEXT NOT NULL,
    attempt TEXT NOT NULL,
    owner_id TEXT,
    owner_epoch BIGINT NOT NULL DEFAULT 0,
    owner_lease_expiry TIMESTAMPTZ,
    scan_upper_bound BIGINT NOT NULL,
    completed_through_id BIGINT NOT NULL DEFAULT -1,
    next_kafka_offset BIGINT NOT NULL DEFAULT 0,
    last_applied_lsn TEXT,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS seam_applied_txs (
    job_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    source_lsn TEXT NOT NULL,
    source_xid TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, generation, source_lsn)
);

CREATE TABLE IF NOT EXISTS seam_candidates (
    job_id TEXT NOT NULL REFERENCES seam_jobs(job_id) ON DELETE CASCADE,
    attempt TEXT NOT NULL,
    chunk_min_id BIGINT NOT NULL,
    chunk_max_id BIGINT NOT NULL,
    id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    evicted BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (job_id, attempt, chunk_min_id, id)
);
CREATE INDEX IF NOT EXISTS seam_candidates_survivors_idx
    ON seam_candidates(job_id, attempt, chunk_min_id) WHERE evicted = FALSE;

CREATE TABLE IF NOT EXISTS seam_route_fence (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    epoch BIGINT NOT NULL DEFAULT 0
);
INSERT INTO seam_route_fence (id, epoch) VALUES (TRUE, 0) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS seam_promotions (
    shadow_job_id TEXT PRIMARY KEY REFERENCES seam_jobs(job_id),
    live_job_id TEXT NOT NULL REFERENCES seam_jobs(job_id),
    barrier_offset BIGINT NOT NULL,
    retired_table TEXT NOT NULL,
    active_table_oid OID NOT NULL,
    promoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS seam_cutover_gates (
    job_id TEXT PRIMARY KEY REFERENCES seam_jobs(job_id) ON DELETE CASCADE,
    target_offset BIGINT NOT NULL CHECK (target_offset >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
