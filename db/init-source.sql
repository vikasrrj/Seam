CREATE TABLE IF NOT EXISTS accounts (
    id BIGINT PRIMARY KEY,
    owner TEXT NOT NULL,
    balance_cents BIGINT NOT NULL
);

ALTER TABLE accounts REPLICA IDENTITY FULL;

-- Second fixture driving the generic row/schema-epoch path: a wide table with
-- nullable columns across the supported type set.
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

ALTER TABLE widgets REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS seam_marker (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    attempt TEXT NOT NULL,
    kind TEXT NOT NULL,
    chunk_min_id BIGINT NOT NULL,
    chunk_max_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS seam_capture_owners (
    slot_name TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    owner_epoch BIGINT NOT NULL,
    backend_pid INTEGER NOT NULL,
    source_system_id TEXT NOT NULL,
    generation TEXT NOT NULL,
    publication TEXT NOT NULL,
    kafka_topic_id TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Publication must be created after tables exist. Replication slot is created
-- by the capture process.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'seam_pub'
    ) THEN
        CREATE PUBLICATION seam_pub FOR TABLE accounts, seam_marker;
    END IF;
END
$$;

-- Widgets replication runs under its own publication so the accounts stream
-- never observes an unsupported relation (capture fails closed on it).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'seam_pub_widgets'
    ) THEN
        CREATE PUBLICATION seam_pub_widgets FOR TABLE widgets, seam_marker;
    END IF;
END
$$;
