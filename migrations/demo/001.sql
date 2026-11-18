CREATE TABLE demo_runs (
    run_id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL UNIQUE,
    scenario TEXT NOT NULL,
    initial_seats INTEGER NOT NULL CHECK (initial_seats IN (0, 1)),
    race_claimed INTEGER NOT NULL DEFAULT 0 CHECK (race_claimed IN (0, 1)),
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE demo_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    active_run_id TEXT REFERENCES demo_runs(run_id),
    state TEXT NOT NULL CHECK (state IN ('PREPARING', 'READY', 'ERROR')),
    active_operation_id TEXT
);

CREATE TABLE control_operations (
    operation_id TEXT PRIMARY KEY,
    request_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    kind TEXT NOT NULL,
    run_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    stage TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('RUNNING', 'SUCCEEDED', 'FAILED')),
    progress_json TEXT NOT NULL,
    error_text TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE purchase_requests (
    run_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    primary_order_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (run_id, request_key)
);

CREATE TABLE purchase_jobs (
    job_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    request_id TEXT NOT NULL UNIQUE,
    order_id TEXT NOT NULL UNIQUE,
    buyer_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'ACCEPTED', 'FAILED', 'CANCELED')),
    body_json TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at_ms INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at_ms INTEGER NOT NULL,
    FOREIGN KEY (run_id, request_key) REFERENCES purchase_requests(run_id, request_key)
);

CREATE TABLE kafka_records (
    journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_value INTEGER NOT NULL,
    record_key TEXT,
    event_id TEXT,
    run_id TEXT,
    order_id TEXT,
    event_type TEXT,
    causation_id TEXT,
    occurred_at TEXT,
    observed_at_ms INTEGER NOT NULL,
    raw_json TEXT NOT NULL,
    parse_error TEXT,
    UNIQUE (topic, partition_id, offset_value)
);
CREATE INDEX journal_order_idx ON kafka_records(run_id, order_id, journal_id);
