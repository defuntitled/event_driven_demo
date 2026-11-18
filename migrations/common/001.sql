CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL UNIQUE,
    scenario TEXT NOT NULL,
    initial_seats INTEGER NOT NULL CHECK (initial_seats IN (0, 1)),
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE service_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    active_run_id TEXT REFERENCES runs(run_id),
    generation INTEGER NOT NULL DEFAULT 0,
    control_operation_id TEXT,
    control_phase INTEGER NOT NULL DEFAULT 0,
    admission TEXT NOT NULL CHECK (admission IN ('CLOSED', 'OPEN'))
);

CREATE TABLE worker_control (
    worker TEXT PRIMARY KEY CHECK (worker IN ('consumer', 'publisher')),
    desired_state TEXT NOT NULL
        CHECK (desired_state IN ('RUNNING', 'PAUSED', 'STOPPED', 'FAULTED', 'ERROR')),
    run_id TEXT,
    generation INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE outbox (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    order_id TEXT NOT NULL,
    logical_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    topic TEXT NOT NULL,
    record_key TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'SENT', 'ABANDONED')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at_ms INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    published_at_ms INTEGER,
    kafka_partition INTEGER,
    kafka_offset INTEGER,
    UNIQUE (run_id, logical_key)
);
CREATE INDEX outbox_pending_idx ON outbox(run_id, status, row_id);

CREATE TABLE processed_events (
    handler TEXT NOT NULL,
    event_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    first_topic TEXT NOT NULL,
    first_partition INTEGER NOT NULL,
    first_offset INTEGER NOT NULL,
    processed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (handler, event_id)
);

CREATE TABLE processing_attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_instance_id TEXT NOT NULL,
    handler TEXT NOT NULL,
    run_id TEXT,
    order_id TEXT,
    event_id TEXT,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_value INTEGER NOT NULL,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    outcome TEXT NOT NULL,
    commit_status TEXT NOT NULL DEFAULT 'NOT_ATTEMPTED',
    database_retries INTEGER NOT NULL DEFAULT 0,
    fault_name TEXT,
    error_text TEXT,
    raw_json TEXT
);
CREATE INDEX attempts_order_idx ON processing_attempts(run_id, order_id, attempt_id);

CREATE TABLE fault_injections (
    run_id TEXT NOT NULL,
    name TEXT NOT NULL,
    remaining INTEGER NOT NULL CHECK (remaining IN (0, 1)),
    triggered_event_id TEXT,
    triggered_at_ms INTEGER,
    PRIMARY KEY (run_id, name)
);
