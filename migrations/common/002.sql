CREATE TABLE run_control_requests (
    generation INTEGER PRIMARY KEY,
    operation_id TEXT NOT NULL,
    request_hash TEXT NOT NULL
);
