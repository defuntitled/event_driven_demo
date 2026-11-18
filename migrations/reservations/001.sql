CREATE TABLE inventory (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    concert_id TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity IN (0, 1)),
    available INTEGER NOT NULL,
    PRIMARY KEY (run_id, concert_id),
    CHECK (available >= 0 AND available <= capacity)
);

CREATE TABLE reservation_decisions (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    order_id TEXT NOT NULL,
    concert_id TEXT NOT NULL,
    payment_id TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('RESERVED', 'REJECTED')),
    reason TEXT,
    ticket_id TEXT UNIQUE,
    source_event_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (run_id, order_id),
    FOREIGN KEY (run_id, concert_id) REFERENCES inventory(run_id, concert_id),
    CHECK (
        (outcome = 'RESERVED' AND ticket_id IS NOT NULL AND reason IS NULL)
        OR (outcome = 'REJECTED' AND ticket_id IS NULL
            AND reason IS NOT NULL AND reason = 'NO_SEATS')
    )
);
