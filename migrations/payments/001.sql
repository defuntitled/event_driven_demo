CREATE TABLE payment_operations (
    payment_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    order_id TEXT NOT NULL,
    concert_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('CHARGE', 'REFUND')),
    amount_cents INTEGER NOT NULL CHECK (amount_cents = 50000),
    currency TEXT NOT NULL CHECK (currency = 'RUB'),
    original_payment_id TEXT REFERENCES payment_operations(payment_id),
    source_event_id TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    UNIQUE (run_id, order_id, kind),
    CHECK (
        (kind = 'CHARGE' AND original_payment_id IS NULL)
        OR (kind = 'REFUND' AND original_payment_id IS NOT NULL)
    )
);
