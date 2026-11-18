CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    request_id TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    buyer_id TEXT NOT NULL,
    concert_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity = 1),
    amount_cents INTEGER NOT NULL CHECK (amount_cents = 50000),
    currency TEXT NOT NULL CHECK (currency = 'RUB'),
    payment_seen INTEGER NOT NULL DEFAULT 0 CHECK (payment_seen IN (0, 1)),
    reservation_outcome TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (reservation_outcome IN ('PENDING', 'RESERVED', 'REJECTED')),
    refund_seen INTEGER NOT NULL DEFAULT 0 CHECK (refund_seen IN (0, 1)),
    charge_id TEXT,
    refund_id TEXT,
    ticket_id TEXT,
    status TEXT NOT NULL DEFAULT 'PROCESSING'
        CHECK (status IN ('PROCESSING', 'PAID_PENDING_RESERVATION',
                          'CONFIRMED', 'REFUND_PENDING', 'REFUNDED')),
    version INTEGER NOT NULL DEFAULT 1,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (run_id, request_id)
);
CREATE INDEX orders_run_idx ON orders(run_id, created_at_ms, order_id);
