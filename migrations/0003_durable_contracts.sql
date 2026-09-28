CREATE TABLE IF NOT EXISTS idempotency_records (
    id TEXT PRIMARY KEY,
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'COMPLETED')),
    response_body TEXT,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    UNIQUE(scope, idempotency_key)
);

CREATE TABLE IF NOT EXISTS domain_events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    company_id TEXT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    causation_id TEXT NOT NULL,
    principal_type TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    scope_type TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_domain_events_company ON domain_events(company_id);

CREATE TABLE IF NOT EXISTS outbox_messages (
    id TEXT PRIMARY KEY,
    domain_event_id TEXT NOT NULL REFERENCES domain_events(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_until TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    published_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_messages(status, available_at);

CREATE TABLE IF NOT EXISTS consumer_inbox (
    consumer_name TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    PRIMARY KEY (consumer_name, event_id)
);
