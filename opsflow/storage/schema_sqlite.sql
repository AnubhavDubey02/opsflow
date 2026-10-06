-- OpsFlow schema, SQLite dialect (development/test adapter).
-- The PostgreSQL schema mirrors this with native types (uuid, timestamptz, jsonb,
-- bigint). Timestamps here are fixed-width UTC text so they compare correctly.

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS category (
    code       TEXT PRIMARY KEY,
    label      TEXT NOT NULL,
    direction  TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT'))
);

INSERT OR IGNORE INTO category (code, label, direction) VALUES
    ('FUEL',                'Fuel & charging',             'DEBIT'),
    ('TOLLS_PARKING',       'Tolls & parking',             'DEBIT'),
    ('VEHICLE_MAINTENANCE', 'Vehicle maintenance & tyres', 'DEBIT'),
    ('SUBCONTRACT_HAULAGE', 'Subcontracted haulage',       'DEBIT'),
    ('OFFICE_SOFTWARE',     'Office & software',           'DEBIT'),
    ('BANK_FEES',           'Bank & card fees',            'DEBIT'),
    ('FREIGHT_REVENUE',     'Freight revenue',             'CREDIT'),
    ('STORAGE_REVENUE',     'Warehousing revenue',         'CREDIT');

CREATE TABLE IF NOT EXISTS source_batch (
    id                     TEXT PRIMARY KEY,
    received_seq           INTEGER NOT NULL UNIQUE,  -- arrival order (timestamps can tie)
    source_type            TEXT NOT NULL CHECK (source_type IN ('BANK_CSV', 'INVOICE_DOC', 'BOOKING_API')),
    source_name            TEXT NOT NULL,
    business_date          TEXT,
    content_sha256         TEXT NOT NULL CHECK (length(content_sha256) = 64),
    status                 TEXT NOT NULL CHECK (status IN (
                               'RECEIVED', 'VALIDATING', 'VALIDATION_FAILED', 'PROCESSING',
                               'NEEDS_REVIEW', 'RETRY_PENDING', 'FAILED', 'PROCESSED')),
    reconciliation_status  TEXT NOT NULL DEFAULT 'NOT_STARTED' CHECK (reconciliation_status IN (
                               'NOT_STARTED', 'PENDING', 'RECONCILED', 'MISMATCH', 'UNKNOWN')),
    control_record_count   INTEGER CHECK (control_record_count >= 0),
    control_debit_minor    INTEGER CHECK (control_debit_minor >= 0),
    control_credit_minor   INTEGER CHECK (control_credit_minor >= 0),
    retry_budget_used      INTEGER NOT NULL DEFAULT 0 CHECK (retry_budget_used >= 0),
    max_attempts           INTEGER NOT NULL CHECK (max_attempts >= 1),
    next_attempt_at        TEXT,
    lease_run_id           TEXT,
    lease_expires_at       TEXT,
    structure_validated_at TEXT,
    received_at            TEXT NOT NULL,
    updated_at             TEXT NOT NULL,
    version                INTEGER NOT NULL DEFAULT 1,
    UNIQUE (source_type, content_sha256),
    -- A retry time only makes sense while waiting to retry.
    CHECK ((status = 'RETRY_PENDING') = (next_attempt_at IS NOT NULL)),
    -- A batch holds a lease if and only if it is being worked on, so a "working"
    -- batch can never be orphaned without a lease that the worker could reclaim.
    CHECK ((lease_run_id IS NOT NULL) = (status IN ('VALIDATING', 'PROCESSING'))),
    CHECK ((lease_run_id IS NULL) = (lease_expires_at IS NULL))
);

CREATE INDEX IF NOT EXISTS ix_batch_status ON source_batch (status, next_attempt_at);

CREATE TABLE IF NOT EXISTS source_blob (
    batch_id  TEXT PRIMARY KEY REFERENCES source_batch (id),
    content   BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS record (
    id                   TEXT PRIMARY KEY,
    batch_id             TEXT NOT NULL REFERENCES source_batch (id),
    line_no              INTEGER NOT NULL,
    raw_payload          TEXT NOT NULL,
    raw_row_error        TEXT,
    content_fingerprint  TEXT,
    status               TEXT NOT NULL CHECK (status IN (
                             'RECEIVED', 'VALIDATED', 'VALIDATION_FAILED', 'DUPLICATE',
                             'NEEDS_REVIEW', 'PROCESSED', 'REJECTED')),
    source_ref           TEXT,
    business_date        TEXT,
    counterparty         TEXT,
    description          TEXT,
    amount_minor         INTEGER CHECK (amount_minor IS NULL OR amount_minor > 0),
    currency             TEXT,
    direction            TEXT CHECK (direction IS NULL OR direction IN ('DEBIT', 'CREDIT')),
    category_code        TEXT REFERENCES category (code),
    category_source      TEXT CHECK (category_source IS NULL OR category_source IN ('RULE', 'AI', 'HUMAN')),
    rule_id              TEXT,
    rule_version         INTEGER,
    decided_attempt_id   TEXT,
    decided_review_id    TEXT,
    duplicate_of         TEXT REFERENCES record (id),
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL,
    version              INTEGER NOT NULL DEFAULT 1,
    UNIQUE (batch_id, line_no),
    CHECK (status <> 'PROCESSED' OR (category_code IS NOT NULL AND category_source IS NOT NULL
                                     AND amount_minor IS NOT NULL AND direction IS NOT NULL)),
    CHECK ((status = 'DUPLICATE') = (duplicate_of IS NOT NULL)),
    -- A final category can only exist on a PROCESSED record.
    CHECK ((category_code IS NULL) = (status <> 'PROCESSED')),
    -- Exactly the decision column matching category_source is set (plan review R1).
    CHECK (
        (category_source IS NULL AND rule_id IS NULL AND decided_attempt_id IS NULL AND decided_review_id IS NULL)
        OR (category_source = 'RULE'  AND rule_id IS NOT NULL AND rule_version IS NOT NULL
            AND decided_attempt_id IS NULL AND decided_review_id IS NULL)
        OR (category_source = 'AI'    AND decided_attempt_id IS NOT NULL AND rule_id IS NULL
            AND decided_review_id IS NULL)
        OR (category_source = 'HUMAN' AND decided_review_id IS NOT NULL AND rule_id IS NULL
            AND decided_attempt_id IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_record_batch_status ON record (batch_id, status);

CREATE TABLE IF NOT EXISTS record_claim (
    source_type          TEXT NOT NULL,
    source_ref           TEXT NOT NULL,
    record_id            TEXT NOT NULL REFERENCES record (id),
    content_fingerprint  TEXT NOT NULL,
    claimed_at           TEXT NOT NULL,
    PRIMARY KEY (source_type, source_ref)
);

CREATE TABLE IF NOT EXISTS validation_issue (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   TEXT NOT NULL REFERENCES source_batch (id),
    record_id  TEXT REFERENCES record (id),  -- NULL = file-level issue
    code       TEXT NOT NULL,
    field      TEXT,
    message    TEXT NOT NULL,
    severity   TEXT NOT NULL CHECK (severity IN ('ERROR', 'WARNING')),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_issue_batch ON validation_issue (batch_id);

CREATE TABLE IF NOT EXISTS classification_attempt (
    id                 TEXT PRIMARY KEY,
    record_id          TEXT NOT NULL REFERENCES record (id),
    method             TEXT NOT NULL CHECK (method IN ('RULE', 'AI')),
    outcome            TEXT NOT NULL CHECK (outcome IN (
                           'MATCHED', 'NO_MATCH', 'CONFLICT',
                           'OK', 'LOW_CONFIDENCE', 'INVALID_RESPONSE', 'PROVIDER_ERROR')),
    rule_id            TEXT,
    rule_version       INTEGER,
    matched_rule_ids   TEXT,
    ruleset_version    TEXT,
    provider           TEXT,
    model              TEXT,
    prompt_version     TEXT,
    input_fingerprint  TEXT,
    category_code      TEXT REFERENCES category (code),
    confidence         REAL CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    threshold          REAL,
    rationale          TEXT,
    raw_response       TEXT,
    latency_ms         INTEGER,
    created_at         TEXT NOT NULL,
    -- Rules never fake a confidence score.
    CHECK (method <> 'RULE' OR confidence IS NULL)
);

CREATE TABLE IF NOT EXISTS review_task (
    id                     TEXT PRIMARY KEY,
    record_id              TEXT NOT NULL REFERENCES record (id),
    batch_id               TEXT NOT NULL REFERENCES source_batch (id),
    reason                 TEXT NOT NULL CHECK (reason IN (
                               'LOW_CONFIDENCE', 'AI_INVALID_RESPONSE', 'NO_RULE_AI_DISABLED',
                               'RULE_CONFLICT', 'DUPLICATE_CONFLICT')),
    suggested_category     TEXT REFERENCES category (code),
    conflicting_record_id  TEXT REFERENCES record (id),
    status                 TEXT NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
    decision               TEXT CHECK (decision IS NULL OR decision IN ('APPROVE', 'CORRECT', 'REJECT')),
    decided_category       TEXT REFERENCES category (code),
    decided_by             TEXT,
    decided_at             TEXT,
    note                   TEXT,
    created_at             TEXT NOT NULL,
    version                INTEGER NOT NULL DEFAULT 1,
    CHECK ((status = 'OPEN') = (decision IS NULL)),
    CHECK (decision IS NULL OR (decided_by IS NOT NULL AND decided_at IS NOT NULL)),
    CHECK (decision NOT IN ('CORRECT', 'REJECT') OR length(trim(coalesce(note, ''))) > 0),
    CHECK (decision <> 'REJECT' OR decided_category IS NULL),
    CHECK (decision NOT IN ('APPROVE', 'CORRECT') OR decided_category IS NOT NULL)
);

-- At most one open review per record.
CREATE UNIQUE INDEX IF NOT EXISTS ux_review_open ON review_task (record_id) WHERE status = 'OPEN';

CREATE TABLE IF NOT EXISTS processing_run (
    id            TEXT PRIMARY KEY,
    batch_id      TEXT NOT NULL REFERENCES source_batch (id),
    attempt_no    INTEGER NOT NULL,
    worker_id     TEXT NOT NULL,
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    outcome       TEXT NOT NULL CHECK (outcome IN (
                      'RUNNING', 'COMPLETED', 'RETRY_SCHEDULED', 'FAILED', 'ABANDONED')),
    error_code    TEXT,
    error_detail  TEXT,
    UNIQUE (batch_id, attempt_no),
    CHECK ((outcome = 'RUNNING') = (finished_at IS NULL))
);

CREATE TABLE IF NOT EXISTS reconciliation_result (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id                TEXT NOT NULL REFERENCES source_batch (id),
    computed_at             TEXT NOT NULL,
    status                  TEXT NOT NULL,
    received                INTEGER NOT NULL,
    processed               INTEGER NOT NULL,
    rejected                INTEGER NOT NULL,
    validation_failed       INTEGER NOT NULL,
    duplicate               INTEGER NOT NULL,
    pending                 INTEGER NOT NULL,
    source_debit_minor      INTEGER NOT NULL,
    source_credit_minor     INTEGER NOT NULL,
    processed_debit_minor   INTEGER NOT NULL,
    processed_credit_minor  INTEGER NOT NULL,
    unknown_amount_count    INTEGER NOT NULL,
    checks                  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_event (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at  TEXT NOT NULL,
    actor        TEXT NOT NULL,
    entity_type  TEXT NOT NULL,
    entity_id    TEXT NOT NULL,
    batch_id     TEXT,
    action       TEXT NOT NULL,
    from_state   TEXT,
    to_state     TEXT,
    run_id       TEXT,
    details      TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS ix_audit_batch ON audit_event (batch_id, id);

-- The audit trail is append-only: history cannot be rewritten.
CREATE TRIGGER IF NOT EXISTS audit_event_no_update BEFORE UPDATE ON audit_event
BEGIN
    SELECT RAISE(ABORT, 'audit_event is append-only');
END;

CREATE TRIGGER IF NOT EXISTS audit_event_no_delete BEFORE DELETE ON audit_event
BEGIN
    SELECT RAISE(ABORT, 'audit_event is append-only');
END;

-- Raw input is immutable once stored.
CREATE TRIGGER IF NOT EXISTS record_raw_immutable BEFORE UPDATE OF raw_payload, line_no, batch_id ON record
BEGIN
    SELECT RAISE(ABORT, 'raw record fields are immutable');
END;

CREATE TRIGGER IF NOT EXISTS blob_immutable BEFORE UPDATE ON source_blob
BEGIN
    SELECT RAISE(ABORT, 'source_blob is immutable');
END;
