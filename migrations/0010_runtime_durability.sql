-- Migration: 0010_runtime_durability.sql
-- MVP Reconciled Physical Migration: M4 Runtime Durability (runtime_checkpoints, run_execution_leases, durable_jobs)

CREATE TABLE IF NOT EXISTS runtime_checkpoints (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    checkpoint_version INTEGER NOT NULL,
    run_state TEXT NOT NULL,
    last_completed_step INTEGER,
    active_step INTEGER,
    execution_phase TEXT NOT NULL,
    context_refs TEXT,
    continuation_metadata TEXT,
    usage_snapshot TEXT,
    safe_to_resume INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    UNIQUE(run_id, checkpoint_version)
);

CREATE INDEX IF NOT EXISTS idx_runtime_checkpoints_run ON runtime_checkpoints(run_id);

CREATE TABLE IF NOT EXISTS run_execution_leases (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    worker_principal_id TEXT NOT NULL,
    lease_version INTEGER NOT NULL DEFAULT 1,
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    released_at TEXT,
    release_reason TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_run_execution_leases_run ON run_execution_leases(run_id);

CREATE TABLE IF NOT EXISTS durable_jobs (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    company_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    available_at TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    lease_owner TEXT,
    lease_until TEXT,
    status TEXT NOT NULL,
    correlation_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    UNIQUE(job_type, run_id)
);

CREATE INDEX IF NOT EXISTS idx_durable_jobs_status ON durable_jobs(status, available_at);
