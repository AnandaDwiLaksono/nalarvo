-- Migration: 0009_execution.sql
-- MVP Reconciled Physical Migration: M4 Execution (runs, execution_steps, model_invocations, runtime_results)

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    assignment_id TEXT,
    executing_agent_id TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    retry_of_run_id TEXT,
    model_profile_version_id TEXT,
    requested_by_type TEXT NOT NULL,
    requested_by_id TEXT NOT NULL,
    queued_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    failure_class TEXT,
    failure_detail TEXT,
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    row_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id, company_id) REFERENCES projects(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (work_item_id, company_id, project_id) REFERENCES work_items(id, company_id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (assignment_id, company_id, project_id) REFERENCES assignments(id, company_id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (executing_agent_id, company_id) REFERENCES agents(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (retry_of_run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    UNIQUE(id, company_id),
    UNIQUE(work_item_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS idx_runs_company_project ON runs(company_id, project_id);
CREATE INDEX IF NOT EXISTS idx_runs_work_item ON runs(work_item_id);
CREATE INDEX IF NOT EXISTS idx_runs_state ON runs(lifecycle_state);

CREATE TABLE IF NOT EXISTS execution_steps (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    sequence_no INTEGER NOT NULL,
    step_type TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL,
    parent_step_id TEXT,
    input_metadata TEXT,
    output_metadata TEXT,
    failure_class TEXT,
    failure_detail TEXT,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, company_id) REFERENCES runs(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (parent_step_id) REFERENCES execution_steps(id) ON DELETE RESTRICT,
    UNIQUE(id, company_id),
    UNIQUE(run_id, sequence_no),
    CHECK (lifecycle_state IN ('PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED','SKIPPED'))
);

CREATE INDEX IF NOT EXISTS idx_execution_steps_run ON execution_steps(run_id);

CREATE TABLE IF NOT EXISTS model_invocations (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    provider_connection_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    model_profile_version_id TEXT,
    invocation_index INTEGER NOT NULL,
    status TEXT NOT NULL,
    request_metadata TEXT,
    response_metadata TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    estimated_cost REAL,
    latency_ms INTEGER,
    provider_request_id TEXT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    failure_class TEXT,
    failure_detail TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, company_id) REFERENCES runs(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (step_id, company_id) REFERENCES execution_steps(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (agent_id, company_id) REFERENCES agents(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_connection_id) REFERENCES provider_connections(id) ON DELETE RESTRICT,
    UNIQUE(id, company_id),
    UNIQUE(run_id, invocation_index)
);

CREATE INDEX IF NOT EXISTS idx_model_invocations_run ON model_invocations(run_id);

CREATE TABLE IF NOT EXISTS runtime_results (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    run_id TEXT NOT NULL UNIQUE,
    run_status TEXT NOT NULL,
    result_summary TEXT NOT NULL,
    output_payload TEXT,
    output_metadata TEXT,
    resource_usage_summary TEXT,
    failure_class TEXT,
    failure_detail TEXT,
    warnings TEXT,
    correlation_id TEXT NOT NULL,
    causation_id TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, company_id) REFERENCES runs(id, company_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_runtime_results_run ON runtime_results(run_id);
