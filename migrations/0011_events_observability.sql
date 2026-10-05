-- Migration: 0011_events_observability.sql
-- MVP Reconciled Physical Migration: M4 Usage Accounting only. Audit/policy/trace expansion remains deferred.

CREATE TABLE IF NOT EXISTS usage_records (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    step_id TEXT,
    provider_connection_id TEXT NOT NULL,
    model_id TEXT NOT NULL,
    usage_type TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit TEXT NOT NULL,
    estimated_cost REAL,
    occurred_at TEXT NOT NULL,
    metadata TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(id) ON DELETE RESTRICT,
    FOREIGN KEY (company_id, workspace_id) REFERENCES companies(id, workspace_id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id, company_id) REFERENCES projects(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (work_item_id, company_id, project_id) REFERENCES work_items(id, company_id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (agent_id, company_id) REFERENCES agents(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT,
    FOREIGN KEY (step_id) REFERENCES execution_steps(id) ON DELETE RESTRICT,
    FOREIGN KEY (provider_connection_id) REFERENCES provider_connections(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_usage_records_company_project ON usage_records(company_id, project_id);
CREATE INDEX IF NOT EXISTS idx_usage_records_run ON usage_records(run_id);
