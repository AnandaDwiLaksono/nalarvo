-- Forward-only extension; published migrations remain immutable.
ALTER TABLE budgets ADD COLUMN project_id TEXT REFERENCES projects(id) ON DELETE RESTRICT;
CREATE INDEX idx_budgets_project ON budgets(company_id, project_id);
