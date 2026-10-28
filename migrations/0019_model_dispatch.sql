-- Durable pre-outcome linkage. ModelInvocation remains append-only outcome history.
CREATE TABLE model_dispatches (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    step_id TEXT,
    provider_connection_id TEXT NOT NULL,
    credential_ref_id TEXT NOT NULL,
    provider_epoch INTEGER NOT NULL,
    credential_epoch INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('PREPARED','DISPATCHING','RESOLVED','ABORTED','OUTCOME_UNKNOWN')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(company_id,workspace_id) REFERENCES companies(id,workspace_id),
    FOREIGN KEY(project_id,company_id) REFERENCES projects(id,company_id),
    FOREIGN KEY(run_id,company_id) REFERENCES runs(id,company_id),
    FOREIGN KEY(step_id,company_id) REFERENCES execution_steps(id,company_id),
    FOREIGN KEY(provider_connection_id,workspace_id) REFERENCES provider_connections(id,workspace_id),
    FOREIGN KEY(credential_ref_id,workspace_id) REFERENCES credential_refs(id,workspace_id)
);
CREATE INDEX model_dispatches_run ON model_dispatches(run_id,status);
CREATE INDEX model_dispatches_project ON model_dispatches(company_id,project_id,status);
ALTER TABLE resource_reservations ADD COLUMN model_dispatch_id TEXT REFERENCES model_dispatches(id);
CREATE INDEX reservations_model_dispatch ON resource_reservations(model_dispatch_id,status);
-- Legacy snapshots already identify some pre-outcome reservations. Preserve ambiguity,
-- terminal Project/Run history and accounting; never infer a release or provider result.
INSERT INTO model_dispatches(id,workspace_id,company_id,project_id,run_id,step_id,provider_connection_id,credential_ref_id,provider_epoch,credential_epoch,status,created_at,updated_at)
SELECT s.invocation_id,c.workspace_id,r.company_id,r.project_id,r.id,
 (SELECT i.step_id FROM model_invocations i WHERE i.id=s.invocation_id AND i.run_id=r.id AND i.company_id=r.company_id),
 p.id,p.credential_ref_id,p.row_version,cr.row_version,'OUTCOME_UNKNOWN',s.created_at,s.created_at
FROM context_snapshots s JOIN runs r ON r.id=s.run_id AND r.company_id=s.company_id
JOIN companies c ON c.id=r.company_id JOIN provider_connections p ON p.id=json_extract(s.model_binding,'$.provider_id') AND p.workspace_id=c.workspace_id
JOIN credential_refs cr ON cr.id=p.credential_ref_id AND cr.workspace_id=c.workspace_id
WHERE EXISTS(SELECT 1 FROM resource_reservations h WHERE h.id='model:'||s.invocation_id||':'||h.budget_id AND h.company_id=r.company_id AND h.status='HELD')
;
UPDATE resource_reservations SET model_dispatch_id=(SELECT d.id FROM model_dispatches d WHERE resource_reservations.id='model:'||d.id||':'||resource_reservations.budget_id AND d.company_id=resource_reservations.company_id)
WHERE EXISTS(SELECT 1 FROM model_dispatches d WHERE resource_reservations.id='model:'||d.id||':'||resource_reservations.budget_id AND d.company_id=resource_reservations.company_id);
CREATE TRIGGER model_dispatch_scope BEFORE INSERT ON model_dispatches
WHEN NOT EXISTS(SELECT 1 FROM runs r JOIN projects p ON p.id=r.project_id AND p.company_id=r.company_id
 JOIN execution_steps s ON s.run_id=r.id AND s.company_id=r.company_id
 WHERE r.id=NEW.run_id AND r.company_id=NEW.company_id AND r.project_id=NEW.project_id
 AND p.status='ACTIVE' AND r.lifecycle_state='RUNNING' AND s.id=NEW.step_id)
BEGIN SELECT RAISE(ABORT,'invalid model dispatch scope or lifecycle'); END;
CREATE TRIGGER model_dispatch_unresolved BEFORE INSERT ON model_dispatches
WHEN EXISTS(SELECT 1 FROM model_dispatches d JOIN runs old ON old.id=d.run_id JOIN runs incoming ON incoming.id=NEW.run_id
 WHERE old.company_id=incoming.company_id AND old.work_item_id=incoming.work_item_id AND old.id<>incoming.id
 AND d.status IN ('PREPARED','DISPATCHING','OUTCOME_UNKNOWN'))
BEGIN SELECT RAISE(ABORT,'unresolved model dispatch requires reconciliation'); END;
CREATE TRIGGER model_dispatch_identity BEFORE UPDATE ON model_dispatches
WHEN NEW.id IS NOT OLD.id OR NEW.workspace_id IS NOT OLD.workspace_id OR NEW.company_id IS NOT OLD.company_id
 OR NEW.project_id IS NOT OLD.project_id OR NEW.run_id IS NOT OLD.run_id OR NEW.step_id IS NOT OLD.step_id
 OR NEW.provider_connection_id IS NOT OLD.provider_connection_id OR NEW.credential_ref_id IS NOT OLD.credential_ref_id
 OR NEW.provider_epoch IS NOT OLD.provider_epoch OR NEW.credential_epoch IS NOT OLD.credential_epoch
 OR NEW.created_at IS NOT OLD.created_at
 OR NOT ((OLD.status='PREPARED' AND NEW.status IN ('DISPATCHING','ABORTED'))
 OR (OLD.status='DISPATCHING' AND NEW.status IN ('RESOLVED','ABORTED','OUTCOME_UNKNOWN')))
BEGIN SELECT RAISE(ABORT,'immutable dispatch identity or terminal state'); END;
CREATE TRIGGER model_dispatch_retained BEFORE DELETE ON model_dispatches
BEGIN SELECT RAISE(ABORT,'retained model dispatch'); END;
CREATE TRIGGER model_reservation_scope BEFORE UPDATE OF model_dispatch_id ON resource_reservations
WHEN OLD.model_dispatch_id IS NOT NULL OR NOT EXISTS(
 SELECT 1 FROM model_dispatches d WHERE d.id=NEW.model_dispatch_id AND d.workspace_id=NEW.workspace_id
 AND d.company_id=NEW.company_id AND NEW.resource_type='MODEL_CALL' AND NEW.id='model:'||d.id||':'||NEW.budget_id)
BEGIN SELECT RAISE(ABORT,'invalid model reservation linkage'); END;
CREATE TRIGGER model_unknown_retry BEFORE INSERT ON runs
WHEN NEW.retry_of_run_id IS NOT NULL AND EXISTS(SELECT 1 FROM model_dispatches d JOIN runs r ON r.id=d.run_id
 WHERE r.work_item_id=NEW.work_item_id AND d.status IN ('PREPARED','DISPATCHING','OUTCOME_UNKNOWN'))
BEGIN SELECT RAISE(ABORT,'unknown model outcome requires reconciliation'); END;
CREATE TRIGGER model_invocations_immutable_update BEFORE UPDATE ON model_invocations
BEGIN SELECT RAISE(ABORT,'immutable model invocation'); END;
CREATE TRIGGER model_invocations_immutable_delete BEFORE DELETE ON model_invocations
BEGIN SELECT RAISE(ABORT,'immutable model invocation'); END;
CREATE TRIGGER runtime_results_immutable_update BEFORE UPDATE ON runtime_results
BEGIN SELECT RAISE(ABORT,'immutable runtime result'); END;
CREATE TRIGGER runtime_results_immutable_delete BEFORE DELETE ON runtime_results
BEGIN SELECT RAISE(ABORT,'immutable runtime result'); END;
