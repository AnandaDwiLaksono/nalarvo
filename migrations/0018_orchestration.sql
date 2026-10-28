-- M7: capability assertions describe matching, never execution authority.
CREATE TABLE agent_capabilities (
    company_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    capability_id TEXT NOT NULL CHECK(length(trim(capability_id)) BETWEEN 1 AND 128),
    PRIMARY KEY(company_id, agent_id, capability_id),
    FOREIGN KEY(agent_id, company_id) REFERENCES agents(id, company_id) ON DELETE RESTRICT
);

CREATE TABLE work_requirements (
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    role_id TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0 CHECK(priority BETWEEN -1000 AND 1000),
    required INTEGER NOT NULL DEFAULT 1 CHECK(required IN (0,1)),
    acceptance_contains TEXT NOT NULL CHECK(length(acceptance_contains) BETWEEN 1 AND 4096),
    PRIMARY KEY(company_id, project_id, work_item_id),
    FOREIGN KEY(work_item_id,company_id,project_id) REFERENCES work_items(id,company_id,project_id) ON DELETE RESTRICT,
    FOREIGN KEY(role_id,company_id) REFERENCES roles(id,company_id) ON DELETE RESTRICT
);
CREATE TABLE work_required_inputs (
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    artifact_version_id TEXT NOT NULL,
    PRIMARY KEY(company_id,project_id,work_item_id,artifact_version_id),
    FOREIGN KEY(work_item_id,company_id,project_id) REFERENCES work_items(id,company_id,project_id) ON DELETE RESTRICT,
    FOREIGN KEY(artifact_version_id,company_id) REFERENCES artifact_versions(id,company_id) ON DELETE RESTRICT
);

CREATE TABLE work_evaluations (
    id TEXT PRIMARY KEY, company_id TEXT NOT NULL, project_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL, run_id TEXT NOT NULL, artifact_version_id TEXT,
    outcome TEXT NOT NULL CHECK(outcome IN ('PASS','FAIL','NEEDS_REVISION','UNSURE')),
    method TEXT NOT NULL CHECK(method IN ('DETERMINISTIC','HUMAN','MODEL')),
    accepted INTEGER NOT NULL CHECK(accepted IN (0,1)), criteria TEXT NOT NULL,
    reason TEXT NOT NULL CHECK(length(reason) BETWEEN 1 AND 4096), reviewer_id TEXT NOT NULL, created_at TEXT NOT NULL,
    FOREIGN KEY(work_item_id,company_id,project_id) REFERENCES work_items(id,company_id,project_id),
    FOREIGN KEY(run_id,company_id) REFERENCES runs(id,company_id),
    FOREIGN KEY(artifact_version_id,company_id) REFERENCES artifact_versions(id,company_id)
);
CREATE TRIGGER work_evaluations_immutable_update BEFORE UPDATE ON work_evaluations BEGIN SELECT RAISE(ABORT,'immutable evaluation'); END;
CREATE TRIGGER work_evaluations_immutable_delete BEFORE DELETE ON work_evaluations BEGIN SELECT RAISE(ABORT,'immutable evaluation'); END;
CREATE TABLE plan_proposals (
    id TEXT PRIMARY KEY, company_id TEXT NOT NULL, project_id TEXT NOT NULL,
    source_run_id TEXT NOT NULL, source_artifact_version_id TEXT NOT NULL, project_version INTEGER NOT NULL,
    proposal TEXT NOT NULL CHECK(json_valid(proposal) AND json_type(proposal)='object'), created_at TEXT NOT NULL,
    UNIQUE(source_run_id), UNIQUE(id,company_id,project_id),
    FOREIGN KEY(project_id,company_id) REFERENCES projects(id,company_id),
    FOREIGN KEY(source_run_id,company_id) REFERENCES runs(id,company_id),
    FOREIGN KEY(source_artifact_version_id,company_id) REFERENCES artifact_versions(id,company_id)
);
CREATE TABLE plan_decisions (
    proposal_id TEXT PRIMARY KEY, company_id TEXT NOT NULL, project_id TEXT NOT NULL,
    decision TEXT NOT NULL CHECK(decision IN ('APPLY','REJECT')), actor_id TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL,
    FOREIGN KEY(proposal_id,company_id,project_id) REFERENCES plan_proposals(id,company_id,project_id)
);
CREATE TRIGGER plan_proposals_no_update BEFORE UPDATE ON plan_proposals BEGIN SELECT RAISE(ABORT,'immutable proposal'); END;
CREATE TRIGGER plan_proposals_no_delete BEFORE DELETE ON plan_proposals BEGIN SELECT RAISE(ABORT,'immutable proposal'); END;
CREATE TRIGGER plan_decisions_no_update BEFORE UPDATE ON plan_decisions BEGIN SELECT RAISE(ABORT,'immutable plan decision'); END;
CREATE TRIGGER plan_decisions_no_delete BEFORE DELETE ON plan_decisions BEGIN SELECT RAISE(ABORT,'immutable plan decision'); END;
CREATE TRIGGER m7_work_acceptance BEFORE UPDATE OF status ON work_items
WHEN NEW.status='COMPLETED' AND OLD.status!='COMPLETED' AND EXISTS(SELECT 1 FROM work_requirements q WHERE q.work_item_id=NEW.id)
AND NOT EXISTS(SELECT 1 FROM work_evaluations e JOIN runs r ON r.id=e.run_id WHERE e.work_item_id=NEW.id AND e.accepted=1 AND e.outcome='PASS' AND r.lifecycle_state='SUCCEEDED')
BEGIN SELECT RAISE(ABORT,'work requires accepted evaluation'); END;
CREATE TRIGGER m7_dependency_completion_guard BEFORE UPDATE OF status ON work_items
WHEN NEW.status='COMPLETED' AND EXISTS(SELECT 1 FROM work_requirements q WHERE q.work_item_id=NEW.id)
AND (EXISTS(SELECT 1 FROM blockers b WHERE b.work_item_id=NEW.id AND b.resolved_at IS NULL) OR EXISTS(SELECT 1 FROM work_dependencies d JOIN work_items w ON w.id=d.depends_on_work_item_id WHERE d.work_item_id=NEW.id AND d.dependency_kind='HARD' AND w.status!='COMPLETED'))
BEGIN SELECT RAISE(ABORT,'blocked work cannot complete'); END;
CREATE TRIGGER m7_one_live_run BEFORE INSERT ON runs
WHEN EXISTS(SELECT 1 FROM work_requirements q WHERE q.work_item_id=NEW.work_item_id)
AND EXISTS(SELECT 1 FROM runs r WHERE r.work_item_id=NEW.work_item_id AND r.lifecycle_state NOT IN ('SUCCEEDED','FAILED','TIMED_OUT','CANCELLED'))
BEGIN SELECT RAISE(ABORT,'work already has live run'); END;
CREATE TRIGGER m7_unknown_retry BEFORE INSERT ON runs
WHEN NEW.retry_of_run_id IS NOT NULL AND EXISTS(SELECT 1 FROM actions a JOIN runs r ON r.id=a.run_id WHERE r.work_item_id=NEW.work_item_id AND a.status='OUTCOME_UNKNOWN')
BEGIN SELECT RAISE(ABORT,'unknown action outcome requires reconciliation'); END;
