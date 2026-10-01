CREATE TABLE work_items (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    objective_id TEXT,
    parent_work_item_id TEXT,
    title TEXT NOT NULL,
    description TEXT,
    logical_type TEXT NOT NULL CHECK (logical_type IN ('TASK', 'RESEARCH', 'REVIEW', 'DELIVERABLE', 'DECISION', 'MAINTENANCE', 'INCIDENT')),
    status TEXT NOT NULL DEFAULT 'BACKLOG' CHECK (status IN ('BACKLOG', 'READY', 'IN_PROGRESS', 'BLOCKED', 'WAITING_APPROVAL', 'COMPLETED', 'FAILED', 'CANCELLED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, company_id),
    UNIQUE(id, project_id),
    UNIQUE(id, company_id, project_id),
    FOREIGN KEY (project_id, company_id) REFERENCES projects(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (objective_id, company_id, project_id) REFERENCES objectives(id, company_id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (parent_work_item_id, company_id, project_id) REFERENCES work_items(id, company_id, project_id) ON DELETE RESTRICT
);

CREATE TABLE work_dependencies (
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    depends_on_work_item_id TEXT NOT NULL,
    dependency_kind TEXT NOT NULL DEFAULT 'HARD' CHECK (dependency_kind IN ('HARD', 'SOFT')),
    created_at TEXT NOT NULL,
    PRIMARY KEY (company_id, project_id, work_item_id, depends_on_work_item_id),
    FOREIGN KEY (work_item_id, company_id, project_id) REFERENCES work_items(id, company_id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (depends_on_work_item_id, company_id, project_id) REFERENCES work_items(id, company_id, project_id) ON DELETE RESTRICT
);

CREATE TABLE assignments (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_allocation_id TEXT NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 1 CHECK (is_primary IN (0, 1)),
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'RELEASED', 'CANCELLED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    assigned_at TEXT NOT NULL,
    ended_at TEXT,
    UNIQUE(id, company_id),
    UNIQUE(id, project_id),
    UNIQUE(id, company_id, project_id),
    FOREIGN KEY (work_item_id, company_id, project_id) REFERENCES work_items(id, company_id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (agent_id, company_id) REFERENCES agents(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (agent_allocation_id, company_id, project_id) REFERENCES agent_allocations(id, company_id, project_id) ON DELETE RESTRICT
);

CREATE TABLE blockers (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    resolved_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(id, company_id),
    UNIQUE(id, project_id),
    UNIQUE(id, company_id, project_id),
    FOREIGN KEY (work_item_id, company_id, project_id) REFERENCES work_items(id, company_id, project_id) ON DELETE RESTRICT
);

CREATE TRIGGER work_dependencies_no_hard_cycle BEFORE INSERT ON work_dependencies
WHEN NEW.dependency_kind = 'HARD' AND EXISTS (
    WITH RECURSIVE reachable(id) AS (
        SELECT depends_on_work_item_id FROM work_dependencies
        WHERE company_id = NEW.company_id AND project_id = NEW.project_id
          AND work_item_id = NEW.depends_on_work_item_id AND dependency_kind = 'HARD'
        UNION
        SELECT dependency.depends_on_work_item_id FROM work_dependencies dependency
        JOIN reachable ON dependency.work_item_id = reachable.id
        WHERE dependency.company_id = NEW.company_id AND dependency.project_id = NEW.project_id
          AND dependency.dependency_kind = 'HARD'
    ) SELECT 1 FROM reachable WHERE id = NEW.work_item_id
)
BEGIN SELECT RAISE(ABORT, 'hard dependency cycle detected'); END;

CREATE TRIGGER work_dependencies_no_hard_cycle_update BEFORE UPDATE OF company_id, project_id, work_item_id, depends_on_work_item_id, dependency_kind ON work_dependencies
WHEN NEW.dependency_kind = 'HARD' AND EXISTS (
    WITH RECURSIVE reachable(id) AS (
        SELECT depends_on_work_item_id FROM work_dependencies
        WHERE company_id = NEW.company_id AND project_id = NEW.project_id
          AND work_item_id = NEW.depends_on_work_item_id AND dependency_kind = 'HARD'
          AND NOT (company_id = OLD.company_id AND project_id = OLD.project_id AND work_item_id = OLD.work_item_id AND depends_on_work_item_id = OLD.depends_on_work_item_id)
        UNION
        SELECT dependency.depends_on_work_item_id FROM work_dependencies dependency
        JOIN reachable ON dependency.work_item_id = reachable.id
        WHERE dependency.company_id = NEW.company_id AND dependency.project_id = NEW.project_id
          AND dependency.dependency_kind = 'HARD'
          AND NOT (dependency.company_id = OLD.company_id AND dependency.project_id = OLD.project_id AND dependency.work_item_id = OLD.work_item_id AND dependency.depends_on_work_item_id = OLD.depends_on_work_item_id)
    ) SELECT 1 FROM reachable WHERE id = NEW.work_item_id
)
BEGIN SELECT RAISE(ABORT, 'hard dependency cycle detected'); END;

CREATE TRIGGER work_items_no_self_parent BEFORE INSERT ON work_items
WHEN NEW.parent_work_item_id IS NOT NULL AND NEW.id = NEW.parent_work_item_id
BEGIN SELECT RAISE(ABORT, 'work item cannot be its own parent'); END;

CREATE TRIGGER work_items_no_self_parent_update BEFORE UPDATE OF parent_work_item_id ON work_items
WHEN NEW.parent_work_item_id IS NOT NULL AND NEW.id = NEW.parent_work_item_id
BEGIN SELECT RAISE(ABORT, 'work item cannot be its own parent'); END;

CREATE TRIGGER work_dependencies_no_self_ref BEFORE INSERT ON work_dependencies
WHEN NEW.work_item_id = NEW.depends_on_work_item_id
BEGIN SELECT RAISE(ABORT, 'cannot depend on self'); END;

CREATE TRIGGER work_dependencies_no_self_ref_update BEFORE UPDATE OF work_item_id, depends_on_work_item_id ON work_dependencies
WHEN NEW.work_item_id = NEW.depends_on_work_item_id
BEGIN SELECT RAISE(ABORT, 'cannot depend on self'); END;

CREATE TRIGGER assignments_agent_allocation_match BEFORE INSERT ON assignments
WHEN NOT EXISTS (
    SELECT 1 FROM agent_allocations
    WHERE id = NEW.agent_allocation_id AND company_id = NEW.company_id AND project_id = NEW.project_id AND agent_id = NEW.agent_id
)
BEGIN SELECT RAISE(ABORT, 'assignment agent must match allocation agent'); END;

CREATE TRIGGER assignments_agent_allocation_match_update BEFORE UPDATE OF company_id, project_id, work_item_id, agent_id, agent_allocation_id ON assignments
WHEN NOT EXISTS (
    SELECT 1 FROM agent_allocations
    WHERE id = NEW.agent_allocation_id AND company_id = NEW.company_id AND project_id = NEW.project_id AND agent_id = NEW.agent_id
)
BEGIN SELECT RAISE(ABORT, 'assignment agent must match allocation agent'); END;

CREATE TRIGGER single_primary_active_assignment_per_work_item BEFORE INSERT ON assignments
WHEN NEW.is_primary = 1 AND NEW.status = 'ACTIVE' AND EXISTS (
    SELECT 1 FROM assignments WHERE work_item_id = NEW.work_item_id AND is_primary = 1 AND status = 'ACTIVE'
)
BEGIN SELECT RAISE(ABORT, 'work item already has a primary active assignment'); END;

CREATE TRIGGER single_primary_active_assignment_per_work_item_update BEFORE UPDATE OF is_primary, status ON assignments
WHEN NEW.is_primary = 1 AND NEW.status = 'ACTIVE' AND EXISTS (
    SELECT 1 FROM assignments WHERE work_item_id = NEW.work_item_id AND id != NEW.id AND is_primary = 1 AND status = 'ACTIVE'
)
BEGIN SELECT RAISE(ABORT, 'work item already has a primary active assignment'); END;

CREATE INDEX idx_work_items_project ON work_items(project_id, status);
CREATE INDEX idx_work_items_objective ON work_items(objective_id);
CREATE INDEX idx_work_dependencies_target ON work_dependencies(depends_on_work_item_id);
CREATE INDEX idx_assignments_work_item ON assignments(work_item_id, status);
CREATE INDEX idx_assignments_agent ON assignments(agent_id, status);
