CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL REFERENCES companies(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    description TEXT,
    priority TEXT NOT NULL DEFAULT 'MEDIUM' CHECK (priority IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    owner_user_id TEXT REFERENCES users(id) ON DELETE RESTRICT,
    target_outcome TEXT,
    target_date TEXT,
    working_root_path TEXT,
    working_root_bound_at TEXT,
    status TEXT NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'STAFFING', 'ACTIVE', 'PAUSED', 'COMPLETED', 'CANCELLED', 'ARCHIVED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, company_id),
    UNIQUE(company_id, name)
);

CREATE TABLE objectives (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    parent_objective_id TEXT,
    title TEXT NOT NULL,
    description TEXT,
    is_primary INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0, 1)),
    is_required INTEGER NOT NULL DEFAULT 1 CHECK (is_required IN (0, 1)),
    status TEXT NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'ACTIVE', 'ACHIEVED', 'FAILED', 'CANCELLED', 'ARCHIVED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, company_id),
    UNIQUE(id, project_id),
    UNIQUE(id, company_id, project_id),
    FOREIGN KEY (project_id, company_id) REFERENCES projects(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (parent_objective_id, company_id, project_id) REFERENCES objectives(id, company_id, project_id) ON DELETE RESTRICT
);

CREATE TABLE teams (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    is_primary INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0, 1)),
    status TEXT NOT NULL DEFAULT 'FORMING' CHECK (status IN ('FORMING', 'ACTIVE', 'PAUSED', 'DISBANDED', 'ARCHIVED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, company_id),
    UNIQUE(id, project_id),
    UNIQUE(id, company_id, project_id),
    UNIQUE(project_id, name),
    FOREIGN KEY (project_id, company_id) REFERENCES projects(id, company_id) ON DELETE RESTRICT
);

CREATE TABLE staffing_requirements (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    team_id TEXT,
    role_id TEXT NOT NULL,
    department_id TEXT,
    desired_count INTEGER NOT NULL CHECK (desired_count > 0),
    required_capability_ids TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'OPEN', 'PARTIALLY_FILLED', 'FILLED', 'BLOCKED', 'CANCELLED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, company_id),
    UNIQUE(id, project_id),
    UNIQUE(id, company_id, project_id),
    FOREIGN KEY (project_id, company_id) REFERENCES projects(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (team_id, company_id, project_id) REFERENCES teams(id, company_id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (role_id, company_id) REFERENCES roles(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (department_id, company_id) REFERENCES departments(id, company_id) ON DELETE RESTRICT
);

CREATE TABLE agent_allocations (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    team_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    staffing_requirement_id TEXT,
    role_id TEXT,
    assigned_by_user_id TEXT REFERENCES users(id) ON DELETE RESTRICT,
    status TEXT NOT NULL DEFAULT 'PLANNED' CHECK (status IN ('PLANNED', 'ACTIVE', 'PAUSED', 'RELEASED', 'CANCELLED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    released_at TEXT,
    UNIQUE(id, company_id),
    UNIQUE(id, project_id),
    UNIQUE(id, company_id, project_id),
    FOREIGN KEY (project_id, company_id) REFERENCES projects(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (team_id, company_id, project_id) REFERENCES teams(id, company_id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (agent_id, company_id) REFERENCES agents(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (staffing_requirement_id, company_id, project_id) REFERENCES staffing_requirements(id, company_id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (role_id, company_id) REFERENCES roles(id, company_id) ON DELETE RESTRICT
);

CREATE TRIGGER agents_department_immutable BEFORE UPDATE OF primary_department_id ON agents
WHEN OLD.primary_department_id IS NOT NEW.primary_department_id
BEGIN SELECT RAISE(ABORT, 'immutable agent department'); END;

CREATE TRIGGER projects_working_root_immutable BEFORE UPDATE OF company_id, id ON projects
BEGIN SELECT RAISE(ABORT, 'immutable project identity'); END;

CREATE TRIGGER single_primary_objective_per_project BEFORE INSERT ON objectives
WHEN NEW.is_primary = 1 AND EXISTS (SELECT 1 FROM objectives WHERE project_id = NEW.project_id AND is_primary = 1 AND status NOT IN ('CANCELLED', 'ARCHIVED'))
BEGIN SELECT RAISE(ABORT, 'project already has a primary objective'); END;

CREATE TRIGGER single_primary_objective_per_project_update BEFORE UPDATE OF is_primary, status ON objectives
WHEN NEW.is_primary = 1 AND NEW.status NOT IN ('CANCELLED', 'ARCHIVED') AND EXISTS (SELECT 1 FROM objectives WHERE project_id = NEW.project_id AND id != NEW.id AND is_primary = 1 AND status NOT IN ('CANCELLED', 'ARCHIVED'))
BEGIN SELECT RAISE(ABORT, 'project already has a primary objective'); END;

CREATE TRIGGER single_primary_team_per_project BEFORE INSERT ON teams
WHEN NEW.is_primary = 1 AND NEW.status = 'ACTIVE' AND EXISTS (SELECT 1 FROM teams WHERE project_id = NEW.project_id AND is_primary = 1 AND status = 'ACTIVE')
BEGIN SELECT RAISE(ABORT, 'project already has a primary active team'); END;

CREATE TRIGGER single_primary_team_per_project_update BEFORE UPDATE OF is_primary, status ON teams
WHEN NEW.is_primary = 1 AND NEW.status = 'ACTIVE' AND EXISTS (SELECT 1 FROM teams WHERE project_id = NEW.project_id AND id != NEW.id AND is_primary = 1 AND status = 'ACTIVE')
BEGIN SELECT RAISE(ABORT, 'project already has a primary active team'); END;

CREATE TRIGGER agent_allocations_staffing_team BEFORE INSERT ON agent_allocations
WHEN NEW.staffing_requirement_id IS NOT NULL AND EXISTS (SELECT 1 FROM staffing_requirements WHERE id = NEW.staffing_requirement_id AND company_id = NEW.company_id AND project_id = NEW.project_id AND team_id IS NOT NULL AND team_id != NEW.team_id)
BEGIN SELECT RAISE(ABORT, 'allocation team must match staffing requirement'); END;

CREATE TRIGGER agent_allocations_staffing_team_update BEFORE UPDATE OF company_id, project_id, team_id, staffing_requirement_id ON agent_allocations
WHEN NEW.staffing_requirement_id IS NOT NULL AND EXISTS (SELECT 1 FROM staffing_requirements WHERE id = NEW.staffing_requirement_id AND company_id = NEW.company_id AND project_id = NEW.project_id AND team_id IS NOT NULL AND team_id != NEW.team_id)
BEGIN SELECT RAISE(ABORT, 'allocation team must match staffing requirement'); END;

CREATE TRIGGER agent_allocations_active_unique BEFORE INSERT ON agent_allocations
WHEN NEW.status IN ('PLANNED', 'ACTIVE', 'PAUSED') AND EXISTS (SELECT 1 FROM agent_allocations WHERE team_id = NEW.team_id AND agent_id = NEW.agent_id AND status IN ('PLANNED', 'ACTIVE', 'PAUSED'))
BEGIN SELECT RAISE(ABORT, 'agent already allocated to team'); END;

CREATE TRIGGER agent_allocations_active_unique_update BEFORE UPDATE OF status, team_id, agent_id ON agent_allocations
WHEN NEW.status IN ('PLANNED', 'ACTIVE', 'PAUSED') AND EXISTS (SELECT 1 FROM agent_allocations WHERE team_id = NEW.team_id AND agent_id = NEW.agent_id AND id != NEW.id AND status IN ('PLANNED', 'ACTIVE', 'PAUSED'))
BEGIN SELECT RAISE(ABORT, 'agent already allocated to team'); END;

CREATE TRIGGER team_disband_no_active_allocations BEFORE UPDATE OF status ON teams
WHEN NEW.status = 'DISBANDED' AND EXISTS (
    SELECT 1 FROM agent_allocations WHERE team_id = NEW.id AND status = 'ACTIVE'
)
BEGIN SELECT RAISE(ABORT, 'cannot disband team with active allocations'); END;

CREATE TRIGGER agent_allocations_capacity_limit BEFORE INSERT ON agent_allocations
WHEN NEW.status = 'ACTIVE' AND (
    SELECT COUNT(*) FROM agent_allocations
    WHERE agent_id = NEW.agent_id AND company_id = NEW.company_id AND status = 'ACTIVE'
) >= (
    SELECT capacity FROM agents WHERE id = NEW.agent_id AND company_id = NEW.company_id
)
BEGIN SELECT RAISE(ABORT, 'agent allocation exceeds capacity'); END;

CREATE TRIGGER agent_allocations_capacity_limit_update BEFORE UPDATE OF status ON agent_allocations
WHEN NEW.status = 'ACTIVE' AND OLD.status != 'ACTIVE' AND (
    SELECT COUNT(*) FROM agent_allocations
    WHERE agent_id = NEW.agent_id AND company_id = NEW.company_id AND status = 'ACTIVE' AND id != NEW.id
) >= (
    SELECT capacity FROM agents WHERE id = NEW.agent_id AND company_id = NEW.company_id
)
BEGIN SELECT RAISE(ABORT, 'agent allocation exceeds capacity'); END;

CREATE INDEX idx_projects_company ON projects(company_id);
CREATE INDEX idx_objectives_project ON objectives(project_id);
CREATE INDEX idx_teams_project ON teams(project_id);
CREATE INDEX idx_staffing_project ON staffing_requirements(project_id);
CREATE INDEX idx_allocations_agent ON agent_allocations(agent_id, status);
CREATE INDEX idx_allocations_team ON agent_allocations(team_id, status);
