CREATE TABLE departments (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL REFERENCES companies(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'RETIRED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, company_id),
    UNIQUE(company_id, name)
);
CREATE TABLE roles (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL REFERENCES companies(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'RETIRED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, company_id),
    UNIQUE(company_id, name)
);
CREATE TABLE department_roles (
    company_id TEXT NOT NULL,
    department_id TEXT NOT NULL,
    role_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (department_id, role_id),
    FOREIGN KEY (department_id, company_id) REFERENCES departments(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (role_id, company_id) REFERENCES roles(id, company_id) ON DELETE RESTRICT
);
CREATE TABLE agents (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL REFERENCES companies(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    primary_department_id TEXT NOT NULL,
    role_id TEXT NOT NULL,
    model_profile_id TEXT,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PAUSED', 'RETIRED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, company_id),
    UNIQUE(company_id, name),
    FOREIGN KEY (primary_department_id, company_id) REFERENCES departments(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (role_id, company_id) REFERENCES roles(id, company_id) ON DELETE RESTRICT,
    FOREIGN KEY (company_id, model_profile_id) REFERENCES company_workspace_resource_grants(company_id, model_profile_id) ON DELETE RESTRICT
);
CREATE TRIGGER agents_require_department_role BEFORE INSERT ON agents
WHEN NOT EXISTS (SELECT 1 FROM department_roles WHERE company_id = NEW.company_id AND department_id = NEW.primary_department_id AND role_id = NEW.role_id)
BEGIN SELECT RAISE(ABORT, 'agent role not assigned to department'); END;
CREATE TRIGGER agents_require_department_role_update BEFORE UPDATE OF company_id, primary_department_id, role_id ON agents
WHEN NOT EXISTS (SELECT 1 FROM department_roles WHERE company_id = NEW.company_id AND department_id = NEW.primary_department_id AND role_id = NEW.role_id)
BEGIN SELECT RAISE(ABORT, 'agent role not assigned to department'); END;
CREATE TRIGGER departments_retirement_guard BEFORE UPDATE OF status ON departments
WHEN NEW.status = 'RETIRED' AND EXISTS (SELECT 1 FROM agents WHERE company_id = NEW.company_id AND primary_department_id = NEW.id AND status != 'RETIRED')
BEGIN SELECT RAISE(ABORT, 'department has active agents'); END;
CREATE INDEX idx_departments_company ON departments(company_id);
CREATE INDEX idx_roles_company ON roles(company_id);
CREATE INDEX idx_agents_company ON agents(company_id);
