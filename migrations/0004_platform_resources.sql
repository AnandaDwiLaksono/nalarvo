-- Forward-only additions to published M1 tables.
ALTER TABLE workspaces ADD COLUMN status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('PROVISIONING', 'ACTIVE', 'SUSPENDED', 'ARCHIVED'));
ALTER TABLE workspaces ADD COLUMN row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0);
CREATE UNIQUE INDEX ux_personal_workspace_owner ON workspaces(owner_user_id) WHERE is_personal = 1;
CREATE UNIQUE INDEX ux_companies_workspace_pair ON companies(id, workspace_id);
ALTER TABLE companies ADD COLUMN activated_at TEXT;
ALTER TABLE companies ADD COLUMN archived_at TEXT;
ALTER TABLE companies ADD COLUMN mission TEXT;
ALTER TABLE companies ADD COLUMN director_user_id TEXT REFERENCES users(id) ON DELETE RESTRICT;

CREATE TABLE credential_refs (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    secret_locator TEXT NOT NULL CHECK (secret_locator LIKE 'secretstore://%' AND length(secret_locator) > 14),
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'DISABLED', 'REVOKED', 'EXPIRED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, workspace_id),
    UNIQUE(workspace_id, name)
);
CREATE TABLE provider_connections (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE RESTRICT,
    credential_ref_id TEXT,
    name TEXT NOT NULL,
    provider_kind TEXT NOT NULL,
    endpoint TEXT,
    status TEXT NOT NULL DEFAULT 'CONFIGURED' CHECK (status IN ('CONFIGURED', 'ENABLED', 'DISABLED', 'REMOVED')),
    health TEXT NOT NULL DEFAULT 'UNKNOWN' CHECK (health IN ('UNKNOWN', 'HEALTHY', 'DEGRADED', 'UNAVAILABLE')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, workspace_id),
    FOREIGN KEY (credential_ref_id, workspace_id) REFERENCES credential_refs(id, workspace_id) ON DELETE RESTRICT
);
CREATE TABLE models (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    provider_connection_id TEXT NOT NULL,
    model_key TEXT NOT NULL,
    display_name TEXT,
    capabilities_json TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(capabilities_json)),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata_json)),
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'RETIRED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, workspace_id),
    UNIQUE(provider_connection_id, model_key),
    FOREIGN KEY (provider_connection_id, workspace_id) REFERENCES provider_connections(id, workspace_id) ON DELETE RESTRICT
);
CREATE TABLE model_profiles (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    current_version INTEGER,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'RETIRED')),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(id, workspace_id),
    FOREIGN KEY (id, workspace_id, current_version) REFERENCES model_profile_versions(profile_id, workspace_id, version) ON DELETE RESTRICT
);
CREATE TABLE model_profile_versions (
    profile_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    model_id TEXT NOT NULL,
    config_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(config_json)),
    created_at TEXT NOT NULL,
    PRIMARY KEY (profile_id, version),
    UNIQUE(profile_id, workspace_id, version),
    FOREIGN KEY (profile_id, workspace_id) REFERENCES model_profiles(id, workspace_id) ON DELETE RESTRICT,
    FOREIGN KEY (model_id, workspace_id) REFERENCES models(id, workspace_id) ON DELETE RESTRICT
);
-- Version revisions are append-only, even via direct SQL.
CREATE TRIGGER model_profile_versions_no_update BEFORE UPDATE ON model_profile_versions BEGIN SELECT RAISE(ABORT, 'immutable profile version'); END;
CREATE TRIGGER model_profile_versions_no_delete BEFORE DELETE ON model_profile_versions BEGIN SELECT RAISE(ABORT, 'immutable profile version'); END;
CREATE TABLE company_workspace_resource_grants (
    company_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    model_profile_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (company_id, model_profile_id),
    FOREIGN KEY (company_id, workspace_id) REFERENCES companies(id, workspace_id) ON DELETE RESTRICT,
    FOREIGN KEY (model_profile_id, workspace_id) REFERENCES model_profiles(id, workspace_id) ON DELETE RESTRICT
);
CREATE INDEX idx_credentials_workspace ON credential_refs(workspace_id);
CREATE INDEX idx_providers_workspace ON provider_connections(workspace_id);
CREATE INDEX idx_models_workspace ON models(workspace_id);
CREATE INDEX idx_profiles_workspace ON model_profiles(workspace_id);

-- Workspace events preserve M1 company-event and outbox contracts.
CREATE TABLE workspace_domain_events (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    occurred_at TEXT NOT NULL
);
CREATE INDEX idx_workspace_events_workspace ON workspace_domain_events(workspace_id);
