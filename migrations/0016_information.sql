CREATE TABLE knowledge_sources (
 id TEXT PRIMARY KEY, company_id TEXT NOT NULL REFERENCES companies(id), project_id TEXT,
 title TEXT NOT NULL, source_type TEXT NOT NULL CHECK(source_type='MANUAL'),
 lifecycle_state TEXT NOT NULL DEFAULT 'ACTIVE' CHECK(lifecycle_state IN ('ACTIVE','RETIRED')),
 created_at TEXT NOT NULL, UNIQUE(id,company_id),
 FOREIGN KEY(project_id,company_id) REFERENCES projects(id,company_id)
);
CREATE TABLE knowledge_items (
 id TEXT PRIMARY KEY, company_id TEXT NOT NULL REFERENCES companies(id), title TEXT NOT NULL,
 scope_type TEXT NOT NULL CHECK(scope_type IN ('COMPANY','PROJECT','AGENT')),
 project_id TEXT, agent_id TEXT, source_id TEXT, source_run_id TEXT,
 visibility TEXT NOT NULL CHECK(visibility IN ('COMPANY','PROJECT','AGENT')),
 sensitivity TEXT NOT NULL CHECK(sensitivity IN ('PUBLIC','INTERNAL','RESTRICTED','SECRET')),
 lifecycle_state TEXT NOT NULL DEFAULT 'DRAFT' CHECK(lifecycle_state IN ('DRAFT','ACTIVE','RETIRED')),
 current_version_id TEXT, row_version INTEGER NOT NULL DEFAULT 1,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(id,company_id),
 CHECK(visibility=scope_type),
 CHECK((scope_type='COMPANY' AND project_id IS NULL AND agent_id IS NULL) OR (scope_type='PROJECT' AND project_id IS NOT NULL AND agent_id IS NULL) OR (scope_type='AGENT' AND agent_id IS NOT NULL)),
 FOREIGN KEY(project_id,company_id) REFERENCES projects(id,company_id),
 FOREIGN KEY(agent_id,company_id) REFERENCES agents(id,company_id),
 FOREIGN KEY(source_id,company_id) REFERENCES knowledge_sources(id,company_id),
 FOREIGN KEY(source_run_id,company_id) REFERENCES runs(id,company_id),
 FOREIGN KEY(current_version_id,id,company_id) REFERENCES knowledge_item_versions(id,entity_id,company_id)
);
CREATE TABLE knowledge_item_versions (
 id TEXT PRIMARY KEY, entity_id TEXT NOT NULL, company_id TEXT NOT NULL,
 version_no INTEGER NOT NULL CHECK(version_no>0), content_hash TEXT NOT NULL,
 provenance TEXT NOT NULL CHECK(json_valid(provenance)), created_at TEXT NOT NULL,
 UNIQUE(entity_id,version_no), UNIQUE(id,entity_id,company_id), UNIQUE(id,company_id),
 FOREIGN KEY(entity_id,company_id) REFERENCES knowledge_items(id,company_id),
 FOREIGN KEY(company_id,content_hash) REFERENCES content_objects(company_id,hash)
);
CREATE TRIGGER knowledge_item_versions_immutable BEFORE UPDATE ON knowledge_item_versions BEGIN SELECT RAISE(ABORT,'immutable information version'); END;
CREATE TRIGGER knowledge_item_versions_retained BEFORE DELETE ON knowledge_item_versions BEGIN SELECT RAISE(ABORT,'retained information version'); END;
CREATE TABLE memory_records (
 id TEXT PRIMARY KEY, company_id TEXT NOT NULL REFERENCES companies(id), title TEXT NOT NULL,
 scope_type TEXT NOT NULL CHECK(scope_type IN ('COMPANY','PROJECT','AGENT')),
 project_id TEXT, agent_id TEXT, source_id TEXT, source_run_id TEXT,
 visibility TEXT NOT NULL CHECK(visibility IN ('COMPANY','PROJECT','AGENT')),
 sensitivity TEXT NOT NULL CHECK(sensitivity IN ('PUBLIC','INTERNAL','RESTRICTED','SECRET')),
 lifecycle_state TEXT NOT NULL DEFAULT 'DRAFT' CHECK(lifecycle_state IN ('DRAFT','ACTIVE','RETIRED')),
 current_version_id TEXT, row_version INTEGER NOT NULL DEFAULT 1,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(id,company_id),
 CHECK(visibility=scope_type),
 CHECK((scope_type='COMPANY' AND project_id IS NULL AND agent_id IS NULL) OR (scope_type='PROJECT' AND project_id IS NOT NULL AND agent_id IS NULL) OR (scope_type='AGENT' AND agent_id IS NOT NULL)),
 FOREIGN KEY(project_id,company_id) REFERENCES projects(id,company_id),
 FOREIGN KEY(agent_id,company_id) REFERENCES agents(id,company_id),
 FOREIGN KEY(source_id,company_id) REFERENCES knowledge_sources(id,company_id),
 FOREIGN KEY(source_run_id,company_id) REFERENCES runs(id,company_id),
 FOREIGN KEY(current_version_id,id,company_id) REFERENCES memory_record_versions(id,entity_id,company_id)
);
CREATE TABLE memory_record_versions (
 id TEXT PRIMARY KEY, entity_id TEXT NOT NULL, company_id TEXT NOT NULL,
 version_no INTEGER NOT NULL CHECK(version_no>0), content_hash TEXT NOT NULL,
 provenance TEXT NOT NULL CHECK(json_valid(provenance)), created_at TEXT NOT NULL,
 UNIQUE(entity_id,version_no), UNIQUE(id,entity_id,company_id), UNIQUE(id,company_id),
 FOREIGN KEY(entity_id,company_id) REFERENCES memory_records(id,company_id),
 FOREIGN KEY(company_id,content_hash) REFERENCES content_objects(company_id,hash)
);
CREATE TRIGGER memory_record_versions_immutable BEFORE UPDATE ON memory_record_versions BEGIN SELECT RAISE(ABORT,'immutable information version'); END;
CREATE TRIGGER memory_record_versions_retained BEFORE DELETE ON memory_record_versions BEGIN SELECT RAISE(ABORT,'retained information version'); END;
CREATE VIRTUAL TABLE information_fts USING fts5(company_id UNINDEXED, kind UNINDEXED, entity_id UNINDEXED, version_id UNINDEXED, title, body, tokenize='unicode61');
