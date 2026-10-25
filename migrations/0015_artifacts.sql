-- M6: immutable content metadata and artifact versions. Blob publication precedes metadata.
CREATE TABLE content_objects (
 company_id TEXT NOT NULL REFERENCES companies(id),
 hash TEXT NOT NULL CHECK(length(hash)=64 AND hash NOT GLOB '*[^0-9a-f]*'),
 size_bytes INTEGER NOT NULL CHECK(size_bytes BETWEEN 0 AND 1048576),
 storage_class TEXT NOT NULL DEFAULT 'LOCAL_SHA256' CHECK(storage_class='LOCAL_SHA256'),
 created_at TEXT NOT NULL,
 PRIMARY KEY(company_id,hash)
);
CREATE TABLE artifacts (
 id TEXT PRIMARY KEY, company_id TEXT NOT NULL REFERENCES companies(id),
 title TEXT NOT NULL, artifact_type TEXT NOT NULL,
 project_id TEXT, work_item_id TEXT, run_id TEXT, agent_id TEXT,
 lifecycle_state TEXT NOT NULL DEFAULT 'ACTIVE' CHECK(lifecycle_state IN ('ACTIVE','ARCHIVED')),
 current_version_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 row_version INTEGER NOT NULL DEFAULT 1,
 UNIQUE(id,company_id),
 FOREIGN KEY(project_id,company_id) REFERENCES projects(id,company_id),
 FOREIGN KEY(work_item_id,company_id,project_id) REFERENCES work_items(id,company_id,project_id),
 FOREIGN KEY(run_id,company_id) REFERENCES runs(id,company_id),
 FOREIGN KEY(agent_id,company_id) REFERENCES agents(id,company_id),
 FOREIGN KEY(current_version_id,id,company_id) REFERENCES artifact_versions(id,artifact_id,company_id)
);
CREATE TABLE artifact_versions (
 id TEXT PRIMARY KEY, artifact_id TEXT NOT NULL, company_id TEXT NOT NULL,
 version_no INTEGER NOT NULL CHECK(version_no>0), content_hash TEXT NOT NULL,
 media_type TEXT NOT NULL, sensitivity TEXT NOT NULL CHECK(sensitivity IN ('PUBLIC','INTERNAL','RESTRICTED','SECRET')),
 provenance TEXT NOT NULL CHECK(json_valid(provenance)), created_at TEXT NOT NULL,
 UNIQUE(artifact_id,version_no), UNIQUE(id,artifact_id,company_id), UNIQUE(id,company_id),
 FOREIGN KEY(artifact_id,company_id) REFERENCES artifacts(id,company_id),
 FOREIGN KEY(company_id,content_hash) REFERENCES content_objects(company_id,hash)
);
CREATE TRIGGER artifact_version_immutable BEFORE UPDATE ON artifact_versions BEGIN SELECT RAISE(ABORT,'immutable artifact version'); END;
CREATE TRIGGER artifact_version_retained BEFORE DELETE ON artifact_versions BEGIN SELECT RAISE(ABORT,'retained artifact version'); END;
CREATE TRIGGER content_object_immutable BEFORE UPDATE ON content_objects BEGIN SELECT RAISE(ABORT,'immutable content object'); END;
