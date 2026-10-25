CREATE TABLE context_snapshots (
 id TEXT PRIMARY KEY, company_id TEXT NOT NULL, run_id TEXT NOT NULL, invocation_id TEXT NOT NULL UNIQUE,
 references_json TEXT NOT NULL CHECK(json_valid(references_json)),
 domain_versions TEXT NOT NULL CHECK(json_valid(domain_versions)),
 model_binding TEXT NOT NULL CHECK(json_valid(model_binding)),
 prompt_hash TEXT NOT NULL CHECK(length(prompt_hash)=64),
 builder_version TEXT NOT NULL CHECK(builder_version='M6.1'),
 estimated_tokens INTEGER NOT NULL CHECK(estimated_tokens BETWEEN 0 AND 16384),
 created_at TEXT NOT NULL,
 FOREIGN KEY(run_id,company_id) REFERENCES runs(id,company_id)
);
CREATE TRIGGER context_snapshot_immutable BEFORE UPDATE ON context_snapshots BEGIN SELECT RAISE(ABORT,'immutable context snapshot'); END;
CREATE TRIGGER context_snapshot_retained BEFORE DELETE ON context_snapshots BEGIN SELECT RAISE(ABORT,'retained context snapshot'); END;
