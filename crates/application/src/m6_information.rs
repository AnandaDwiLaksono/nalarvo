//! Explicit owner-curated information; raw model output is never captured automatically.
use crate::{
    ApplicationContext, ApplicationError,
    m6::{register_content, text_valid},
};
use chrono::Utc;
use nalarvo_domain::information::*;
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction, sqlite::SqliteRow};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceInput {
    pub title: String,
    pub project_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RetrievalScope {
    pub project_id: Option<String>,
    pub agent_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InformationInput {
    pub title: String,
    pub content: String,
    pub scope: InformationScope,
    pub project_id: Option<String>,
    pub agent_id: Option<String>,
    pub sensitivity: Sensitivity,
    pub source_id: Option<String>,
    pub source_run_id: Option<String>,
}
fn invalid() -> ApplicationError {
    ApplicationError::Validation("Invalid information boundary".into())
}
fn missing() -> ApplicationError {
    ApplicationError::NotFound("information".into())
}
fn tables(kind: &str) -> Result<(&'static str, &'static str), ApplicationError> {
    match kind {
        "KNOWLEDGE" => Ok(("knowledge_items", "knowledge_item_versions")),
        "MEMORY" => Ok(("memory_records", "memory_record_versions")),
        _ => Err(invalid()),
    }
}
/// Conservative heuristic, not a secret classifier. Explicit restricted/secret always excluded.
pub fn safe_context_text(text: &str) -> bool {
    let s = text.to_ascii_lowercase();
    ![
        "password",
        "api_key",
        "api-key",
        "authorization:",
        "bearer ",
        "private key",
        "credential",
        "secret=",
        "secret:",
        "sk-",
    ]
    .iter()
    .any(|p| s.contains(p))
        && !text.contains('\0')
}
fn decode<T: serde::de::DeserializeOwned>(s: String) -> Result<T, ApplicationError> {
    serde_json::from_value(s.into()).map_err(|_| invalid())
}
fn record(row: SqliteRow, kind: &str) -> Result<InformationRecord, ApplicationError> {
    let scope: InformationScope = decode(row.get("scope_type"))?;
    let visibility: String = row.get("visibility");
    let project: Option<String> = row.get("project_id");
    let agent: Option<String> = row.get("agent_id");
    if visibility != scope.as_str()
        || !match scope {
            InformationScope::Company => project.is_none() && agent.is_none(),
            InformationScope::Project => project.is_some() && agent.is_none(),
            InformationScope::Agent => agent.is_some(),
        }
    {
        return Err(invalid());
    }
    Ok(InformationRecord {
        id: row.get("id"),
        kind: kind.into(),
        company_id: row.get("company_id"),
        title: row.get("title"),
        scope: decode(row.get("scope_type"))?,
        project_id: row.get("project_id"),
        agent_id: row.get("agent_id"),
        sensitivity: decode(row.get("sensitivity"))?,
        lifecycle_state: decode(row.get("lifecycle_state"))?,
        row_version: row.get("row_version"),
        version_id: row.get("version_id"),
        version_no: row.get("version_no"),
        content_ref: ContentRef {
            hash: row.get("content_hash"),
            size_bytes: row.get::<i64, _>("size_bytes") as u64,
        },
        provenance: serde_json::from_str(row.get("provenance")).map_err(|_| invalid())?,
        updated_at: row.get("updated_at"),
    })
}
fn select(table: &str, versions: &str) -> String {
    format!(
        "SELECT e.*,v.id AS version_id,v.version_no,v.content_hash,v.provenance,o.size_bytes FROM {table} e JOIN {versions} v ON v.id=e.current_version_id AND v.entity_id=e.id AND v.company_id=e.company_id JOIN content_objects o ON o.company_id=v.company_id AND o.hash=v.content_hash"
    )
}
impl ApplicationContext {
    pub async fn validate_retrieval_scope(
        &self,
        company: &str,
        scope: &RetrievalScope,
    ) -> Result<(), ApplicationError> {
        self.information_company(company).await?;
        if let Some(p) = &scope.project_id {
            let n: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM projects WHERE company_id=? AND id=? AND status='ACTIVE'",
            )
            .bind(company)
            .bind(p)
            .fetch_one(&self.pool)
            .await?;
            if n != 1 {
                return Err(missing());
            }
        }
        if let Some(a) = &scope.agent_id {
            let n: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM agents WHERE company_id=? AND id=? AND status='ACTIVE'",
            )
            .bind(company)
            .bind(a)
            .fetch_one(&self.pool)
            .await?;
            if n != 1 {
                return Err(missing());
            }
        }
        Ok(())
    }
    pub async fn create_knowledge_source(
        &self,
        company: &str,
        input: SourceInput,
    ) -> Result<KnowledgeSource, ApplicationError> {
        self.validate_retrieval_scope(
            company,
            &RetrievalScope {
                project_id: input.project_id.clone(),
                agent_id: None,
            },
        )
        .await?;
        if !text_valid(&input.title, 256) || !safe_context_text(&input.title) {
            return Err(invalid());
        }
        let s = KnowledgeSource {
            id: Uuid::now_v7().to_string(),
            company_id: company.into(),
            title: input.title,
            project_id: input.project_id,
        };
        sqlx::query("INSERT INTO knowledge_sources(id,company_id,title,project_id,source_type,created_at) VALUES(?,?,?,?,'MANUAL',?)").bind(&s.id).bind(company).bind(&s.title).bind(&s.project_id).bind(Utc::now().to_rfc3339()).execute(&self.pool).await?;
        Ok(s)
    }
    pub async fn create_knowledge(
        &self,
        company: &str,
        input: InformationInput,
    ) -> Result<InformationRecord, ApplicationError> {
        self.write_information(company, "KNOWLEDGE", None, input, None)
            .await
    }
    pub async fn capture_memory_candidate(
        &self,
        company: &str,
        input: InformationInput,
    ) -> Result<InformationRecord, ApplicationError> {
        self.write_information(company, "MEMORY", None, input, None)
            .await
    }
    pub async fn revise_knowledge(
        &self,
        company: &str,
        id: &str,
        input: InformationInput,
        expected: i64,
    ) -> Result<InformationRecord, ApplicationError> {
        self.write_information(company, "KNOWLEDGE", Some(id), input, Some(expected))
            .await
    }
    async fn write_information(
        &self,
        company: &str,
        kind: &str,
        id: Option<&str>,
        input: InformationInput,
        expected: Option<i64>,
    ) -> Result<InformationRecord, ApplicationError> {
        let (table, versions) = tables(kind)?;
        self.validate_retrieval_scope(
            company,
            &RetrievalScope {
                project_id: input.project_id.clone(),
                agent_id: input.agent_id.clone(),
            },
        )
        .await?;
        if !text_valid(&input.title, 256)
            || !text_valid(&input.content, 65536)
            || !safe_context_text(&input.title)
            || (!input.sensitivity.retrievable() && kind == "MEMORY")
            || (input.sensitivity.retrievable() && !safe_context_text(&input.content))
        {
            return Err(invalid());
        }
        if !match input.scope {
            InformationScope::Company => input.project_id.is_none() && input.agent_id.is_none(),
            InformationScope::Project => input.project_id.is_some() && input.agent_id.is_none(),
            InformationScope::Agent => input.agent_id.is_some(),
        } {
            return Err(invalid());
        }
        if kind == "KNOWLEDGE" && input.source_id.is_none()
            || kind == "MEMORY" && input.source_id.is_some()
        {
            return Err(invalid());
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(s) = &input.source_id {
            let n:i64=sqlx::query_scalar("SELECT COUNT(*) FROM knowledge_sources WHERE id=? AND company_id=? AND lifecycle_state='ACTIVE' AND (project_id IS NULL OR project_id=?)").bind(s).bind(company).bind(&input.project_id).fetch_one(&mut *tx).await?;
            if n != 1 {
                return Err(missing());
            }
        }
        if let Some(run) = &input.source_run_id {
            let n:i64=sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE id=? AND company_id=? AND project_id=? AND executing_agent_id=?").bind(run).bind(company).bind(&input.project_id).bind(&input.agent_id).fetch_one(&mut *tx).await?;
            if n != 1 {
                return Err(missing());
            }
        }
        let now = Utc::now().to_rfc3339();
        let entity = id
            .map(str::to_owned)
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        if id.is_some() {
            let n=sqlx::query(&format!("UPDATE {table} SET title=?,sensitivity=?,updated_at=?,row_version=row_version+1 WHERE id=? AND company_id=? AND row_version=? AND lifecycle_state!='RETIRED' AND scope_type=? AND project_id IS ? AND agent_id IS ? AND source_id IS ? AND source_run_id IS ?")).bind(&input.title).bind(input.sensitivity.as_str()).bind(&now).bind(&entity).bind(company).bind(expected).bind(input.scope.as_str()).bind(&input.project_id).bind(&input.agent_id).bind(&input.source_id).bind(&input.source_run_id).execute(&mut *tx).await?.rows_affected();
            if n != 1 {
                return Err(invalid());
            }
        } else {
            sqlx::query(&format!("INSERT INTO {table}(id,company_id,title,scope_type,project_id,agent_id,source_id,source_run_id,visibility,sensitivity,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")).bind(&entity).bind(company).bind(&input.title).bind(input.scope.as_str()).bind(&input.project_id).bind(&input.agent_id).bind(&input.source_id).bind(&input.source_run_id).bind(input.scope.as_str()).bind(input.sensitivity.as_str()).bind(&now).bind(&now).execute(&mut *tx).await?;
        }
        let reference = self
            .store()?
            .put(input.content.as_bytes())
            .map_err(|_| invalid())?;
        register_content(&mut tx, company, &reference, &now).await?;
        let version:i64=sqlx::query_scalar(&format!("SELECT COALESCE(MAX(version_no),0)+1 FROM {versions} WHERE entity_id=? AND company_id=?")).bind(&entity).bind(company).fetch_one(&mut *tx).await?;
        let vid = Uuid::now_v7().to_string();
        let provenance = serde_json::json!({"company_id":company,"source_id":input.source_id,"source_run_id":input.source_run_id,"capture":"EXPLICIT_OWNER_CURATED","authority":"NONE","scope":input.scope,"project_id":input.project_id,"agent_id":input.agent_id,"sensitivity":input.sensitivity,"title":input.title});
        sqlx::query(&format!("INSERT INTO {versions}(id,entity_id,company_id,version_no,content_hash,provenance,created_at) VALUES(?,?,?,?,?,?,?)")).bind(&vid).bind(&entity).bind(company).bind(version).bind(&reference.hash).bind(provenance.to_string()).bind(&now).execute(&mut *tx).await?;
        sqlx::query(&format!(
            "UPDATE {table} SET current_version_id=? WHERE id=? AND company_id=?"
        ))
        .bind(&vid)
        .bind(&entity)
        .bind(company)
        .execute(&mut *tx)
        .await?;
        self.index_one(&mut tx, company, kind, &entity).await?;
        tx.commit().await?;
        self.get_information(company, kind, &entity).await
    }
    pub async fn get_information(
        &self,
        company: &str,
        kind: &str,
        id: &str,
    ) -> Result<InformationRecord, ApplicationError> {
        self.information_company(company).await?;
        let (t, v) = tables(kind)?;
        let row = sqlx::query(&format!("{} WHERE e.company_id=? AND e.id=?", select(t, v)))
            .bind(company)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(missing)?;
        record(row, kind)
    }
    pub async fn list_information(
        &self,
        company: &str,
        kind: &str,
    ) -> Result<Vec<InformationRecord>, ApplicationError> {
        self.information_company(company).await?;
        let (t, v) = tables(kind)?;
        sqlx::query(&format!(
            "{} WHERE e.company_id=? ORDER BY e.updated_at DESC LIMIT 100",
            select(t, v)
        ))
        .bind(company)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|r| record(r, kind))
        .collect()
    }
    pub async fn transition_information(
        &self,
        company: &str,
        kind: &str,
        id: &str,
        state: InformationLifecycle,
        expected: i64,
    ) -> Result<InformationRecord, ApplicationError> {
        self.information_company(company).await?;
        let (t, _) = tables(kind)?;
        if state == InformationLifecycle::Draft {
            return Err(invalid());
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let n=sqlx::query(&format!("UPDATE {t} SET lifecycle_state=?,row_version=row_version+1,updated_at=? WHERE id=? AND company_id=? AND row_version=? AND ((lifecycle_state='DRAFT' AND ?='ACTIVE') OR (lifecycle_state IN ('DRAFT','ACTIVE') AND ?='RETIRED'))")).bind(state.as_str()).bind(Utc::now().to_rfc3339()).bind(id).bind(company).bind(expected).bind(state.as_str()).bind(state.as_str()).execute(&mut *tx).await?.rows_affected();
        if n != 1 {
            return Err(invalid());
        }
        self.index_one(&mut tx, company, kind, id).await?;
        tx.commit().await?;
        self.get_information(company, kind, id).await
    }
    async fn index_one(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        company: &str,
        kind: &str,
        id: &str,
    ) -> Result<(), ApplicationError> {
        let (t, v) = tables(kind)?;
        sqlx::query("DELETE FROM information_fts WHERE company_id=? AND kind=? AND entity_id=?")
            .bind(company)
            .bind(kind)
            .bind(id)
            .execute(&mut **tx)
            .await?;
        let row=sqlx::query(&format!("{} WHERE e.company_id=? AND e.id=? AND e.lifecycle_state='ACTIVE' AND e.sensitivity IN ('PUBLIC','INTERNAL')",select(t,v))).bind(company).bind(id).fetch_optional(&mut **tx).await?;
        if let Some(row) = row {
            let r = record(row, kind)?;
            let bytes = self.store()?.read(&r.content_ref).map_err(|_| invalid())?;
            let text = String::from_utf8(bytes).map_err(|_| invalid())?;
            if !safe_context_text(&text) || !safe_context_text(&r.title) {
                return Err(invalid());
            }
            sqlx::query("INSERT INTO information_fts(company_id,kind,entity_id,version_id,title,body) VALUES(?,?,?,?,?,?)").bind(company).bind(kind).bind(id).bind(r.version_id).bind(r.title).bind(text).execute(&mut **tx).await?;
        }
        Ok(())
    }
    pub async fn rebuild_information_index(&self, company: &str) -> Result<(), ApplicationError> {
        self.information_company(company).await?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        sqlx::query("DELETE FROM information_fts WHERE company_id=?")
            .bind(company)
            .execute(&mut *tx)
            .await?;
        for kind in ["KNOWLEDGE", "MEMORY"] {
            let (t, _) = tables(kind)?;
            let ids: Vec<String> =
                sqlx::query_scalar(&format!("SELECT id FROM {t} WHERE company_id=?"))
                    .bind(company)
                    .fetch_all(&mut *tx)
                    .await?;
            for id in ids {
                self.index_one(&mut tx, company, kind, &id).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn search_information(
        &self,
        company: &str,
        scope: &RetrievalScope,
        query: &str,
    ) -> Result<Vec<InformationRecord>, ApplicationError> {
        self.validate_retrieval_scope(company, scope).await?;
        if !text_valid(query, 256) || !safe_context_text(query) {
            return Err(invalid());
        }
        let terms: Vec<_> = query
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty())
            .take(12)
            .map(|s| format!("\"{s}\""))
            .collect();
        if terms.is_empty() {
            return Ok(vec![]);
        }
        let ids=sqlx::query("SELECT kind,entity_id,version_id FROM information_fts WHERE information_fts MATCH ? AND company_id=? ORDER BY rank LIMIT 100").bind(terms.join(" OR ")).bind(company).fetch_all(&self.pool).await?;
        let mut found = Vec::new();
        for row in ids {
            let kind: String = row.get("kind");
            let id: String = row.get("entity_id");
            let version: String = row.get("version_id");
            let r = self.get_information(company, &kind, &id).await?;
            if r.version_id == version
                && r.lifecycle_state == InformationLifecycle::Active
                && r.sensitivity.retrievable()
                && (r.project_id.is_none() || r.project_id == scope.project_id)
                && (r.agent_id.is_none() || r.agent_id == scope.agent_id)
            {
                let bytes = self.store()?.read(&r.content_ref).map_err(|_| invalid())?;
                if !safe_context_text(std::str::from_utf8(&bytes).map_err(|_| invalid())?) {
                    return Err(invalid());
                }
                if kind == "KNOWLEDGE" {
                    let source = r.provenance["source_id"].as_str().ok_or_else(invalid)?;
                    let n:i64=sqlx::query_scalar("SELECT COUNT(*) FROM knowledge_sources WHERE id=? AND company_id=? AND lifecycle_state='ACTIVE'").bind(source).bind(company).fetch_one(&self.pool).await?;
                    if n != 1 {
                        continue;
                    }
                }
                found.push(r);
                if found.len() == 24 {
                    break;
                }
            }
        }
        Ok(found)
    }
}
