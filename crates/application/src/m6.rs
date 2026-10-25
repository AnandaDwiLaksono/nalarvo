//! M6 information operations. Every lookup is company scoped before blob access.
use crate::{ApplicationContext, ApplicationError};
use chrono::Utc;
use nalarvo_domain::information::{Artifact, ArtifactVersion, ContentRef, Sensitivity};
use serde::{Deserialize, Serialize};
use sqlx::{Row, Sqlite, Transaction};
use uuid::Uuid;
pub const MAX_ARTIFACT_VERSION_PAGE_SIZE: i64 = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactInput {
    pub title: String,
    pub artifact_type: String,
    pub project_id: Option<String>,
    pub work_item_id: Option<String>,
    pub run_id: Option<String>,
    pub agent_id: Option<String>,
    pub sensitivity: Sensitivity,
    pub media_type: String,
    pub content: String,
}
fn invalid() -> ApplicationError {
    ApplicationError::Validation("Invalid information or content boundary".into())
}
fn missing() -> ApplicationError {
    ApplicationError::NotFound("information".into())
}
pub(crate) fn text_valid(text: &str, maximum: usize) -> bool {
    !text.trim().is_empty() && text.len() <= maximum && !text.contains('\0')
}
impl ApplicationContext {
    pub fn validate_project_content_boundary(
        &self,
        path: &std::path::Path,
    ) -> Result<(), ApplicationError> {
        if self
            .content_store
            .as_ref()
            .is_some_and(|s| !s.permits_project_root(path))
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub async fn validate_content_roots(&self) -> Result<(), ApplicationError> {
        let roots: Vec<String> = sqlx::query_scalar(
            "SELECT working_root_path FROM projects WHERE working_root_path IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await?;
        for root in roots {
            self.validate_project_content_boundary(std::path::Path::new(&root))?;
        }
        Ok(())
    }
    pub fn with_content_store(
        mut self,
        store: std::sync::Arc<dyn crate::content_store::ContentStore>,
    ) -> Self {
        self.content_store = Some(store);
        self
    }
    pub(crate) fn store(
        &self,
    ) -> Result<&dyn crate::content_store::ContentStore, ApplicationError> {
        self.content_store.as_deref().ok_or_else(invalid)
    }
    pub(crate) async fn information_company(&self, company: &str) -> Result<(), ApplicationError> {
        let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM companies c JOIN workspaces w ON w.id=c.workspace_id WHERE c.id=? AND c.status!='ARCHIVED' AND w.status='ACTIVE'").bind(company).fetch_one(&self.pool).await?;
        if n != 1 {
            return Err(missing());
        }
        Ok(())
    }
    pub async fn create_artifact(
        &self,
        company: &str,
        input: ArtifactInput,
    ) -> Result<ArtifactVersion, ApplicationError> {
        self.write_artifact(company, None, input).await
    }
    pub async fn add_artifact_version(
        &self,
        company: &str,
        artifact: &str,
        input: ArtifactInput,
    ) -> Result<ArtifactVersion, ApplicationError> {
        self.write_artifact(company, Some(artifact), input).await
    }
    async fn write_artifact(
        &self,
        company: &str,
        artifact: Option<&str>,
        input: ArtifactInput,
    ) -> Result<ArtifactVersion, ApplicationError> {
        self.information_company(company).await?;
        if !text_valid(&input.title, 256)
            || !text_valid(&input.artifact_type, 64)
            || !text_valid(&input.media_type, 128)
            || input.content.len() > crate::content_store::MAX_CONTENT_BYTES
        {
            return Err(invalid());
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(project) = &input.project_id {
            let n: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM projects WHERE id=? AND company_id=?")
                    .bind(project)
                    .bind(company)
                    .fetch_one(&mut *tx)
                    .await?;
            if n != 1 {
                return Err(missing());
            }
        }
        if let Some(item) = &input.work_item_id {
            let n: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM work_items WHERE id=? AND company_id=? AND project_id=?",
            )
            .bind(item)
            .bind(company)
            .bind(&input.project_id)
            .fetch_one(&mut *tx)
            .await?;
            if n != 1 {
                return Err(missing());
            }
        }
        if let Some(agent) = &input.agent_id {
            let n: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM agents WHERE id=? AND company_id=?")
                    .bind(agent)
                    .bind(company)
                    .fetch_one(&mut *tx)
                    .await?;
            if n != 1 {
                return Err(missing());
            }
        }
        if let Some(run) = &input.run_id {
            let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs WHERE id=? AND company_id=? AND project_id=? AND work_item_id=? AND executing_agent_id=?").bind(run).bind(company).bind(&input.project_id).bind(&input.work_item_id).bind(&input.agent_id).fetch_one(&mut *tx).await?;
            if n != 1 {
                return Err(missing());
            }
        }
        let now = Utc::now().to_rfc3339();
        let id = artifact
            .map(str::to_owned)
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        if artifact.is_some() {
            let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM artifacts WHERE id=? AND company_id=? AND lifecycle_state='ACTIVE' AND project_id IS ? AND work_item_id IS ? AND run_id IS ? AND agent_id IS ?").bind(&id).bind(company).bind(&input.project_id).bind(&input.work_item_id).bind(&input.run_id).bind(&input.agent_id).fetch_one(&mut *tx).await?;
            if n != 1 {
                return Err(missing());
            }
        } else {
            sqlx::query("INSERT INTO artifacts(id,company_id,title,artifact_type,project_id,work_item_id,run_id,agent_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)")
                .bind(&id).bind(company).bind(&input.title).bind(&input.artifact_type).bind(&input.project_id).bind(&input.work_item_id).bind(&input.run_id).bind(&input.agent_id).bind(&now).bind(&now).execute(&mut *tx).await?;
        }
        // Publish before commit: failed metadata leaves an unreachable orphan, never a false version.
        let reference = self
            .store()?
            .put(input.content.as_bytes())
            .map_err(|_| invalid())?;
        register_content(&mut tx, company, &reference, &now).await?;
        let version: i64 = sqlx::query_scalar("SELECT COALESCE(MAX(version_no),0)+1 FROM artifact_versions WHERE artifact_id=? AND company_id=?").bind(&id).bind(company).fetch_one(&mut *tx).await?;
        let result = ArtifactVersion {
            id: Uuid::now_v7().to_string(),
            artifact_id: id,
            company_id: company.into(),
            version_no: version,
            content_ref: reference,
            media_type: input.media_type,
            sensitivity: input.sensitivity,
            provenance: serde_json::json!({"company_id":company,"project_id":input.project_id,"work_item_id":input.work_item_id,"run_id":input.run_id,"agent_id":input.agent_id,"created_by":"LOCAL_OWNER"}),
            created_at: now,
        };
        sqlx::query("INSERT INTO artifact_versions(id,artifact_id,company_id,version_no,content_hash,media_type,sensitivity,provenance,created_at) VALUES(?,?,?,?,?,?,?,?,?)")
            .bind(&result.id).bind(&result.artifact_id).bind(company).bind(version).bind(&result.content_ref.hash).bind(&result.media_type).bind(result.sensitivity.as_str()).bind(result.provenance.to_string()).bind(&result.created_at).execute(&mut *tx).await?;
        sqlx::query("UPDATE artifacts SET current_version_id=?,updated_at=?,row_version=row_version+1 WHERE id=? AND company_id=?").bind(&result.id).bind(&result.created_at).bind(&result.artifact_id).bind(company).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(result)
    }
    pub async fn list_artifacts(&self, company: &str) -> Result<Vec<Artifact>, ApplicationError> {
        self.information_company(company).await?;
        let rows = sqlx::query(
            "SELECT * FROM artifacts WHERE company_id=? ORDER BY created_at DESC LIMIT 100",
        )
        .bind(company)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| Artifact {
                id: r.get("id"),
                company_id: r.get("company_id"),
                title: r.get("title"),
                artifact_type: r.get("artifact_type"),
                project_id: r.get("project_id"),
                current_version_id: r.get("current_version_id"),
            })
            .collect())
    }
    pub async fn artifact_versions(
        &self,
        company: &str,
        artifact: &str,
    ) -> Result<Vec<ArtifactVersion>, ApplicationError> {
        self.artifact_versions_page(company, artifact, 0, MAX_ARTIFACT_VERSION_PAGE_SIZE)
            .await
    }
    pub async fn artifact_versions_page(
        &self,
        company: &str,
        artifact: &str,
        after_version: i64,
        limit: i64,
    ) -> Result<Vec<ArtifactVersion>, ApplicationError> {
        self.information_company(company).await?;
        if after_version < 0 || !(1..=MAX_ARTIFACT_VERSION_PAGE_SIZE).contains(&limit) {
            return Err(invalid());
        }
        let exists: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM artifacts WHERE company_id=? AND id=?")
                .bind(company)
                .bind(artifact)
                .fetch_one(&self.pool)
                .await?;
        if exists != 1 {
            return Err(missing());
        }
        // Immutable, unique version_no is a stable keyset cursor even during concurrent appends.
        let rows = sqlx::query("SELECT v.*,o.size_bytes FROM artifact_versions v JOIN content_objects o ON o.company_id=v.company_id AND o.hash=v.content_hash WHERE v.company_id=? AND v.artifact_id=? AND v.version_no>? ORDER BY v.version_no ASC LIMIT ?").bind(company).bind(artifact).bind(after_version).bind(limit).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                Ok(ArtifactVersion {
                    id: r.get("id"),
                    artifact_id: r.get("artifact_id"),
                    company_id: r.get("company_id"),
                    version_no: r.get("version_no"),
                    content_ref: ContentRef {
                        hash: r.get("content_hash"),
                        size_bytes: r.get::<i64, _>("size_bytes") as u64,
                    },
                    media_type: r.get("media_type"),
                    sensitivity: serde_json::from_value(serde_json::Value::String(
                        r.get("sensitivity"),
                    ))
                    .map_err(|_| invalid())?,
                    provenance: serde_json::from_str(r.get("provenance")).map_err(|_| invalid())?,
                    created_at: r.get("created_at"),
                })
            })
            .collect()
    }
    pub async fn read_artifact_content(
        &self,
        company: &str,
        artifact: &str,
        version: &str,
    ) -> Result<Vec<u8>, ApplicationError> {
        self.information_company(company).await?;
        let row = sqlx::query("SELECT v.content_hash,o.size_bytes FROM artifact_versions v JOIN content_objects o ON o.company_id=v.company_id AND o.hash=v.content_hash WHERE v.company_id=? AND v.artifact_id=? AND v.id=?")
            .bind(company).bind(artifact).bind(version).fetch_optional(&self.pool).await?.ok_or_else(missing)?;
        self.store()?
            .read(&ContentRef {
                hash: row.get("content_hash"),
                size_bytes: row.get::<i64, _>("size_bytes") as u64,
            })
            .map_err(|_| invalid())
    }
}
pub(crate) async fn register_content(
    tx: &mut Transaction<'_, Sqlite>,
    company: &str,
    reference: &ContentRef,
    now: &str,
) -> Result<(), ApplicationError> {
    sqlx::query("INSERT INTO content_objects(company_id,hash,size_bytes,created_at) VALUES(?,?,?,?) ON CONFLICT(company_id,hash) DO NOTHING").bind(company).bind(&reference.hash).bind(reference.size_bytes as i64).bind(now).execute(&mut **tx).await?;
    let size: i64 =
        sqlx::query_scalar("SELECT size_bytes FROM content_objects WHERE company_id=? AND hash=?")
            .bind(company)
            .bind(&reference.hash)
            .fetch_one(&mut **tx)
            .await?;
    if size as u64 != reference.size_bytes {
        return Err(invalid());
    }
    Ok(())
}
