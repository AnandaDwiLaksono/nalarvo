use chrono::{DateTime, Duration, Utc};
use nalarvo_domain::{
    Company, CompanyId, CompanyStatus, DomainError, DomainEvent, OutboxMessage, OutboxStatus,
    PrincipalRef, PrincipalType, ScopeRef, ScopeType, UserId, WorkspaceId,
};
use sha2::{Digest, Sha256};
use sqlx::{
    Row, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("Domain error: {0}")]
    Domain(#[from] DomainError),

    #[error("Idempotency key reuse mismatch: {0}")]
    IdempotencyMismatch(String),

    #[error("Optimistic lock conflict: current version is {current}, expected {expected}")]
    StaleVersion { current: i64, expected: i64 },

    #[error("Company not found: {0}")]
    NotFound(String),
}

pub async fn create_pool(database_url: &str) -> Result<SqlitePool, PersistenceError> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(std::time::Duration::from_millis(5000));

    let pool = SqlitePoolOptions::new()
        .max_connections(10)
        .connect_with(options)
        .await?;

    Ok(pool)
}

pub async fn run_migrations(pool: &SqlitePool) -> Result<(), PersistenceError> {
    sqlx::migrate!("../../migrations").run(pool).await?;
    Ok(())
}

pub fn hash_request(payload: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hex::encode(hasher.finalize())
}

pub async fn bootstrap_personal_workspace(
    pool: &SqlitePool,
    user_id: &UserId,
    workspace_id: &WorkspaceId,
    email: &str,
    full_name: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO users (id, email, full_name, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?) ON CONFLICT(email) DO NOTHING",
    )
    .bind(&user_id.0)
    .bind(email)
    .bind(full_name)
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    let owner_id: String = sqlx::query("SELECT id FROM users WHERE email = ?")
        .bind(email)
        .fetch_one(&mut *tx)
        .await?
        .get(0);

    if let Some(existing) =
        sqlx::query("SELECT id FROM workspaces WHERE owner_user_id = ? AND is_personal = 1")
            .bind(&owner_id)
            .fetch_optional(&mut *tx)
            .await?
    {
        let existing_id: String = existing.get(0);
        sqlx::query(
            "INSERT INTO workspace_memberships (workspace_id, user_id, role, created_at)
             VALUES (?, ?, 'OWNER', ?) ON CONFLICT(workspace_id, user_id) DO NOTHING",
        )
        .bind(existing_id)
        .bind(&owner_id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(());
    }

    sqlx::query(
        "INSERT INTO workspaces (id, owner_user_id, name, slug, is_personal, status, created_at, updated_at)
         VALUES (?, ?, 'Personal Workspace', ?, 1, 'PROVISIONING', ?, ?)",
    )
    .bind(&workspace_id.0)
    .bind(&owner_id)
    .bind(format!("personal-{}", &owner_id[..8.min(owner_id.len())]))
    .bind(&now)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO workspace_memberships (workspace_id, user_id, role, created_at)
         VALUES (?, ?, 'OWNER', ?)",
    )
    .bind(&workspace_id.0)
    .bind(&owner_id)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO workspace_domain_events (id, workspace_id, event_type, payload, occurred_at)
         VALUES (?, ?, 'WorkspaceProvisioned', '{\"lifecycle\":\"PROVISIONING\"}', ?)",
    )
    .bind(Uuid::now_v7().to_string())
    .bind(&workspace_id.0)
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    sqlx::query("UPDATE workspaces SET status = 'ACTIVE', updated_at = ?, row_version = row_version + 1 WHERE id = ? AND status = 'PROVISIONING'")
        .bind(&now)
        .bind(&workspace_id.0)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub enum IdempotencyCheck {
    Proceed,
    Cached(String),
}

pub async fn check_idempotency_tx(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &str,
    key: &str,
    request_hash: &str,
) -> Result<IdempotencyCheck, PersistenceError> {
    let row = sqlx::query(
        "SELECT request_hash, status, response_body FROM idempotency_records WHERE scope = ? AND idempotency_key = ?",
    )
    .bind(scope)
    .bind(key)
    .fetch_optional(&mut **tx)
    .await?;

    if let Some(row) = row {
        let saved_hash: String = row.get(0);
        let status: String = row.get(1);
        let response_body: Option<String> = row.get(2);

        if saved_hash != request_hash {
            return Err(PersistenceError::IdempotencyMismatch(key.to_string()));
        }

        if status == "COMPLETED"
            && let Some(body) = response_body
        {
            return Ok(IdempotencyCheck::Cached(body));
        }
    }

    Ok(IdempotencyCheck::Proceed)
}

pub async fn save_idempotency_record_tx(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &str,
    key: &str,
    request_hash: &str,
    response_body: &str,
) -> Result<(), PersistenceError> {
    let id = Uuid::now_v7().to_string();
    let now = Utc::now();
    let expires = now + Duration::days(7);

    sqlx::query(
        "INSERT INTO idempotency_records (id, scope, idempotency_key, request_hash, status, response_body, created_at, expires_at)
         VALUES (?, ?, ?, ?, 'COMPLETED', ?, ?, ?)
         ON CONFLICT(scope, idempotency_key) DO UPDATE SET
            status = 'COMPLETED',
            response_body = excluded.response_body",
    )
    .bind(id)
    .bind(scope)
    .bind(key)
    .bind(request_hash)
    .bind(response_body)
    .bind(now.to_rfc3339())
    .bind(expires.to_rfc3339())
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn insert_company_tx(
    tx: &mut Transaction<'_, Sqlite>,
    company: &Company,
) -> Result<(), PersistenceError> {
    sqlx::query(
        "INSERT INTO companies (id, workspace_id, name, description, mission, director_user_id, status, row_version, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&company.id.0)
    .bind(&company.workspace_id.0)
    .bind(&company.name)
    .bind(&company.description)
    .bind(&company.mission)
    .bind(company.director_user_id.as_ref().map(|id| &id.0))
    .bind(company.status.to_string())
    .bind(company.row_version)
    .bind(company.created_at.to_rfc3339())
    .bind(company.updated_at.to_rfc3339())
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn insert_domain_event_and_outbox_tx(
    tx: &mut Transaction<'_, Sqlite>,
    event: &DomainEvent,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let payload_str = serde_json::to_string(&event.payload)
        .map_err(|e| DomainError::Validation(e.to_string()))?;

    sqlx::query(
        "INSERT INTO domain_events (
            id, event_type, schema_version, company_id, aggregate_type, aggregate_id,
            aggregate_version, occurred_at, correlation_id, causation_id,
            principal_type, principal_id, scope_type, scope_id, payload, created_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&event.event_id)
    .bind(&event.event_type)
    .bind(event.schema_version as i64)
    .bind(&event.company_id.0)
    .bind(&event.aggregate_type)
    .bind(&event.aggregate_id)
    .bind(event.aggregate_version)
    .bind(event.occurred_at.to_rfc3339())
    .bind(&event.correlation_id)
    .bind(&event.causation_id)
    .bind(event.principal.principal_type.to_string())
    .bind(&event.principal.principal_id)
    .bind(event.scope.scope_type.to_string())
    .bind(&event.scope.scope_id)
    .bind(payload_str)
    .bind(&now)
    .execute(&mut **tx)
    .await?;

    let outbox_id = Uuid::now_v7().to_string();

    sqlx::query(
        "INSERT INTO outbox_messages (
            id, domain_event_id, status, attempt_count, available_at, created_at
         ) VALUES (?, ?, 'PENDING', 0, ?, ?)",
    )
    .bind(outbox_id)
    .bind(&event.event_id)
    .bind(&now)
    .bind(&now)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

pub async fn get_company(
    pool: &SqlitePool,
    workspace_id: &WorkspaceId,
    company_id: &CompanyId,
) -> Result<Option<Company>, PersistenceError> {
    let row = sqlx::query(
        "SELECT id, workspace_id, name, description, status, row_version, created_at, updated_at, mission, director_user_id
         FROM companies
         WHERE id = ? AND workspace_id = ?",
    )
    .bind(&company_id.0)
    .bind(&workspace_id.0)
    .fetch_optional(pool)
    .await?;

    row.map(row_to_company).transpose()
}

pub async fn get_company_by_id(
    pool: &SqlitePool,
    company_id: &CompanyId,
) -> Result<Option<Company>, PersistenceError> {
    let row = sqlx::query(
        "SELECT id, workspace_id, name, description, status, row_version, created_at, updated_at, mission, director_user_id
         FROM companies
         WHERE id = ?",
    )
    .bind(&company_id.0)
    .fetch_optional(pool)
    .await?;

    row.map(row_to_company).transpose()
}

pub async fn list_companies(
    pool: &SqlitePool,
    workspace_id: &WorkspaceId,
) -> Result<Vec<Company>, PersistenceError> {
    let rows = sqlx::query(
        "SELECT id, workspace_id, name, description, status, row_version, created_at, updated_at, mission, director_user_id
         FROM companies
         WHERE workspace_id = ?
         ORDER BY created_at ASC",
    )
    .bind(&workspace_id.0)
    .fetch_all(pool)
    .await?;

    rows.into_iter().map(row_to_company).collect()
}

pub async fn update_company_tx(
    tx: &mut Transaction<'_, Sqlite>,
    company: &Company,
    expected_version: i64,
) -> Result<(), PersistenceError> {
    let result = sqlx::query(
        "UPDATE companies
         SET name = ?, description = ?, mission = ?, director_user_id = ?, status = ?, activated_at = CASE WHEN ? = 'ACTIVE' AND status != 'ACTIVE' THEN ? ELSE activated_at END, archived_at = CASE WHEN ? = 'ARCHIVED' THEN ? ELSE archived_at END, row_version = row_version + 1, updated_at = ?
         WHERE id = ? AND workspace_id = ? AND row_version = ?",
    )
    .bind(&company.name)
    .bind(&company.description)
    .bind(&company.mission)
    .bind(company.director_user_id.as_ref().map(|id| &id.0))
    .bind(company.status.to_string())
    .bind(company.status.to_string())
    .bind(company.updated_at.to_rfc3339())
    .bind(company.status.to_string())
    .bind(company.updated_at.to_rfc3339())
    .bind(company.updated_at.to_rfc3339())
    .bind(&company.id.0)
    .bind(&company.workspace_id.0)
    .bind(expected_version)
    .execute(&mut **tx)
    .await?;

    if result.rows_affected() == 0 {
        // Fetch current version to report accurate StaleVersion error
        let row =
            sqlx::query("SELECT row_version FROM companies WHERE id = ? AND workspace_id = ?")
                .bind(&company.id.0)
                .bind(&company.workspace_id.0)
                .fetch_optional(&mut **tx)
                .await?;

        if let Some(row) = row {
            let current_version: i64 = row.get(0);
            return Err(PersistenceError::StaleVersion {
                current: current_version,
                expected: expected_version,
            });
        } else {
            return Err(PersistenceError::NotFound(company.id.0.clone()));
        }
    }

    Ok(())
}

pub async fn fetch_pending_outbox(
    pool: &SqlitePool,
    limit: i64,
    lease_owner: &str,
    lease_seconds: i64,
) -> Result<Vec<(OutboxMessage, DomainEvent)>, PersistenceError> {
    let mut tx = pool.begin().await?;
    let now = Utc::now();
    let now_str = now.to_rfc3339();
    let lease_until = (now + Duration::seconds(lease_seconds)).to_rfc3339();

    let rows = sqlx::query(
        "SELECT o.id, o.domain_event_id, o.status, o.attempt_count, o.available_at,
                o.lease_owner, o.lease_until, o.last_error, o.created_at, o.published_at,
                e.id, e.event_type, e.schema_version, e.company_id, e.aggregate_type,
                e.aggregate_id, e.aggregate_version, e.occurred_at, e.correlation_id,
                e.causation_id, e.payload, e.principal_type, e.principal_id, e.scope_type, e.scope_id
         FROM outbox_messages o
         JOIN domain_events e ON o.domain_event_id = e.id
         WHERE o.status = 'PENDING'
           AND o.available_at <= ?
           AND (o.lease_until IS NULL OR o.lease_until < ?)
         ORDER BY o.created_at ASC
         LIMIT ?",
    )
    .bind(&now_str)
    .bind(&now_str)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;

    let mut result = Vec::with_capacity(rows.len());

    for row in rows {
        let outbox_id: String = row.get(0);
        let domain_event_id: String = row.get(1);
        let status_str: String = row.get(2);
        let attempt_count: i64 = row.get(3);
        let available_at_str: String = row.get(4);
        let _saved_lease_owner: Option<String> = row.get(5);
        let _saved_lease_until: Option<String> = row.get(6);
        let last_error: Option<String> = row.get(7);
        let created_at_str: String = row.get(8);
        let published_at_str: Option<String> = row.get(9);

        let event_id: String = row.get(10);
        let event_type: String = row.get(11);
        let schema_version: i64 = row.get(12);
        let company_id_str: String = row.get(13);
        let aggregate_type: String = row.get(14);
        let aggregate_id: String = row.get(15);
        let aggregate_version: i64 = row.get(16);
        let occurred_at_str: String = row.get(17);
        let correlation_id: String = row.get(18);
        let causation_id: String = row.get(19);
        let payload_str: String = row.get(20);
        let principal_type_str: String = row.get(21);
        let principal_id: String = row.get(22);
        let scope_type_str: String = row.get(23);
        let scope_id: String = row.get(24);

        // Claim lease
        sqlx::query(
            "UPDATE outbox_messages
             SET lease_owner = ?, lease_until = ?, attempt_count = attempt_count + 1
             WHERE id = ?",
        )
        .bind(lease_owner)
        .bind(&lease_until)
        .bind(&outbox_id)
        .execute(&mut *tx)
        .await?;

        let status = match status_str.as_str() {
            "PUBLISHED" => OutboxStatus::Published,
            "FAILED" => OutboxStatus::Failed,
            _ => OutboxStatus::Pending,
        };

        let outbox = OutboxMessage {
            id: outbox_id,
            domain_event_id,
            status,
            attempt_count: attempt_count as u32,
            available_at: DateTime::parse_from_rfc3339(&available_at_str)
                .map(|t| t.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            lease_owner: Some(lease_owner.to_string()),
            lease_until: DateTime::parse_from_rfc3339(&lease_until)
                .ok()
                .map(|t| t.with_timezone(&Utc)),
            last_error,
            created_at: DateTime::parse_from_rfc3339(&created_at_str)
                .map(|t| t.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            published_at: published_at_str.and_then(|s| {
                DateTime::parse_from_rfc3339(&s)
                    .ok()
                    .map(|t| t.with_timezone(&Utc))
            }),
        };

        let payload = serde_json::from_str(&payload_str).unwrap_or(serde_json::Value::Null);

        let principal_type = match principal_type_str.as_str() {
            "AGENT" => PrincipalType::Agent,
            "SYSTEM" => PrincipalType::System,
            _ => PrincipalType::User,
        };
        let scope_type = match scope_type_str.as_str() {
            "WORKSPACE" => ScopeType::Workspace,
            _ => ScopeType::Company,
        };

        let event = DomainEvent {
            event_id,
            event_type,
            schema_version: schema_version as u32,
            company_id: CompanyId(company_id_str),
            aggregate_type,
            aggregate_id,
            aggregate_version,
            occurred_at: DateTime::parse_from_rfc3339(&occurred_at_str)
                .map(|t| t.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now()),
            correlation_id,
            causation_id,
            principal: PrincipalRef {
                principal_type,
                principal_id,
            },
            scope: ScopeRef {
                scope_type,
                scope_id,
            },
            payload,
        };

        result.push((outbox, event));
    }

    tx.commit().await?;
    Ok(result)
}

pub async fn mark_outbox_published(
    pool: &SqlitePool,
    outbox_id: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        "UPDATE outbox_messages
         SET status = 'PUBLISHED', published_at = ?, lease_owner = NULL, lease_until = NULL
         WHERE id = ?",
    )
    .bind(&now)
    .bind(outbox_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn record_outbox_failure(
    pool: &SqlitePool,
    outbox_id: &str,
    error: &str,
    retry_delay_seconds: i64,
) -> Result<(), PersistenceError> {
    let available_at = (Utc::now() + Duration::seconds(retry_delay_seconds)).to_rfc3339();
    sqlx::query(
        "UPDATE outbox_messages
         SET last_error = ?, available_at = ?, lease_owner = NULL, lease_until = NULL
         WHERE id = ?",
    )
    .bind(error)
    .bind(&available_at)
    .bind(outbox_id)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn consumer_inbox_dedup(
    pool: &SqlitePool,
    consumer_name: &str,
    event_id: &str,
) -> Result<bool, PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let result = sqlx::query(
        "INSERT INTO consumer_inbox (consumer_name, event_id, processed_at)
         VALUES (?, ?, ?)
         ON CONFLICT(consumer_name, event_id) DO NOTHING",
    )
    .bind(consumer_name)
    .bind(event_id)
    .bind(&now)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceRecord {
    pub id: String,
    pub owner_user_id: String,
    pub name: String,
    pub slug: String,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceRecord {
    pub id: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkforceRecord {
    pub id: String,
    pub name: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialRefRecord {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub secret_locator: String,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderConnectionRecord {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub provider_kind: String,
    pub credential_ref_id: Option<String>,
    pub endpoint: Option<String>,
    pub status: String,
    pub health: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepartmentRecord {
    pub id: String,
    pub company_id: String,
    pub name: String,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleRecord {
    pub id: String,
    pub company_id: String,
    pub name: String,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentRecord {
    pub id: String,
    pub company_id: String,
    pub name: String,
    pub primary_department_id: String,
    pub role_id: String,
    pub model_profile_id: Option<String>,
    pub capacity: i64,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

pub async fn provision_personal_workspace(
    pool: &SqlitePool,
    user_id: &UserId,
    workspace_id: &WorkspaceId,
    email: &str,
    full_name: &str,
) -> Result<WorkspaceRecord, PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO users (id, email, full_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(email) DO NOTHING")
        .bind(&user_id.0).bind(email).bind(full_name).bind(&now).bind(&now).execute(&mut *tx).await?;
    let owner_id: String = sqlx::query("SELECT id FROM users WHERE email = ?")
        .bind(email)
        .fetch_one(&mut *tx)
        .await?
        .get(0);
    if let Some(row) = sqlx::query("SELECT id, owner_user_id, name, slug, status, row_version, created_at, updated_at FROM workspaces WHERE owner_user_id = ? AND is_personal = 1")
        .bind(&owner_id).fetch_optional(&mut *tx).await? {
        tx.commit().await?;
        return Ok(workspace_row(row));
    }
    sqlx::query("INSERT INTO workspaces (id, owner_user_id, name, slug, is_personal, status, created_at, updated_at) VALUES (?, ?, 'Personal Workspace', ?, 1, 'PROVISIONING', ?, ?)")
        .bind(&workspace_id.0).bind(&owner_id).bind(format!("personal-{}", &owner_id[..8.min(owner_id.len())])).bind(&now).bind(&now).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO workspace_memberships (workspace_id, user_id, role, created_at) VALUES (?, ?, 'OWNER', ?)")
        .bind(&workspace_id.0).bind(&owner_id).bind(&now).execute(&mut *tx).await?;
    let event_id = Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO workspace_domain_events (id, workspace_id, event_type, payload, occurred_at) VALUES (?, ?, 'WorkspaceProvisioned', '{}', ?)")
        .bind(&event_id).bind(&workspace_id.0).bind(&now).execute(&mut *tx).await?;
    sqlx::query("UPDATE workspaces SET status = 'ACTIVE', updated_at = ?, row_version = row_version + 1 WHERE id = ?")
        .bind(&now)
        .bind(&workspace_id.0)
        .execute(&mut *tx)
        .await?;
    let row =
        sqlx::query("SELECT id, owner_user_id, name, slug, status, row_version, created_at, updated_at FROM workspaces WHERE id = ?")
            .bind(&workspace_id.0)
            .fetch_one(&mut *tx)
            .await?;
    tx.commit().await?;
    Ok(workspace_row(row))
}

pub async fn get_workspace(
    pool: &SqlitePool,
    workspace_id: &WorkspaceId,
) -> Result<Option<WorkspaceRecord>, PersistenceError> {
    sqlx::query("SELECT id, owner_user_id, name, slug, status, row_version, created_at, updated_at FROM workspaces WHERE id = ?")
        .bind(&workspace_id.0)
        .fetch_optional(pool)
        .await
        .map(|row| row.map(workspace_row))
        .map_err(Into::into)
}

pub async fn list_workspaces_for_user(
    pool: &SqlitePool,
    user_id: &UserId,
) -> Result<Vec<WorkspaceRecord>, PersistenceError> {
    let rows = sqlx::query("SELECT w.id, w.owner_user_id, w.name, w.slug, w.status, w.row_version, w.created_at, w.updated_at FROM workspaces w JOIN workspace_memberships m ON m.workspace_id = w.id WHERE m.user_id = ? ORDER BY w.created_at")
        .bind(&user_id.0).fetch_all(pool).await?;
    Ok(rows.into_iter().map(workspace_row).collect())
}

pub async fn create_credential_ref(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
    name: &str,
    secret_locator: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO credential_refs (id, workspace_id, name, secret_locator, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)").bind(id).bind(&workspace.0).bind(name).bind(secret_locator).bind(&now).bind(&now).execute(pool).await?;
    Ok(())
}
pub async fn list_credential_refs(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
) -> Result<Vec<ResourceRecord>, PersistenceError> {
    list_resources(pool, "credential_refs", workspace).await
}
pub async fn create_provider_connection(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
    name: &str,
    kind: &str,
    credential: Option<&str>,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO provider_connections (id, workspace_id, credential_ref_id, name, provider_kind, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)").bind(id).bind(&workspace.0).bind(credential).bind(name).bind(kind).bind(&now).bind(&now).execute(pool).await?;
    Ok(())
}
pub async fn get_provider_connection(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
) -> Result<Option<ResourceRecord>, PersistenceError> {
    get_resource(pool, "provider_connections", workspace, id).await
}
pub async fn create_model(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
    provider: &str,
    key: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO models (id, workspace_id, provider_connection_id, model_key, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)").bind(id).bind(&workspace.0).bind(provider).bind(key).bind(&now).bind(&now).execute(pool).await?;
    Ok(())
}
pub async fn list_models(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
) -> Result<Vec<ResourceRecord>, PersistenceError> {
    list_resources(pool, "models", workspace).await
}
pub async fn create_model_profile(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
    name: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO model_profiles (id, workspace_id, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)").bind(id).bind(&workspace.0).bind(name).bind(&now).bind(&now).execute(pool).await?;
    Ok(())
}
pub async fn create_model_profile_version(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    profile: &str,
    version: i64,
    model: &str,
    config: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO model_profile_versions (profile_id, workspace_id, version, model_id, config_json, created_at) VALUES (?, ?, ?, ?, ?, ?)").bind(profile).bind(&workspace.0).bind(version).bind(model).bind(config).bind(&now).execute(&mut *tx).await?;
    sqlx::query("UPDATE model_profiles SET current_version = ?, row_version = row_version + 1, updated_at = ? WHERE id = ? AND workspace_id = ?").bind(version).bind(&now).bind(profile).bind(&workspace.0).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}
pub async fn get_model_profile(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
) -> Result<Option<ResourceRecord>, PersistenceError> {
    get_resource(pool, "model_profiles", workspace, id).await
}
pub async fn grant_model_profile(
    pool: &SqlitePool,
    company: &CompanyId,
    profile: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let result = sqlx::query("INSERT INTO company_workspace_resource_grants (company_id, workspace_id, model_profile_id, created_at) SELECT c.id, c.workspace_id, p.id, ? FROM companies c JOIN model_profiles p ON p.workspace_id = c.workspace_id WHERE c.id = ? AND p.id = ? AND p.status = 'ACTIVE'").bind(&now).bind(&company.0).bind(profile).execute(pool).await?;
    if result.rows_affected() == 0 {
        return Err(PersistenceError::NotFound(profile.into()));
    }
    Ok(())
}
pub async fn company_has_model_profile(
    pool: &SqlitePool,
    company: &CompanyId,
    profile: &str,
) -> Result<bool, PersistenceError> {
    Ok(sqlx::query("SELECT 1 FROM company_workspace_resource_grants WHERE company_id = ? AND model_profile_id = ?").bind(&company.0).bind(profile).fetch_optional(pool).await?.is_some())
}

pub async fn create_department(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
    name: &str,
) -> Result<(), PersistenceError> {
    create_workforce(pool, "departments", company, id, name).await
}
pub async fn create_role(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
    name: &str,
) -> Result<(), PersistenceError> {
    create_workforce(pool, "roles", company, id, name).await
}
pub async fn assign_department_role(
    pool: &SqlitePool,
    company: &CompanyId,
    department: &str,
    role: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO department_roles (company_id, department_id, role_id, created_at) VALUES (?, ?, ?, ?)").bind(&company.0).bind(department).bind(role).bind(&now).execute(pool).await?;
    Ok(())
}
#[allow(clippy::too_many_arguments)]
pub async fn create_agent(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
    name: &str,
    department: &str,
    role: &str,
    profile: Option<&str>,
    capacity: i64,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO agents (id, company_id, name, primary_department_id, role_id, model_profile_id, capacity, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)").bind(id).bind(&company.0).bind(name).bind(department).bind(role).bind(profile).bind(capacity).bind(&now).bind(&now).execute(pool).await?;
    Ok(())
}
pub async fn get_agent(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
) -> Result<Option<WorkforceRecord>, PersistenceError> {
    get_workforce(pool, "agents", company, id).await
}
pub async fn list_agents(
    pool: &SqlitePool,
    company: &CompanyId,
) -> Result<Vec<WorkforceRecord>, PersistenceError> {
    list_workforce(pool, "agents", company).await
}
pub async fn list_departments(
    pool: &SqlitePool,
    company: &CompanyId,
) -> Result<Vec<WorkforceRecord>, PersistenceError> {
    list_workforce(pool, "departments", company).await
}
pub async fn list_roles(
    pool: &SqlitePool,
    company: &CompanyId,
) -> Result<Vec<WorkforceRecord>, PersistenceError> {
    list_workforce(pool, "roles", company).await
}
pub async fn list_department_roles(
    pool: &SqlitePool,
    company: &CompanyId,
) -> Result<Vec<WorkforceRecord>, PersistenceError> {
    let rows = sqlx::query("SELECT department_id, role_id, 'ACTIVE' FROM department_roles WHERE company_id = ? ORDER BY department_id, role_id").bind(&company.0).fetch_all(pool).await?;
    Ok(rows.into_iter().map(workforce_row).collect())
}
pub async fn retire_agent(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
) -> Result<(), PersistenceError> {
    retire_workforce(pool, "agents", company, id).await
}
pub async fn retire_department(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
) -> Result<(), PersistenceError> {
    retire_workforce(pool, "departments", company, id).await
}
pub async fn get_credential_ref(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
) -> Result<Option<CredentialRefRecord>, PersistenceError> {
    let row = sqlx::query("SELECT id, workspace_id, name, secret_locator, status, row_version, created_at, updated_at FROM credential_refs WHERE workspace_id = ? AND id = ?")
        .bind(&workspace.0).bind(id).fetch_optional(pool).await?;
    Ok(row.map(|r| CredentialRefRecord {
        id: r.get(0),
        workspace_id: r.get(1),
        name: r.get(2),
        secret_locator: r.get(3),
        status: r.get(4),
        row_version: r.get(5),
        created_at: r.get(6),
        updated_at: r.get(7),
    }))
}

pub async fn list_credential_refs_full(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
) -> Result<Vec<CredentialRefRecord>, PersistenceError> {
    let rows = sqlx::query("SELECT id, workspace_id, name, secret_locator, status, row_version, created_at, updated_at FROM credential_refs WHERE workspace_id = ? ORDER BY created_at")
        .bind(&workspace.0).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|r| CredentialRefRecord {
            id: r.get(0),
            workspace_id: r.get(1),
            name: r.get(2),
            secret_locator: r.get(3),
            status: r.get(4),
            row_version: r.get(5),
            created_at: r.get(6),
            updated_at: r.get(7),
        })
        .collect())
}

pub async fn update_credential_ref_status(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
    new_status: &str,
    expected_version: i64,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let result = sqlx::query("UPDATE credential_refs SET status = ?, row_version = row_version + 1, updated_at = ? WHERE workspace_id = ? AND id = ? AND row_version = ?")
        .bind(new_status).bind(&now).bind(&workspace.0).bind(id).bind(expected_version).execute(pool).await?;
    if result.rows_affected() == 0 {
        let row = sqlx::query(
            "SELECT row_version FROM credential_refs WHERE workspace_id = ? AND id = ?",
        )
        .bind(&workspace.0)
        .bind(id)
        .fetch_optional(pool)
        .await?;
        if let Some(r) = row {
            let current: i64 = r.get(0);
            return Err(PersistenceError::StaleVersion {
                current,
                expected: expected_version,
            });
        }
        return Err(PersistenceError::NotFound(id.into()));
    }
    Ok(())
}

pub async fn list_provider_connections(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
) -> Result<Vec<ProviderConnectionRecord>, PersistenceError> {
    let rows = sqlx::query("SELECT id, workspace_id, name, provider_kind, credential_ref_id, endpoint, status, health, row_version, created_at, updated_at FROM provider_connections WHERE workspace_id = ? ORDER BY created_at")
        .bind(&workspace.0).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|r| ProviderConnectionRecord {
            id: r.get(0),
            workspace_id: r.get(1),
            name: r.get(2),
            provider_kind: r.get(3),
            credential_ref_id: r.get(4),
            endpoint: r.get(5),
            status: r.get(6),
            health: r.get(7),
            row_version: r.get(8),
            created_at: r.get(9),
            updated_at: r.get(10),
        })
        .collect())
}

pub async fn get_provider_connection_full(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
) -> Result<Option<ProviderConnectionRecord>, PersistenceError> {
    let row = sqlx::query("SELECT id, workspace_id, name, provider_kind, credential_ref_id, endpoint, status, health, row_version, created_at, updated_at FROM provider_connections WHERE workspace_id = ? AND id = ?")
        .bind(&workspace.0).bind(id).fetch_optional(pool).await?;
    Ok(row.map(|r| ProviderConnectionRecord {
        id: r.get(0),
        workspace_id: r.get(1),
        name: r.get(2),
        provider_kind: r.get(3),
        credential_ref_id: r.get(4),
        endpoint: r.get(5),
        status: r.get(6),
        health: r.get(7),
        row_version: r.get(8),
        created_at: r.get(9),
        updated_at: r.get(10),
    }))
}

pub async fn update_provider_status(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
    new_status: &str,
    expected_version: i64,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let result = sqlx::query("UPDATE provider_connections SET status = ?, row_version = row_version + 1, updated_at = ? WHERE workspace_id = ? AND id = ? AND row_version = ?")
        .bind(new_status).bind(&now).bind(&workspace.0).bind(id).bind(expected_version).execute(pool).await?;
    if result.rows_affected() == 0 {
        let row = sqlx::query(
            "SELECT row_version FROM provider_connections WHERE workspace_id = ? AND id = ?",
        )
        .bind(&workspace.0)
        .bind(id)
        .fetch_optional(pool)
        .await?;
        if let Some(r) = row {
            let current: i64 = r.get(0);
            return Err(PersistenceError::StaleVersion {
                current,
                expected: expected_version,
            });
        }
        return Err(PersistenceError::NotFound(id.into()));
    }
    Ok(())
}

pub async fn update_provider_health(
    pool: &SqlitePool,
    workspace: &WorkspaceId,
    id: &str,
    new_health: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query("UPDATE provider_connections SET health = ?, updated_at = ? WHERE workspace_id = ? AND id = ?")
        .bind(new_health).bind(&now).bind(&workspace.0).bind(id).execute(pool).await?;
    Ok(())
}

pub async fn get_department(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
) -> Result<Option<DepartmentRecord>, PersistenceError> {
    let row = sqlx::query("SELECT id, company_id, name, status, row_version, created_at, updated_at FROM departments WHERE company_id = ? AND id = ?")
        .bind(&company.0).bind(id).fetch_optional(pool).await?;
    Ok(row.map(|r| DepartmentRecord {
        id: r.get(0),
        company_id: r.get(1),
        name: r.get(2),
        status: r.get(3),
        row_version: r.get(4),
        created_at: r.get(5),
        updated_at: r.get(6),
    }))
}

pub async fn list_departments_full(
    pool: &SqlitePool,
    company: &CompanyId,
) -> Result<Vec<DepartmentRecord>, PersistenceError> {
    let rows = sqlx::query("SELECT id, company_id, name, status, row_version, created_at, updated_at FROM departments WHERE company_id = ? ORDER BY created_at")
        .bind(&company.0).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|r| DepartmentRecord {
            id: r.get(0),
            company_id: r.get(1),
            name: r.get(2),
            status: r.get(3),
            row_version: r.get(4),
            created_at: r.get(5),
            updated_at: r.get(6),
        })
        .collect())
}

pub async fn update_department(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
    name: &str,
    expected_version: i64,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let result = sqlx::query("UPDATE departments SET name = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?")
        .bind(name).bind(&now).bind(&company.0).bind(id).bind(expected_version).execute(pool).await?;
    if result.rows_affected() == 0 {
        let row =
            sqlx::query("SELECT row_version FROM departments WHERE company_id = ? AND id = ?")
                .bind(&company.0)
                .bind(id)
                .fetch_optional(pool)
                .await?;
        if let Some(r) = row {
            let current: i64 = r.get(0);
            return Err(PersistenceError::StaleVersion {
                current,
                expected: expected_version,
            });
        }
        return Err(PersistenceError::NotFound(id.into()));
    }
    Ok(())
}

pub async fn get_role(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
) -> Result<Option<RoleRecord>, PersistenceError> {
    let row = sqlx::query("SELECT id, company_id, name, status, row_version, created_at, updated_at FROM roles WHERE company_id = ? AND id = ?")
        .bind(&company.0).bind(id).fetch_optional(pool).await?;
    Ok(row.map(|r| RoleRecord {
        id: r.get(0),
        company_id: r.get(1),
        name: r.get(2),
        status: r.get(3),
        row_version: r.get(4),
        created_at: r.get(5),
        updated_at: r.get(6),
    }))
}

pub async fn list_roles_full(
    pool: &SqlitePool,
    company: &CompanyId,
) -> Result<Vec<RoleRecord>, PersistenceError> {
    let rows = sqlx::query("SELECT id, company_id, name, status, row_version, created_at, updated_at FROM roles WHERE company_id = ? ORDER BY created_at")
        .bind(&company.0).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|r| RoleRecord {
            id: r.get(0),
            company_id: r.get(1),
            name: r.get(2),
            status: r.get(3),
            row_version: r.get(4),
            created_at: r.get(5),
            updated_at: r.get(6),
        })
        .collect())
}

pub async fn update_role(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
    name: &str,
    expected_version: i64,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let result = sqlx::query("UPDATE roles SET name = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?")
        .bind(name).bind(&now).bind(&company.0).bind(id).bind(expected_version).execute(pool).await?;
    if result.rows_affected() == 0 {
        let row = sqlx::query("SELECT row_version FROM roles WHERE company_id = ? AND id = ?")
            .bind(&company.0)
            .bind(id)
            .fetch_optional(pool)
            .await?;
        if let Some(r) = row {
            let current: i64 = r.get(0);
            return Err(PersistenceError::StaleVersion {
                current,
                expected: expected_version,
            });
        }
        return Err(PersistenceError::NotFound(id.into()));
    }
    Ok(())
}

pub async fn get_agent_full(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
) -> Result<Option<AgentRecord>, PersistenceError> {
    let row = sqlx::query("SELECT id, company_id, name, primary_department_id, role_id, model_profile_id, capacity, status, row_version, created_at, updated_at FROM agents WHERE company_id = ? AND id = ?")
        .bind(&company.0).bind(id).fetch_optional(pool).await?;
    Ok(row.map(|r| AgentRecord {
        id: r.get(0),
        company_id: r.get(1),
        name: r.get(2),
        primary_department_id: r.get(3),
        role_id: r.get(4),
        model_profile_id: r.get(5),
        capacity: r.get(6),
        status: r.get(7),
        row_version: r.get(8),
        created_at: r.get(9),
        updated_at: r.get(10),
    }))
}

pub async fn list_agents_full(
    pool: &SqlitePool,
    company: &CompanyId,
) -> Result<Vec<AgentRecord>, PersistenceError> {
    let rows = sqlx::query("SELECT id, company_id, name, primary_department_id, role_id, model_profile_id, capacity, status, row_version, created_at, updated_at FROM agents WHERE company_id = ? ORDER BY created_at")
        .bind(&company.0).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|r| AgentRecord {
            id: r.get(0),
            company_id: r.get(1),
            name: r.get(2),
            primary_department_id: r.get(3),
            role_id: r.get(4),
            model_profile_id: r.get(5),
            capacity: r.get(6),
            status: r.get(7),
            row_version: r.get(8),
            created_at: r.get(9),
            updated_at: r.get(10),
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
pub async fn update_agent(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
    name: &str,
    primary_department_id: &str,
    role_id: &str,
    model_profile_id: Option<&str>,
    capacity: i64,
    expected_version: i64,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let result = sqlx::query("UPDATE agents SET name = ?, primary_department_id = ?, role_id = ?, model_profile_id = ?, capacity = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?")
        .bind(name).bind(primary_department_id).bind(role_id).bind(model_profile_id).bind(capacity).bind(&now).bind(&company.0).bind(id).bind(expected_version).execute(pool).await?;
    if result.rows_affected() == 0 {
        let row = sqlx::query("SELECT row_version FROM agents WHERE company_id = ? AND id = ?")
            .bind(&company.0)
            .bind(id)
            .fetch_optional(pool)
            .await?;
        if let Some(r) = row {
            let current: i64 = r.get(0);
            return Err(PersistenceError::StaleVersion {
                current,
                expected: expected_version,
            });
        }
        return Err(PersistenceError::NotFound(id.into()));
    }
    Ok(())
}

pub async fn update_agent_status(
    pool: &SqlitePool,
    company: &CompanyId,
    id: &str,
    new_status: &str,
    expected_version: i64,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let result = sqlx::query("UPDATE agents SET status = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?")
        .bind(new_status).bind(&now).bind(&company.0).bind(id).bind(expected_version).execute(pool).await?;
    if result.rows_affected() == 0 {
        let row = sqlx::query("SELECT row_version FROM agents WHERE company_id = ? AND id = ?")
            .bind(&company.0)
            .bind(id)
            .fetch_optional(pool)
            .await?;
        if let Some(r) = row {
            let current: i64 = r.get(0);
            return Err(PersistenceError::StaleVersion {
                current,
                expected: expected_version,
            });
        }
        return Err(PersistenceError::NotFound(id.into()));
    }
    Ok(())
}

fn workspace_row(row: sqlx::sqlite::SqliteRow) -> WorkspaceRecord {
    WorkspaceRecord {
        id: row.get(0),
        owner_user_id: row.get(1),
        name: row.get(2),
        slug: row.get(3),
        status: row.get(4),
        row_version: row.get(5),
        created_at: row.get(6),
        updated_at: row.get(7),
    }
}
async fn list_resources(
    pool: &SqlitePool,
    table: &str,
    workspace: &WorkspaceId,
) -> Result<Vec<ResourceRecord>, PersistenceError> {
    let sql = format!("SELECT id, status FROM {table} WHERE workspace_id = ? ORDER BY id");
    let rows = sqlx::query(&sql).bind(&workspace.0).fetch_all(pool).await?;
    Ok(rows.into_iter().map(resource_row).collect())
}
async fn get_resource(
    pool: &SqlitePool,
    table: &str,
    workspace: &WorkspaceId,
    id: &str,
) -> Result<Option<ResourceRecord>, PersistenceError> {
    let sql = format!("SELECT id, status FROM {table} WHERE workspace_id = ? AND id = ?");
    Ok(sqlx::query(&sql)
        .bind(&workspace.0)
        .bind(id)
        .fetch_optional(pool)
        .await?
        .map(resource_row))
}
fn resource_row(row: sqlx::sqlite::SqliteRow) -> ResourceRecord {
    ResourceRecord {
        id: row.get(0),
        status: row.get(1),
    }
}
async fn create_workforce(
    pool: &SqlitePool,
    table: &str,
    company: &CompanyId,
    id: &str,
    name: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let sql = format!(
        "INSERT INTO {table} (id, company_id, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?)"
    );
    sqlx::query(&sql)
        .bind(id)
        .bind(&company.0)
        .bind(name)
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await?;
    Ok(())
}
async fn list_workforce(
    pool: &SqlitePool,
    table: &str,
    company: &CompanyId,
) -> Result<Vec<WorkforceRecord>, PersistenceError> {
    let sql = format!("SELECT id, name, status FROM {table} WHERE company_id = ? ORDER BY id");
    let rows = sqlx::query(&sql).bind(&company.0).fetch_all(pool).await?;
    Ok(rows.into_iter().map(workforce_row).collect())
}
async fn get_workforce(
    pool: &SqlitePool,
    table: &str,
    company: &CompanyId,
    id: &str,
) -> Result<Option<WorkforceRecord>, PersistenceError> {
    let sql = format!("SELECT id, name, status FROM {table} WHERE company_id = ? AND id = ?");
    Ok(sqlx::query(&sql)
        .bind(&company.0)
        .bind(id)
        .fetch_optional(pool)
        .await?
        .map(workforce_row))
}
async fn retire_workforce(
    pool: &SqlitePool,
    table: &str,
    company: &CompanyId,
    id: &str,
) -> Result<(), PersistenceError> {
    let now = Utc::now().to_rfc3339();
    let sql = format!(
        "UPDATE {table} SET status = 'RETIRED', row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ?"
    );
    let result = sqlx::query(&sql)
        .bind(&now)
        .bind(&company.0)
        .bind(id)
        .execute(pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(PersistenceError::NotFound(id.into()));
    }
    Ok(())
}
fn workforce_row(row: sqlx::sqlite::SqliteRow) -> WorkforceRecord {
    WorkforceRecord {
        id: row.get(0),
        name: row.get(1),
        status: row.get(2),
    }
}

fn row_to_company(row: sqlx::sqlite::SqliteRow) -> Result<Company, PersistenceError> {
    let id: String = row.get(0);
    let workspace_id: String = row.get(1);
    let name: String = row.get(2);
    let description: Option<String> = row.get(3);
    let status_str: String = row.get(4);
    let row_version: i64 = row.get(5);
    let created_at_str: String = row.get(6);
    let updated_at_str: String = row.get(7);
    let mission: Option<String> = row.try_get("mission").unwrap_or(None);
    let director_user_id: Option<String> = row.try_get("director_user_id").unwrap_or(None);

    let status = CompanyStatus::from_str(&status_str)?;
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| DomainError::Validation(format!("Invalid created_at: {e}")))?;
    let updated_at = DateTime::parse_from_rfc3339(&updated_at_str)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| DomainError::Validation(format!("Invalid updated_at: {e}")))?;

    Ok(Company {
        id: CompanyId(id),
        workspace_id: WorkspaceId(workspace_id),
        name,
        description,
        mission,
        director_user_id: director_user_id.map(UserId),
        status,
        row_version,
        created_at,
        updated_at,
    })
}
