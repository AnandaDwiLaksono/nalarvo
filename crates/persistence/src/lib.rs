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

    sqlx::query(
        "INSERT INTO users (id, email, full_name, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(id) DO NOTHING",
    )
    .bind(&user_id.0)
    .bind(email)
    .bind(full_name)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO workspaces (id, owner_user_id, name, slug, is_personal, created_at, updated_at)
         VALUES (?, ?, ?, ?, 1, ?, ?)
         ON CONFLICT(id) DO NOTHING",
    )
    .bind(&workspace_id.0)
    .bind(&user_id.0)
    .bind("Personal Workspace")
    .bind("personal")
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO workspace_memberships (workspace_id, user_id, role, created_at)
         VALUES (?, ?, 'OWNER', ?)
         ON CONFLICT(workspace_id, user_id) DO NOTHING",
    )
    .bind(&workspace_id.0)
    .bind(&user_id.0)
    .bind(&now)
    .execute(pool)
    .await?;

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
        "INSERT INTO companies (id, workspace_id, name, description, status, row_version, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&company.id.0)
    .bind(&company.workspace_id.0)
    .bind(&company.name)
    .bind(&company.description)
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
        "SELECT id, workspace_id, name, description, status, row_version, created_at, updated_at
         FROM companies
         WHERE id = ? AND workspace_id = ?",
    )
    .bind(&company_id.0)
    .bind(&workspace_id.0)
    .fetch_optional(pool)
    .await?;

    row.map(row_to_company).transpose()
}

pub async fn list_companies(
    pool: &SqlitePool,
    workspace_id: &WorkspaceId,
) -> Result<Vec<Company>, PersistenceError> {
    let rows = sqlx::query(
        "SELECT id, workspace_id, name, description, status, row_version, created_at, updated_at
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
         SET name = ?, description = ?, status = ?, row_version = row_version + 1, updated_at = ?
         WHERE id = ? AND workspace_id = ? AND row_version = ?",
    )
    .bind(&company.name)
    .bind(&company.description)
    .bind(company.status.to_string())
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

fn row_to_company(row: sqlx::sqlite::SqliteRow) -> Result<Company, PersistenceError> {
    let id: String = row.get(0);
    let workspace_id: String = row.get(1);
    let name: String = row.get(2);
    let description: Option<String> = row.get(3);
    let status_str: String = row.get(4);
    let row_version: i64 = row.get(5);
    let created_at_str: String = row.get(6);
    let updated_at_str: String = row.get(7);

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
        status,
        row_version,
        created_at,
        updated_at,
    })
}
