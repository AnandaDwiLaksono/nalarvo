use nalarvo_domain::{
    AuditRecord, AuditSink, Company, CompanyId, DomainError, DomainEvent, PrincipalRef, ScopeRef,
    UserId, WorkspaceId,
};
use nalarvo_persistence::{
    IdempotencyCheck, PersistenceError, bootstrap_personal_workspace, check_idempotency_tx,
    create_pool, fetch_pending_outbox, get_company, hash_request, insert_company_tx,
    insert_domain_event_and_outbox_tx, list_companies, mark_outbox_published, run_migrations,
    save_idempotency_record_tx, update_company_tx,
};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::broadcast;
use tracing::error;
use uuid::Uuid;

pub mod m2;
pub use nalarvo_persistence::{
    AgentRecord, CredentialRefRecord, DepartmentRecord, ProviderConnectionRecord, RoleRecord,
    WorkspaceRecord,
};
mod secret_store;
pub use secret_store::{
    CredentialRef, FakeSecretStore, SecretStore, SecretStoreError, SecretValue,
};

#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error("Domain error: {0}")]
    Domain(DomainError),

    #[error("Persistence error: {0}")]
    Persistence(PersistenceError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Unauthorized")]
    Unauthorized,

    #[error("Validation failed: {0}")]
    Validation(String),

    #[error("Secret store operation failed")]
    SecretStore(#[from] SecretStoreError),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Stale version: current {current}, expected {expected}")]
    StaleVersion { current: i64, expected: i64 },

    #[error("Idempotency key reuse mismatch: {0}")]
    IdempotencyKeyReuseMismatch(String),

    #[error("Scope violation: {0}")]
    ScopeViolation(String),
}

impl From<DomainError> for ApplicationError {
    fn from(err: DomainError) -> Self {
        match err {
            DomainError::StaleVersion { current, expected } => {
                Self::StaleVersion { current, expected }
            }
            DomainError::Validation(msg) => Self::Validation(msg),
            DomainError::NotFound(id) => Self::NotFound(id),
            DomainError::ScopeViolation(msg) => Self::ScopeViolation(msg),
            DomainError::IdempotencyKeyReuseMismatch(msg) => Self::IdempotencyKeyReuseMismatch(msg),
        }
    }
}

impl From<PersistenceError> for ApplicationError {
    fn from(err: PersistenceError) -> Self {
        match err {
            PersistenceError::Domain(d) => Self::Domain(d),
            PersistenceError::Database(e) => Self::Database(e),
            PersistenceError::StaleVersion { current, expected } => {
                Self::StaleVersion { current, expected }
            }
            PersistenceError::IdempotencyMismatch(key) => Self::IdempotencyKeyReuseMismatch(key),
            PersistenceError::NotFound(id) => Self::NotFound(id),
            other => Self::Persistence(other),
        }
    }
}

#[derive(Clone, Default)]
pub struct TracingAuditSink;

impl AuditSink for TracingAuditSink {
    fn record(&self, record: AuditRecord) {
        tracing::info!(
            audit_id = %record.audit_id,
            action = %record.action,
            principal_type = %record.principal.principal_type,
            principal_id = %record.principal.principal_id,
            scope_type = %record.scope.scope_type,
            scope_id = %record.scope.scope_id,
            details = %record.details,
            "AUDIT"
        );
    }
}

#[derive(Clone, Default)]
pub struct InMemoryAuditSink {
    records: Arc<Mutex<Vec<AuditRecord>>>,
}

impl InMemoryAuditSink {
    pub fn new() -> Self {
        Self {
            records: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn records(&self) -> Vec<AuditRecord> {
        self.records.lock().unwrap().clone()
    }
}

impl AuditSink for InMemoryAuditSink {
    fn record(&self, record: AuditRecord) {
        self.records.lock().unwrap().push(record);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCompanyCommand {
    pub workspace_id: WorkspaceId,
    pub name: String,
    pub description: Option<String>,
    pub principal: Option<PrincipalRef>,
    pub idempotency_key: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateCompanyMetadataCommand {
    pub workspace_id: WorkspaceId,
    pub company_id: CompanyId,
    pub name: String,
    pub description: Option<String>,
    pub expected_version: i64,
    pub principal: Option<PrincipalRef>,
    pub idempotency_key: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

#[derive(Clone)]
pub struct ApplicationContext {
    pub pool: SqlitePool,
    pub event_broadcaster: broadcast::Sender<DomainEvent>,
    pub audit_sink: Arc<dyn AuditSink>,
    secret_store: Arc<dyn SecretStore>,
}

impl ApplicationContext {
    pub fn new(pool: SqlitePool) -> Self {
        Self::with_ports(
            pool,
            Arc::new(TracingAuditSink),
            Arc::new(FakeSecretStore::default()),
        )
    }

    pub fn with_audit_sink(pool: SqlitePool, audit_sink: Arc<dyn AuditSink>) -> Self {
        Self::with_ports(pool, audit_sink, Arc::new(FakeSecretStore::default()))
    }

    pub fn with_secret_store(mut self, secret_store: Arc<dyn SecretStore>) -> Self {
        self.secret_store = secret_store;
        self
    }

    pub fn with_ports(
        pool: SqlitePool,
        audit_sink: Arc<dyn AuditSink>,
        secret_store: Arc<dyn SecretStore>,
    ) -> Self {
        let (event_broadcaster, _) = broadcast::channel(1024);
        Self {
            pool,
            event_broadcaster,
            audit_sink,
            secret_store,
        }
    }

    pub async fn init(database_url: &str) -> Result<Self, ApplicationError> {
        Self::init_with_secret_store(database_url, Arc::new(FakeSecretStore::default())).await
    }

    pub async fn init_with_secret_store(
        database_url: &str,
        secret_store: Arc<dyn SecretStore>,
    ) -> Result<Self, ApplicationError> {
        let pool = create_pool(database_url).await?;
        run_migrations(&pool).await?;

        // Bootstrap deterministic personal workspace
        let default_user = UserId("0191e4b8-0001-7000-8000-000000000001".into());
        let default_workspace = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
        bootstrap_personal_workspace(
            &pool,
            &default_user,
            &default_workspace,
            "local@nalarvo.local",
            "Nalarvo Owner",
        )
        .await?;

        Ok(Self::with_ports(
            pool,
            Arc::new(TracingAuditSink),
            secret_store,
        ))
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<DomainEvent> {
        self.event_broadcaster.subscribe()
    }

    pub async fn create_company(
        &self,
        cmd: CreateCompanyCommand,
    ) -> Result<Company, ApplicationError> {
        let principal = cmd
            .principal
            .clone()
            .unwrap_or_else(|| PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001"));

        let name = cmd.name.trim().to_string();
        if name.is_empty() {
            return Err(ApplicationError::Validation(
                "Company name cannot be empty".into(),
            ));
        }

        let correlation_id = cmd
            .correlation_id
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        let causation_id = cmd
            .causation_id
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| correlation_id.clone());

        let req_bytes =
            serde_json::to_vec(&cmd).map_err(|e| ApplicationError::Validation(e.to_string()))?;
        let req_hash = hash_request(&req_bytes);

        let idempotency_scope = format!(
            "workspace:{}:{}:CreateCompany",
            cmd.workspace_id.0, principal.principal_id
        );

        let mut tx = self.pool.begin().await?;

        if let Some(ref key) = cmd.idempotency_key {
            let check = check_idempotency_tx(&mut tx, &idempotency_scope, key, &req_hash).await?;

            if let IdempotencyCheck::Cached(body) = check {
                let company: Company = serde_json::from_str(&body)
                    .map_err(|e| ApplicationError::Validation(e.to_string()))?;
                return Ok(company);
            }
        }

        let company = Company::create(cmd.workspace_id.clone(), name, cmd.description)?;
        let event = DomainEvent::company_created(
            &company,
            principal.clone(),
            correlation_id.clone(),
            causation_id.clone(),
        );

        insert_company_tx(&mut tx, &company).await?;
        insert_domain_event_and_outbox_tx(&mut tx, &event).await?;

        if let Some(ref key) = cmd.idempotency_key {
            let body = serde_json::to_string(&company)
                .map_err(|e| ApplicationError::Validation(e.to_string()))?;
            save_idempotency_record_tx(&mut tx, &idempotency_scope, key, &req_hash, &body).await?;
        }

        tx.commit().await?;

        // Structured safe audit sink record
        self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "CompanyCreated".into(),
            principal: principal.clone(),
            scope: ScopeRef::workspace(cmd.workspace_id.0.clone()),
            occurred_at: chrono::Utc::now(),
            details: serde_json::json!({
                "company_id": company.id.0,
                "name": company.name,
                "correlation_id": correlation_id,
            }),
        });

        Ok(company)
    }

    pub async fn update_company_metadata(
        &self,
        cmd: UpdateCompanyMetadataCommand,
    ) -> Result<Company, ApplicationError> {
        let principal = cmd
            .principal
            .clone()
            .unwrap_or_else(|| PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001"));

        let correlation_id = cmd
            .correlation_id
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        let causation_id = cmd
            .causation_id
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| correlation_id.clone());

        let req_bytes =
            serde_json::to_vec(&cmd).map_err(|e| ApplicationError::Validation(e.to_string()))?;
        let req_hash = hash_request(&req_bytes);

        let idempotency_scope = format!(
            "company:{}:{}:UpdateCompanyMetadata",
            cmd.company_id.0, principal.principal_id
        );

        let mut tx = self.pool.begin().await?;

        if let Some(ref key) = cmd.idempotency_key {
            let check = check_idempotency_tx(&mut tx, &idempotency_scope, key, &req_hash).await?;

            if let IdempotencyCheck::Cached(body) = check {
                let company: Company = serde_json::from_str(&body)
                    .map_err(|e| ApplicationError::Validation(e.to_string()))?;
                return Ok(company);
            }
        }

        let existing = get_company(&self.pool, &cmd.workspace_id, &cmd.company_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(cmd.company_id.0.clone()))?;

        if existing.row_version != cmd.expected_version {
            self.audit_sink.record(AuditRecord {
                audit_id: Uuid::now_v7().to_string(),
                action: "StaleVersionRejected".into(),
                principal: principal.clone(),
                scope: ScopeRef::company(cmd.company_id.0.clone()),
                occurred_at: chrono::Utc::now(),
                details: serde_json::json!({
                    "company_id": cmd.company_id.0,
                    "expected_version": cmd.expected_version,
                    "current_version": existing.row_version,
                }),
            });
            return Err(ApplicationError::StaleVersion {
                current: existing.row_version,
                expected: cmd.expected_version,
            });
        }

        let mut updated = existing;
        updated.update_metadata(cmd.name, cmd.description, cmd.expected_version)?;

        let payload = serde_json::json!({
            "company_id": updated.id.0,
            "workspace_id": updated.workspace_id.0,
            "name": updated.name,
            "description": updated.description,
            "row_version": updated.row_version,
        });

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "CompanyMetadataUpdated".into(),
            schema_version: 1,
            company_id: updated.id.clone(),
            aggregate_type: "Company".into(),
            aggregate_id: updated.id.0.clone(),
            aggregate_version: updated.row_version,
            occurred_at: updated.updated_at,
            correlation_id: correlation_id.clone(),
            causation_id: causation_id.clone(),
            principal: principal.clone(),
            scope: ScopeRef::company(updated.id.0.clone()),
            payload,
        };

        update_company_tx(&mut tx, &updated, cmd.expected_version).await?;
        insert_domain_event_and_outbox_tx(&mut tx, &event).await?;

        if let Some(ref key) = cmd.idempotency_key {
            let body = serde_json::to_string(&updated)
                .map_err(|e| ApplicationError::Validation(e.to_string()))?;
            save_idempotency_record_tx(&mut tx, &idempotency_scope, key, &req_hash, &body).await?;
        }

        tx.commit().await?;

        self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "CompanyMetadataUpdated".into(),
            principal: principal.clone(),
            scope: ScopeRef::company(cmd.company_id.0.clone()),
            occurred_at: chrono::Utc::now(),
            details: serde_json::json!({
                "company_id": updated.id.0,
                "name": updated.name,
                "new_version": updated.row_version,
                "correlation_id": correlation_id,
            }),
        });

        Ok(updated)
    }

    pub async fn get_company(
        &self,
        workspace_id: &WorkspaceId,
        company_id: &CompanyId,
    ) -> Result<Company, ApplicationError> {
        get_company(&self.pool, workspace_id, company_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(company_id.0.clone()))
    }

    pub async fn list_companies(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<Company>, ApplicationError> {
        Ok(list_companies(&self.pool, workspace_id).await?)
    }

    pub async fn dispatch_outbox_batch(
        &self,
        limit: i64,
        lease_owner: &str,
        lease_duration_secs: i64,
    ) -> Result<usize, ApplicationError> {
        let pending =
            fetch_pending_outbox(&self.pool, limit, lease_owner, lease_duration_secs).await?;

        let mut dispatched = 0;
        for (outbox, event) in pending {
            // Send to event broadcast channel for SSE observers (zero receivers is non-failing)
            let _ = self.event_broadcaster.send(event);
            mark_outbox_published(&self.pool, &outbox.id).await?;
            dispatched += 1;
        }

        Ok(dispatched)
    }
}

pub fn start_outbox_dispatcher(
    app_ctx: ApplicationContext,
    interval_ms: u64,
    worker_id: String,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(interval_ms));
        loop {
            interval.tick().await;
            if let Err(e) = app_ctx.dispatch_outbox_batch(50, &worker_id, 30).await {
                error!(error = %e, "outbox dispatcher batch failed");
            }
        }
    })
}
