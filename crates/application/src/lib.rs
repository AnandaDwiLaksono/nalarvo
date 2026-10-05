use chrono::Utc;
use nalarvo_domain::{
    AgentAllocation, AgentAllocationStatus, AssignmentStatus, AuditRecord, AuditSink, Company,
    CompanyId, DependencyType, DomainError, DomainEvent, Objective, ObjectiveStatus, PrincipalRef,
    Project, ScopeRef, StaffingRequirement, StaffingRequirementStatus, Team, TeamStatus, UserId,
    WorkAssignment, WorkDependency, WorkItem, WorkItemStatus, WorkspaceId,
};
use nalarvo_persistence::{
    self as persistence, IdempotencyCheck, PersistenceError, bootstrap_personal_workspace,
    check_idempotency_tx, create_pool, fetch_pending_outbox, get_company, get_project,
    get_work_item, hash_request, insert_agent_allocation_tx, insert_company_tx,
    insert_domain_event_and_outbox_tx, insert_objective_tx, insert_project_tx,
    insert_staffing_requirement_tx, insert_team_tx, insert_work_assignment_tx,
    insert_work_dependency_tx, insert_work_item_tx, list_agent_allocations, list_companies,
    list_objectives, list_staffing_requirements, list_teams, list_work_assignments,
    list_work_dependencies, list_work_items, mark_outbox_published, run_migrations,
    save_idempotency_record_tx, update_agent_allocation_status_tx, update_company_tx,
    update_objective_status_tx, update_staffing_requirement_status_tx, update_team_status_tx,
    update_work_assignment_status_tx, update_work_item_status_tx,
};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::broadcast;
use tracing::error;
use uuid::Uuid;

pub mod m2;
pub mod m4;
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

// M3 command envelope: the stored request hash covers all fields except generated event IDs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CommandMeta {
    pub principal: Option<PrincipalRef>,
    pub idempotency_key: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

macro_rules! m3_command {
    ($name:ident { $($field:ident: $ty:ty = $default:expr),* $(,)? }) => {
        #[derive(Debug, Clone, Serialize, Deserialize)]
        pub struct $name {
            pub company_id: CompanyId,
            pub project_id: String,
            $(pub $field: $ty,)*
            #[serde(default)]
            pub meta: CommandMeta,
        }
        impl $name {
            pub fn idempotency(mut self, key: impl Into<String>) -> Self {
                self.meta.idempotency_key = Some(key.into());
                self
            }
        }
    };
}
m3_command!(CreateObjectiveCommand {
    title: String = String::new(), description: Option<String> = None,
    parent_objective_id: Option<String> = None, is_primary: bool = false, is_required: bool = true
});
m3_command!(CreateTeamCommand {
    name: String = String::new(),
    is_primary: bool = false
});
m3_command!(CreateStaffingRequirementCommand {
    team_id: Option<String> = None, role_id: String = String::new(),
    department_id: Option<String> = None, desired_count: u32 = 1,
    required_capability_ids: Vec<String> = Vec::new()
});
m3_command!(CreateAgentAllocationCommand {
    team_id: String = String::new(), agent_id: String = String::new(),
    staffing_requirement_id: Option<String> = None
});
m3_command!(CreateWorkItemCommand {
    title: String = String::new(), description: Option<String> = None,
    objective_id: Option<String> = None, parent_work_item_id: Option<String> = None,
    work_type: nalarvo_domain::WorkItemType = nalarvo_domain::WorkItemType::Task
});
m3_command!(CreateWorkDependencyCommand {
    work_item_id: String = String::new(),
    depends_on_work_item_id: String = String::new(),
    dependency_type: DependencyType = DependencyType::Hard
});
m3_command!(CreateWorkAssignmentCommand {
    work_item_id: String = String::new(),
    agent_id: String = String::new(),
    allocation_id: String = String::new(),
    is_primary: bool = true
});
impl CreateObjectiveCommand {
    pub fn new(company_id: CompanyId, project_id: String, title: impl Into<String>) -> Self {
        Self {
            company_id,
            project_id,
            title: title.into(),
            description: None,
            parent_objective_id: None,
            is_primary: false,
            is_required: true,
            meta: CommandMeta::default(),
        }
    }
}
impl CreateTeamCommand {
    pub fn new(company_id: CompanyId, project_id: String, name: impl Into<String>) -> Self {
        Self {
            company_id,
            project_id,
            name: name.into(),
            is_primary: false,
            meta: CommandMeta::default(),
        }
    }
}
impl CreateStaffingRequirementCommand {
    pub fn new(
        company_id: CompanyId,
        project_id: String,
        team_id: Option<String>,
        role_id: impl Into<String>,
        desired_count: u32,
    ) -> Self {
        Self {
            company_id,
            project_id,
            team_id,
            role_id: role_id.into(),
            department_id: None,
            desired_count,
            required_capability_ids: Vec::new(),
            meta: CommandMeta::default(),
        }
    }
}
impl CreateAgentAllocationCommand {
    pub fn new(
        company_id: CompanyId,
        project_id: String,
        team_id: String,
        agent_id: impl Into<String>,
        staffing_requirement_id: Option<String>,
    ) -> Self {
        Self {
            company_id,
            project_id,
            team_id,
            agent_id: agent_id.into(),
            staffing_requirement_id,
            meta: CommandMeta::default(),
        }
    }
}
impl CreateWorkItemCommand {
    pub fn new(company_id: CompanyId, project_id: String, title: impl Into<String>) -> Self {
        Self {
            company_id,
            project_id,
            title: title.into(),
            description: None,
            objective_id: None,
            parent_work_item_id: None,
            work_type: nalarvo_domain::WorkItemType::Task,
            meta: CommandMeta::default(),
        }
    }
}
impl CreateWorkDependencyCommand {
    pub fn new(
        company_id: CompanyId,
        project_id: String,
        work_item_id: String,
        depends_on_work_item_id: String,
        dependency_type: DependencyType,
    ) -> Self {
        Self {
            company_id,
            project_id,
            work_item_id,
            depends_on_work_item_id,
            dependency_type,
            meta: CommandMeta::default(),
        }
    }
}
impl CreateWorkAssignmentCommand {
    pub fn new(
        company_id: CompanyId,
        project_id: String,
        work_item_id: String,
        agent_id: impl Into<String>,
        allocation_id: String,
        is_primary: bool,
    ) -> Self {
        Self {
            company_id,
            project_id,
            work_item_id,
            agent_id: agent_id.into(),
            allocation_id,
            is_primary,
            meta: CommandMeta::default(),
        }
    }
}

#[derive(Clone)]
pub struct ApplicationContext {
    pub pool: SqlitePool,
    pub event_broadcaster: broadcast::Sender<DomainEvent>,
    pub audit_sink: Arc<dyn AuditSink>,
    secret_store: Arc<dyn SecretStore>,
}

macro_rules! m3_create {
    ($self:expr, $company_id:expr, $project_id:expr, $meta:expr, $cmd:expr, $action:expr, $entity_type:expr, $entity_id:expr, $entity:expr, |$tx:ident| $insert:expr) => {{
        let principal = $meta
            .principal
            .clone()
            .unwrap_or_else(|| PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001"));
        let correlation_id = $meta
            .correlation_id
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::now_v7().to_string());
        let causation_id = $meta
            .causation_id
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| correlation_id.clone());

        let req_bytes = serde_json::to_vec(&$cmd)
            .map_err(|e| ApplicationError::Validation(e.to_string()))?;
        let req_hash = hash_request(&req_bytes);
        let idempotency_scope = format!(
            "company:{}:{}:{}:{}",
            $company_id.0, $project_id, principal.principal_id, $action
        );

        let mut tx = $self.pool.begin().await?;

        if let Some(ref key) = $meta.idempotency_key {
            let check = check_idempotency_tx(&mut tx, &idempotency_scope, key, &req_hash).await?;
            if let IdempotencyCheck::Cached(body) = check {
                let cached = serde_json::from_str(&body)
                    .map_err(|e| ApplicationError::Validation(e.to_string()))?;
                return Ok(cached);
            }
        }

        let $tx = &mut tx;
        $insert.await?;

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: format!("{}Created", $entity_type),
            schema_version: 1,
            company_id: $company_id.clone(),
            aggregate_type: $entity_type.into(),
            aggregate_id: $entity_id.to_string(),
            aggregate_version: 1,
            occurred_at: Utc::now(),
            correlation_id: correlation_id.clone(),
            causation_id: causation_id.clone(),
            principal: principal.clone(),
            scope: ScopeRef::company(&$company_id.0),
            payload: serde_json::to_value(&$entity)
                .map_err(|e| ApplicationError::Validation(e.to_string()))?,
        };
        insert_domain_event_and_outbox_tx(&mut tx, &event).await?;

        if let Some(ref key) = $meta.idempotency_key {
            let body = serde_json::to_string(&$entity)
                .map_err(|e| ApplicationError::Validation(e.to_string()))?;
            save_idempotency_record_tx(&mut tx, &idempotency_scope, key, &req_hash, &body).await?;
        }

        tx.commit().await?;

        $self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: format!("{}Created", $entity_type),
            principal,
            scope: ScopeRef::company(&$company_id.0),
            occurred_at: Utc::now(),
            details: serde_json::json!({
                "company_id": $company_id.0,
                "project_id": $project_id,
                "id": $entity_id,
                "correlation_id": correlation_id,
            }),
        });

        Ok::<_, ApplicationError>($entity)
    }};
}

fn dependency_id(company: &CompanyId, project: &str, work_item: &str, depends_on: &str) -> String {
    hash_request(format!("{}:{project}:{work_item}:{depends_on}", company.0).as_bytes())
}

macro_rules! m3_transition {
    ($self:expr, $company:expr, $project:expr, $entity_type:expr, $entity_id:expr, $new_version:expr, $status_str:expr, |$tx:ident| $update:expr) => {{
        let principal = PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001");
        let correlation_id = Uuid::now_v7().to_string();
        let causation_id = correlation_id.clone();

        let mut tx = $self.pool.begin().await?;
        let $tx = &mut tx;
        $update.await?;

        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: format!("{}StatusUpdated", $entity_type),
            schema_version: 1,
            company_id: $company.clone(),
            aggregate_type: $entity_type.into(),
            aggregate_id: $entity_id.to_string(),
            aggregate_version: $new_version,
            occurred_at: Utc::now(),
            correlation_id: correlation_id.clone(),
            causation_id: causation_id.clone(),
            principal: principal.clone(),
            scope: ScopeRef::company(&$company.0),
            payload: serde_json::json!({
                "id": $entity_id,
                "status": $status_str,
                "row_version": $new_version,
            }),
        };
        insert_domain_event_and_outbox_tx(&mut tx, &event).await?;

        tx.commit().await?;

        $self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: format!("{}StatusUpdated", $entity_type),
            principal,
            scope: ScopeRef::company(&$company.0),
            occurred_at: Utc::now(),
            details: serde_json::json!({
                "company_id": $company.0,
                "project_id": $project,
                "id": $entity_id,
                "status": $status_str,
                "new_version": $new_version,
                "correlation_id": correlation_id,
            }),
        });

        Ok::<(), ApplicationError>(())
    }};
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

    pub async fn create_project(
        &self,
        company_id: &CompanyId,
        name: String,
        description: Option<String>,
    ) -> Result<Project, ApplicationError> {
        let project = Project::create(company_id.clone(), name, description)?;
        let mut tx = self.pool.begin().await?;
        insert_project_tx(&mut tx, &project).await?;
        tx.commit().await?;
        Ok(project)
    }

    pub async fn get_project(
        &self,
        company_id: &CompanyId,
        project_id: &str,
    ) -> Result<Project, ApplicationError> {
        get_project(&self.pool, company_id, project_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(project_id.to_string()))
    }

    pub async fn list_projects(
        &self,
        company_id: &CompanyId,
    ) -> Result<Vec<Project>, ApplicationError> {
        let rows = sqlx::query("SELECT id, company_id, name, description, priority, owner_user_id, target_outcome, target_date, working_root_path, working_root_bound_at, status, row_version, created_at, updated_at FROM projects WHERE company_id = ? ORDER BY created_at ASC")
            .bind(&company_id.0)
            .fetch_all(&self.pool)
            .await?;

        let mut projects = Vec::new();
        for r in rows {
            use sqlx::Row;
            let id: String = r.get(0);
            let cid: String = r.get(1);
            let name: String = r.get(2);
            let description: Option<String> = r.get(3);
            let priority_str: String = r.get(4);
            let owner_user_id: Option<String> = r.get(5);
            let target_outcome: Option<String> = r.get(6);
            let target_date: Option<String> = r.get(7);
            let working_root_path: Option<String> = r.get(8);
            let bound_at_str: Option<String> = r.get(9);
            let status_str: String = r.get(10);
            let row_version: i64 = r.get(11);
            let created_at_str: String = r.get(12);
            let updated_at_str: String = r.get(13);

            let priority = priority_str.parse()?;
            let status = status_str.parse()?;
            let bound_at = bound_at_str.map(|s| {
                chrono::DateTime::parse_from_rfc3339(&s)
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            });
            let created_at = chrono::DateTime::parse_from_rfc3339(&created_at_str)
                .unwrap()
                .with_timezone(&chrono::Utc);
            let updated_at = chrono::DateTime::parse_from_rfc3339(&updated_at_str)
                .unwrap()
                .with_timezone(&chrono::Utc);

            projects.push(Project {
                id,
                company_id: CompanyId(cid),
                name,
                description,
                priority,
                owner_user_id: owner_user_id.map(UserId),
                target_outcome,
                target_date,
                working_root_path,
                working_root_bound_at: bound_at,
                status,
                row_version,
                created_at,
                updated_at,
            });
        }
        Ok(projects)
    }

    pub async fn bind_project_working_root(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        path: String,
        expected_version: i64,
    ) -> Result<Project, ApplicationError> {
        let mut proj = self.get_project(company_id, project_id).await?;
        if proj.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        proj.bind_working_root(path, expected_version)?;
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "UPDATE projects SET working_root_path = ?, working_root_bound_at = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?",
        )
        .bind(&proj.working_root_path)
        .bind(proj.working_root_bound_at.map(|t| t.to_rfc3339()))
        .bind(proj.updated_at.to_rfc3339())
        .bind(&company_id.0)
        .bind(project_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() == 0 {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        tx.commit().await?;
        Ok(proj)
    }

    pub async fn unbind_project_working_root(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        expected_version: i64,
    ) -> Result<Project, ApplicationError> {
        let mut proj = self.get_project(company_id, project_id).await?;
        if proj.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        proj.unbind_working_root(expected_version)?;
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "UPDATE projects SET working_root_path = NULL, working_root_bound_at = NULL, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?",
        )
        .bind(proj.updated_at.to_rfc3339())
        .bind(&company_id.0)
        .bind(project_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() == 0 {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        tx.commit().await?;
        Ok(proj)
    }

    pub async fn start_project_staffing(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        expected_version: i64,
    ) -> Result<Project, ApplicationError> {
        let mut proj = self.get_project(company_id, project_id).await?;
        if proj.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        proj.start_staffing(expected_version)?;
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "UPDATE projects SET status = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?",
        )
        .bind(proj.status.to_string())
        .bind(proj.updated_at.to_rfc3339())
        .bind(&company_id.0)
        .bind(project_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() == 0 {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        tx.commit().await?;
        Ok(proj)
    }

    pub async fn activate_project(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        expected_version: i64,
    ) -> Result<Project, ApplicationError> {
        let mut proj = self.get_project(company_id, project_id).await?;
        if proj.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        proj.activate(expected_version)?;
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "UPDATE projects SET status = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?",
        )
        .bind(proj.status.to_string())
        .bind(proj.updated_at.to_rfc3339())
        .bind(&company_id.0)
        .bind(project_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() == 0 {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        tx.commit().await?;
        Ok(proj)
    }

    pub async fn pause_project(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        expected_version: i64,
    ) -> Result<Project, ApplicationError> {
        let mut proj = self.get_project(company_id, project_id).await?;
        if proj.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        proj.pause(expected_version)?;
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "UPDATE projects SET status = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?",
        )
        .bind(proj.status.to_string())
        .bind(proj.updated_at.to_rfc3339())
        .bind(&company_id.0)
        .bind(project_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() == 0 {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        tx.commit().await?;
        Ok(proj)
    }

    pub async fn resume_project(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        expected_version: i64,
    ) -> Result<Project, ApplicationError> {
        let mut proj = self.get_project(company_id, project_id).await?;
        if proj.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        proj.resume(expected_version)?;
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "UPDATE projects SET status = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?",
        )
        .bind(proj.status.to_string())
        .bind(proj.updated_at.to_rfc3339())
        .bind(&company_id.0)
        .bind(project_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() == 0 {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        tx.commit().await?;
        Ok(proj)
    }

    pub async fn complete_project(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        expected_version: i64,
    ) -> Result<Project, ApplicationError> {
        let mut proj = self.get_project(company_id, project_id).await?;
        if proj.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        proj.complete(expected_version)?;
        let mut tx = self.pool.begin().await?;
        let now_str = proj.updated_at.to_rfc3339();

        // 1. Release active allocations
        sqlx::query(
            "UPDATE agent_allocations SET status = 'RELEASED', row_version = row_version + 1, ended_at = ?, updated_at = ? WHERE company_id = ? AND project_id = ? AND status IN ('PLANNED', 'ACTIVE', 'PAUSED')",
        )
        .bind(&now_str)
        .bind(&now_str)
        .bind(&company_id.0)
        .bind(project_id)
        .execute(&mut *tx)
        .await?;

        // 2. Disband active teams
        sqlx::query(
            "UPDATE teams SET status = 'DISBANDED', row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND project_id = ? AND status IN ('FORMING', 'ACTIVE', 'PAUSED')",
        )
        .bind(&now_str)
        .bind(&company_id.0)
        .bind(project_id)
        .execute(&mut *tx)
        .await?;

        // 3. Release active assignments
        sqlx::query(
            "UPDATE assignments SET status = 'RELEASED', row_version = row_version + 1, ended_at = ? WHERE company_id = ? AND project_id = ? AND status = 'ACTIVE'",
        )
        .bind(&now_str)
        .bind(&company_id.0)
        .bind(project_id)
        .execute(&mut *tx)
        .await?;

        // 4. Update project status
        let res = sqlx::query(
            "UPDATE projects SET status = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?",
        )
        .bind(proj.status.to_string())
        .bind(&now_str)
        .bind(&company_id.0)
        .bind(project_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() == 0 {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        tx.commit().await?;
        Ok(proj)
    }

    pub async fn cancel_project(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        expected_version: i64,
    ) -> Result<Project, ApplicationError> {
        let mut proj = self.get_project(company_id, project_id).await?;
        if proj.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        proj.cancel(expected_version)?;
        let mut tx = self.pool.begin().await?;
        let now_str = proj.updated_at.to_rfc3339();

        // 1. Cancel nonterminal work items (completed work items remain COMPLETED)
        sqlx::query(
            "UPDATE work_items SET status = 'CANCELLED', row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND project_id = ? AND status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')",
        )
        .bind(&now_str)
        .bind(&company_id.0)
        .bind(project_id)
        .execute(&mut *tx)
        .await?;

        // 2. Release active allocations
        sqlx::query(
            "UPDATE agent_allocations SET status = 'RELEASED', row_version = row_version + 1, ended_at = ?, updated_at = ? WHERE company_id = ? AND project_id = ? AND status IN ('PLANNED', 'ACTIVE', 'PAUSED')",
        )
        .bind(&now_str)
        .bind(&now_str)
        .bind(&company_id.0)
        .bind(project_id)
        .execute(&mut *tx)
        .await?;

        // 3. Disband active teams
        sqlx::query(
            "UPDATE teams SET status = 'DISBANDED', row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND project_id = ? AND status IN ('FORMING', 'ACTIVE', 'PAUSED')",
        )
        .bind(&now_str)
        .bind(&company_id.0)
        .bind(project_id)
        .execute(&mut *tx)
        .await?;

        // 4. Release active assignments
        sqlx::query(
            "UPDATE assignments SET status = 'RELEASED', row_version = row_version + 1, ended_at = ? WHERE company_id = ? AND project_id = ? AND status = 'ACTIVE'",
        )
        .bind(&now_str)
        .bind(&company_id.0)
        .bind(project_id)
        .execute(&mut *tx)
        .await?;

        // 5. Update project status
        let res = sqlx::query(
            "UPDATE projects SET status = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?",
        )
        .bind(proj.status.to_string())
        .bind(&now_str)
        .bind(&company_id.0)
        .bind(project_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() == 0 {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        tx.commit().await?;
        Ok(proj)
    }

    pub async fn archive_project(
        &self,
        company_id: &CompanyId,
        project_id: &str,
        expected_version: i64,
    ) -> Result<Project, ApplicationError> {
        let mut proj = self.get_project(company_id, project_id).await?;
        if proj.row_version != expected_version {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        proj.archive(expected_version)?;
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "UPDATE projects SET status = ?, row_version = row_version + 1, updated_at = ? WHERE company_id = ? AND id = ? AND row_version = ?",
        )
        .bind(proj.status.to_string())
        .bind(proj.updated_at.to_rfc3339())
        .bind(&company_id.0)
        .bind(project_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;
        if res.rows_affected() == 0 {
            return Err(ApplicationError::StaleVersion {
                current: proj.row_version,
                expected: expected_version,
            });
        }
        tx.commit().await?;
        Ok(proj)
    }

    pub async fn create_objective(
        &self,
        cmd: CreateObjectiveCommand,
    ) -> Result<Objective, ApplicationError> {
        let project = self.get_project(&cmd.company_id, &cmd.project_id).await?;
        if let Some(parent) = &cmd.parent_objective_id {
            persistence::get_objective(&self.pool, &cmd.company_id, parent)
                .await?
                .filter(|o| o.project_id == project.id)
                .ok_or_else(|| ApplicationError::NotFound(parent.clone()))?;
        }
        let title = cmd.title.trim().to_string();
        if title.is_empty() {
            return Err(ApplicationError::Validation(
                "Objective title cannot be empty".into(),
            ));
        }
        let now = Utc::now();
        let obj = Objective {
            id: Uuid::now_v7().to_string(),
            company_id: cmd.company_id.clone(),
            project_id: cmd.project_id.clone(),
            parent_objective_id: cmd.parent_objective_id.clone(),
            title,
            description: cmd.description.clone(),
            is_primary: cmd.is_primary,
            is_required: cmd.is_required,
            status: ObjectiveStatus::Draft,
            row_version: 1,
            created_at: now,
            updated_at: now,
        };
        m3_create!(
            self,
            cmd.company_id,
            cmd.project_id,
            cmd.meta,
            cmd,
            "CreateObjective",
            "Objective",
            obj.id,
            obj,
            |tx| insert_objective_tx(tx, &obj)
        )
    }

    pub async fn list_objectives(
        &self,
        company: &CompanyId,
        project: &str,
    ) -> Result<Vec<Objective>, ApplicationError> {
        self.get_project(company, project).await?;
        Ok(list_objectives(&self.pool, company, project).await?)
    }

    pub async fn get_objective(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
    ) -> Result<Objective, ApplicationError> {
        self.get_project(company, project).await?;
        persistence::get_objective(&self.pool, company, id)
            .await?
            .filter(|v| v.project_id == project)
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn get_team(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
    ) -> Result<Team, ApplicationError> {
        self.get_project(company, project).await?;
        persistence::get_team(&self.pool, company, id)
            .await?
            .filter(|v| v.project_id == project)
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn create_team(&self, cmd: CreateTeamCommand) -> Result<Team, ApplicationError> {
        self.get_project(&cmd.company_id, &cmd.project_id).await?;
        let name = cmd.name.trim().to_string();
        if name.is_empty() {
            return Err(ApplicationError::Validation(
                "Team name cannot be empty".into(),
            ));
        }
        let now = Utc::now();
        let team = Team {
            id: Uuid::now_v7().to_string(),
            company_id: cmd.company_id.clone(),
            project_id: cmd.project_id.clone(),
            name,
            is_primary: cmd.is_primary,
            status: TeamStatus::Forming,
            row_version: 1,
            created_at: now,
            updated_at: now,
        };
        m3_create!(
            self,
            cmd.company_id,
            cmd.project_id,
            cmd.meta,
            cmd,
            "CreateTeam",
            "Team",
            team.id,
            team,
            |tx| insert_team_tx(tx, &team)
        )
    }

    pub async fn list_teams(
        &self,
        company: &CompanyId,
        project: &str,
    ) -> Result<Vec<Team>, ApplicationError> {
        self.get_project(company, project).await?;
        Ok(list_teams(&self.pool, company, project).await?)
    }

    pub async fn create_staffing_requirement(
        &self,
        cmd: CreateStaffingRequirementCommand,
    ) -> Result<StaffingRequirement, ApplicationError> {
        self.get_project(&cmd.company_id, &cmd.project_id).await?;
        if let Some(id) = &cmd.team_id {
            self.get_team(&cmd.company_id, &cmd.project_id, id).await?;
        }
        if cmd.desired_count == 0 || cmd.role_id.trim().is_empty() {
            return Err(ApplicationError::Validation(
                "Staffing role and desired count are required".into(),
            ));
        }
        let now = Utc::now();
        let req = StaffingRequirement {
            id: Uuid::now_v7().to_string(),
            company_id: cmd.company_id.clone(),
            project_id: cmd.project_id.clone(),
            team_id: cmd.team_id.clone(),
            role_id: cmd.role_id.clone(),
            department_id: cmd.department_id.clone(),
            desired_count: cmd.desired_count,
            required_capability_ids: cmd.required_capability_ids.clone(),
            status: StaffingRequirementStatus::Draft,
            row_version: 1,
            created_at: now,
            updated_at: now,
        };
        m3_create!(
            self,
            cmd.company_id,
            cmd.project_id,
            cmd.meta,
            cmd,
            "CreateStaffingRequirement",
            "StaffingRequirement",
            req.id,
            req,
            |tx| insert_staffing_requirement_tx(tx, &req)
        )
    }

    pub async fn list_staffing_requirements(
        &self,
        company: &CompanyId,
        project: &str,
    ) -> Result<Vec<StaffingRequirement>, ApplicationError> {
        self.get_project(company, project).await?;
        Ok(list_staffing_requirements(&self.pool, company, project).await?)
    }

    pub async fn get_staffing_requirement(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
    ) -> Result<StaffingRequirement, ApplicationError> {
        self.get_project(company, project).await?;
        persistence::get_staffing_requirement(&self.pool, company, id)
            .await?
            .filter(|v| v.project_id == project)
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn reconcile_staffing_requirement(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
        expected: i64,
    ) -> Result<StaffingRequirement, ApplicationError> {
        let mut requirement = self.get_staffing_requirement(company, project, id).await?;
        if requirement.row_version != expected {
            return Err(ApplicationError::StaleVersion {
                current: requirement.row_version,
                expected,
            });
        }
        let active: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_allocations WHERE company_id = ? AND project_id = ? AND staffing_requirement_id = ? AND status = 'ACTIVE'")
            .bind(&company.0).bind(project).bind(id).fetch_one(&self.pool).await?;
        let old_status = requirement.status;
        requirement.reconcile(active as u32);
        if requirement.status != old_status {
            let status = requirement.status;
            m3_transition!(
                self,
                company,
                project,
                "StaffingRequirement",
                id,
                requirement.row_version,
                status.to_string(),
                |tx| update_staffing_requirement_status_tx(tx, company, id, status, expected)
            )?;
        }
        Ok(requirement)
    }

    pub async fn create_agent_allocation(
        &self,
        cmd: CreateAgentAllocationCommand,
    ) -> Result<AgentAllocation, ApplicationError> {
        self.get_team(&cmd.company_id, &cmd.project_id, &cmd.team_id)
            .await?;
        if let Some(id) = &cmd.staffing_requirement_id {
            persistence::get_staffing_requirement(&self.pool, &cmd.company_id, id)
                .await?
                .filter(|r| r.project_id == cmd.project_id)
                .ok_or_else(|| ApplicationError::NotFound(id.clone()))?;
        }
        let now = Utc::now();
        let alloc = AgentAllocation {
            id: Uuid::now_v7().to_string(),
            company_id: cmd.company_id.clone(),
            project_id: cmd.project_id.clone(),
            team_id: cmd.team_id.clone(),
            agent_id: cmd.agent_id.clone(),
            staffing_requirement_id: cmd.staffing_requirement_id.clone(),
            status: AgentAllocationStatus::Planned,
            row_version: 1,
            created_at: now,
            updated_at: now,
            released_at: None,
        };
        m3_create!(
            self,
            cmd.company_id,
            cmd.project_id,
            cmd.meta,
            cmd,
            "CreateAgentAllocation",
            "AgentAllocation",
            alloc.id,
            alloc,
            |tx| insert_agent_allocation_tx(tx, &alloc)
        )
    }

    pub async fn list_agent_allocations(
        &self,
        company: &CompanyId,
        project: &str,
    ) -> Result<Vec<AgentAllocation>, ApplicationError> {
        self.get_project(company, project).await?;
        Ok(list_agent_allocations(&self.pool, company, project).await?)
    }

    pub async fn get_agent_allocation(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
    ) -> Result<AgentAllocation, ApplicationError> {
        self.get_project(company, project).await?;
        persistence::get_agent_allocation(&self.pool, company, id)
            .await?
            .filter(|v| v.project_id == project)
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn create_work_item(
        &self,
        cmd: CreateWorkItemCommand,
    ) -> Result<WorkItem, ApplicationError> {
        self.get_project(&cmd.company_id, &cmd.project_id).await?;
        if let Some(id) = &cmd.objective_id {
            persistence::get_objective(&self.pool, &cmd.company_id, id)
                .await?
                .filter(|v| v.project_id == cmd.project_id)
                .ok_or_else(|| ApplicationError::NotFound(id.clone()))?;
        }
        let mut item = WorkItem::create(
            cmd.company_id.clone(),
            cmd.project_id.clone(),
            cmd.title.clone(),
        )?;
        item.description = cmd.description.clone();
        item.objective_id = cmd.objective_id.clone();
        item.parent_work_item_id = cmd.parent_work_item_id.clone();
        item.work_type = cmd.work_type;
        m3_create!(
            self,
            cmd.company_id,
            cmd.project_id,
            cmd.meta,
            cmd,
            "CreateWorkItem",
            "WorkItem",
            item.id,
            item,
            |tx| insert_work_item_tx(tx, &item)
        )
    }

    pub async fn create_work_dependency(
        &self,
        cmd: CreateWorkDependencyCommand,
    ) -> Result<WorkDependency, ApplicationError> {
        let item = self
            .get_work_item(&cmd.company_id, &cmd.work_item_id)
            .await?;
        let dep_item = self
            .get_work_item(&cmd.company_id, &cmd.depends_on_work_item_id)
            .await?;
        if item.project_id != cmd.project_id
            || dep_item.project_id != cmd.project_id
            || item.id == dep_item.id
        {
            return Err(ApplicationError::Validation(
                "Dependency items must be distinct and in the project".into(),
            ));
        }
        let dependency = WorkDependency {
            id: dependency_id(
                &cmd.company_id,
                &cmd.project_id,
                &cmd.work_item_id,
                &cmd.depends_on_work_item_id,
            ),
            company_id: cmd.company_id.clone(),
            project_id: cmd.project_id.clone(),
            work_item_id: cmd.work_item_id.clone(),
            depends_on_work_item_id: cmd.depends_on_work_item_id.clone(),
            dependency_type: cmd.dependency_type,
            created_at: Utc::now(),
        };
        m3_create!(
            self,
            cmd.company_id,
            cmd.project_id,
            cmd.meta,
            cmd,
            "CreateWorkDependency",
            "WorkDependency",
            dependency.id,
            dependency,
            |tx| insert_work_dependency_tx(tx, &dependency)
        )
    }

    pub async fn get_work_dependency(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
    ) -> Result<WorkDependency, ApplicationError> {
        self.get_project(company, project).await?;
        self.list_work_dependencies(company, project)
            .await?
            .into_iter()
            .find(|v| v.id == id)
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn list_work_dependencies(
        &self,
        company: &CompanyId,
        project: &str,
    ) -> Result<Vec<WorkDependency>, ApplicationError> {
        self.get_project(company, project).await?;
        Ok(list_work_dependencies(&self.pool, company, project).await?)
    }

    pub async fn delete_work_dependency(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
    ) -> Result<(), ApplicationError> {
        let dependency = self.get_work_dependency(company, project, id).await?;
        let correlation_id = Uuid::now_v7().to_string();
        let principal = PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001");
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query("DELETE FROM work_dependencies WHERE company_id = ? AND project_id = ? AND work_item_id = ? AND depends_on_work_item_id = ?")
            .bind(&company.0).bind(project).bind(&dependency.work_item_id).bind(&dependency.depends_on_work_item_id).execute(&mut *tx).await?;
        if result.rows_affected() == 0 {
            return Err(ApplicationError::NotFound(id.into()));
        }
        let event = DomainEvent {
            event_id: Uuid::now_v7().to_string(),
            event_type: "WorkDependencyDeleted".into(),
            schema_version: 1,
            company_id: company.clone(),
            aggregate_type: "WorkDependency".into(),
            aggregate_id: id.into(),
            aggregate_version: 1,
            occurred_at: Utc::now(),
            correlation_id: correlation_id.clone(),
            causation_id: correlation_id,
            principal: principal.clone(),
            scope: ScopeRef::company(&company.0),
            payload: serde_json::to_value(&dependency)
                .map_err(|e| ApplicationError::Validation(e.to_string()))?,
        };
        insert_domain_event_and_outbox_tx(&mut tx, &event).await?;
        tx.commit().await?;
        self.audit_sink.record(AuditRecord {
            audit_id: Uuid::now_v7().to_string(),
            action: "WorkDependencyDeleted".into(),
            principal,
            scope: ScopeRef::company(&company.0),
            occurred_at: Utc::now(),
            details: serde_json::json!({"company_id": company.0, "project_id": project, "id": id}),
        });
        Ok(())
    }

    pub async fn get_work_assignment(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
    ) -> Result<WorkAssignment, ApplicationError> {
        self.get_project(company, project).await?;
        persistence::get_work_assignment(&self.pool, company, id)
            .await?
            .filter(|v| v.project_id == project)
            .ok_or_else(|| ApplicationError::NotFound(id.into()))
    }

    pub async fn release_work_assignment(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
        expected: i64,
    ) -> Result<WorkAssignment, ApplicationError> {
        self.transition_work_assignment(company, project, id, AssignmentStatus::Released, expected)
            .await
    }

    pub async fn reassign_work_assignment(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
        allocation_id: &str,
        expected: i64,
    ) -> Result<WorkAssignment, ApplicationError> {
        let old = self.get_work_assignment(company, project, id).await?;
        if old.row_version != expected {
            return Err(ApplicationError::StaleVersion {
                current: old.row_version,
                expected,
            });
        }
        if old.status != AssignmentStatus::Active {
            return Err(ApplicationError::Validation(
                "Only active assignments can be reassigned".into(),
            ));
        }
        let alloc = self
            .get_agent_allocation(company, project, allocation_id)
            .await?;
        self.release_work_assignment(company, project, id, expected)
            .await?;
        self.create_work_assignment(CreateWorkAssignmentCommand::new(
            company.clone(),
            project.into(),
            old.work_item_id,
            alloc.agent_id,
            allocation_id.into(),
            old.is_primary,
        ))
        .await
    }

    pub async fn create_work_assignment(
        &self,
        cmd: CreateWorkAssignmentCommand,
    ) -> Result<WorkAssignment, ApplicationError> {
        let item = self
            .get_work_item(&cmd.company_id, &cmd.work_item_id)
            .await?;
        let alloc =
            persistence::get_agent_allocation(&self.pool, &cmd.company_id, &cmd.allocation_id)
                .await?
                .filter(|a| a.project_id == cmd.project_id && a.agent_id == cmd.agent_id)
                .ok_or_else(|| ApplicationError::NotFound(cmd.allocation_id.clone()))?;
        if item.project_id != cmd.project_id || alloc.status != AgentAllocationStatus::Active {
            return Err(ApplicationError::Validation(
                "Assignment requires an active project allocation".into(),
            ));
        }
        let now = Utc::now();
        let assign = WorkAssignment {
            id: Uuid::now_v7().to_string(),
            company_id: cmd.company_id.clone(),
            project_id: cmd.project_id.clone(),
            work_item_id: cmd.work_item_id.clone(),
            agent_id: cmd.agent_id.clone(),
            is_primary: cmd.is_primary,
            status: AssignmentStatus::Active,
            row_version: 1,
            created_at: now,
            updated_at: now,
            released_at: None,
        };
        let alloc_id = cmd.allocation_id.clone();
        m3_create!(
            self,
            cmd.company_id,
            cmd.project_id,
            cmd.meta,
            cmd,
            "CreateWorkAssignment",
            "WorkAssignment",
            assign.id,
            assign,
            |tx| insert_work_assignment_tx(tx, &assign, &alloc_id)
        )
    }

    pub async fn list_work_assignments(
        &self,
        company: &CompanyId,
        project: &str,
    ) -> Result<Vec<WorkAssignment>, ApplicationError> {
        self.get_project(company, project).await?;
        Ok(list_work_assignments(&self.pool, company, project).await?)
    }

    pub async fn transition_objective(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
        status: ObjectiveStatus,
        expected: i64,
    ) -> Result<Objective, ApplicationError> {
        let mut v = persistence::get_objective(&self.pool, company, id)
            .await?
            .filter(|v| v.project_id == project)
            .ok_or_else(|| ApplicationError::NotFound(id.into()))?;
        if v.row_version != expected {
            return Err(ApplicationError::StaleVersion {
                current: v.row_version,
                expected,
            });
        }
        match status {
            ObjectiveStatus::Active => v.activate(expected)?,
            ObjectiveStatus::Achieved | ObjectiveStatus::Failed | ObjectiveStatus::Cancelled => {
                v.finish(status, expected)?
            }
            ObjectiveStatus::Archived => v.archive(expected)?,
            _ => {
                return Err(ApplicationError::Validation(
                    "Invalid objective transition".into(),
                ));
            }
        };
        let status_str = status.to_string();
        let new_version = v.row_version;
        m3_transition!(
            self,
            company,
            project,
            "Objective",
            id,
            new_version,
            &status_str,
            |tx| update_objective_status_tx(tx, company, id, status, expected)
        )?;
        Ok(v)
    }

    pub async fn transition_team(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
        status: TeamStatus,
        expected: i64,
    ) -> Result<Team, ApplicationError> {
        let mut v = self.get_team(company, project, id).await?;
        if v.row_version != expected {
            return Err(ApplicationError::StaleVersion {
                current: v.row_version,
                expected,
            });
        }
        if !matches!(
            (v.status, status),
            (
                TeamStatus::Forming,
                TeamStatus::Active | TeamStatus::Disbanded
            ) | (
                TeamStatus::Active,
                TeamStatus::Paused | TeamStatus::Disbanded
            ) | (
                TeamStatus::Paused,
                TeamStatus::Active | TeamStatus::Disbanded
            ) | (TeamStatus::Disbanded, TeamStatus::Archived)
        ) {
            return Err(ApplicationError::Validation(
                "Invalid team transition".into(),
            ));
        }
        if status == TeamStatus::Disbanded {
            let active_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM agent_allocations WHERE company_id = ? AND project_id = ? AND team_id = ? AND status = 'ACTIVE'",
            )
            .bind(&company.0)
            .bind(project)
            .bind(id)
            .fetch_one(&self.pool)
            .await?;
            if active_count > 0 {
                return Err(ApplicationError::Validation(
                    "cannot disband team with active allocations".into(),
                ));
            }
        }
        v.status = status;
        v.row_version += 1;
        v.updated_at = Utc::now();
        let status_str = status.to_string();
        let new_version = v.row_version;
        m3_transition!(
            self,
            company,
            project,
            "Team",
            id,
            new_version,
            &status_str,
            |tx| update_team_status_tx(tx, company, id, status, expected)
        )?;
        Ok(v)
    }

    pub async fn transition_staffing_requirement(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
        status: StaffingRequirementStatus,
        expected: i64,
    ) -> Result<StaffingRequirement, ApplicationError> {
        let mut v = persistence::get_staffing_requirement(&self.pool, company, id)
            .await?
            .filter(|v| v.project_id == project)
            .ok_or_else(|| ApplicationError::NotFound(id.into()))?;
        if v.row_version != expected {
            return Err(ApplicationError::StaleVersion {
                current: v.row_version,
                expected,
            });
        }
        match status {
            StaffingRequirementStatus::Open => {
                if v.status == StaffingRequirementStatus::Blocked {
                    v.unblock()?;
                } else {
                    v.open()?;
                }
            }
            StaffingRequirementStatus::Blocked => {
                if v.status == StaffingRequirementStatus::Draft {
                    v.status = StaffingRequirementStatus::Blocked;
                    v.row_version += 1;
                    v.updated_at = Utc::now();
                } else {
                    v.block()?;
                }
            }
            StaffingRequirementStatus::Cancelled => v.cancel()?,
            _ => {
                return Err(ApplicationError::Validation(
                    "Invalid staffing transition".into(),
                ));
            }
        }
        let status_str = status.to_string();
        let new_version = v.row_version;
        m3_transition!(
            self,
            company,
            project,
            "StaffingRequirement",
            id,
            new_version,
            &status_str,
            |tx| update_staffing_requirement_status_tx(tx, company, id, status, expected)
        )?;
        Ok(v)
    }

    pub async fn transition_agent_allocation(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
        status: AgentAllocationStatus,
        expected: i64,
    ) -> Result<AgentAllocation, ApplicationError> {
        let mut v = persistence::get_agent_allocation(&self.pool, company, id)
            .await?
            .filter(|v| v.project_id == project)
            .ok_or_else(|| ApplicationError::NotFound(id.into()))?;
        if v.row_version != expected {
            return Err(ApplicationError::StaleVersion {
                current: v.row_version,
                expected,
            });
        }
        if !matches!(
            (v.status, status),
            (
                AgentAllocationStatus::Planned,
                AgentAllocationStatus::Active | AgentAllocationStatus::Cancelled
            ) | (
                AgentAllocationStatus::Active,
                AgentAllocationStatus::Paused
                    | AgentAllocationStatus::Released
                    | AgentAllocationStatus::Cancelled
            ) | (
                AgentAllocationStatus::Paused,
                AgentAllocationStatus::Active
                    | AgentAllocationStatus::Released
                    | AgentAllocationStatus::Cancelled
            )
        ) {
            return Err(ApplicationError::Validation(
                "Invalid allocation transition".into(),
            ));
        }
        v.status = status;
        v.row_version += 1;
        v.updated_at = Utc::now();
        if matches!(
            status,
            AgentAllocationStatus::Released | AgentAllocationStatus::Cancelled
        ) {
            v.released_at = Some(v.updated_at);
        }
        let status_str = status.to_string();
        let new_version = v.row_version;
        m3_transition!(
            self,
            company,
            project,
            "AgentAllocation",
            id,
            new_version,
            &status_str,
            |tx| update_agent_allocation_status_tx(tx, company, id, status, expected)
        )?;
        Ok(v)
    }

    pub async fn transition_work_item(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
        status: WorkItemStatus,
        expected: i64,
    ) -> Result<WorkItem, ApplicationError> {
        let mut v = self.get_work_item(company, id).await?;
        if v.project_id != project {
            return Err(ApplicationError::NotFound(id.into()));
        }
        if v.row_version != expected {
            return Err(ApplicationError::StaleVersion {
                current: v.row_version,
                expected,
            });
        }
        match status {
            WorkItemStatus::Ready => {
                let mut has_unresolved_hard_dep = false;
                for dependency in list_work_dependencies(&self.pool, company, project)
                    .await?
                    .into_iter()
                    .filter(|dependency| {
                        dependency.work_item_id == id
                            && dependency.dependency_type == DependencyType::Hard
                    })
                {
                    let blocker = self
                        .get_work_item(company, &dependency.depends_on_work_item_id)
                        .await?;
                    if blocker.status != WorkItemStatus::Completed {
                        has_unresolved_hard_dep = true;
                        break;
                    }
                }
                v.ready(expected, has_unresolved_hard_dep)?;
            }
            WorkItemStatus::InProgress => {
                if v.status == WorkItemStatus::Blocked {
                    v.unblock(expected)?;
                } else {
                    v.start(expected)?;
                }
            }
            WorkItemStatus::Completed => v.complete(expected)?,
            WorkItemStatus::Failed => v.fail(expected)?,
            WorkItemStatus::Cancelled => v.cancel(expected)?,
            WorkItemStatus::Blocked => v.block(expected)?,
            _ => {
                return Err(ApplicationError::Validation(
                    "Unsupported work transition".into(),
                ));
            }
        }
        let status_str = status.to_string();
        let new_version = v.row_version;
        m3_transition!(
            self,
            company,
            project,
            "WorkItem",
            id,
            new_version,
            &status_str,
            |tx| update_work_item_status_tx(tx, company, id, status, expected)
        )?;
        Ok(v)
    }

    pub async fn transition_work_assignment(
        &self,
        company: &CompanyId,
        project: &str,
        id: &str,
        status: AssignmentStatus,
        expected: i64,
    ) -> Result<WorkAssignment, ApplicationError> {
        let mut v = persistence::get_work_assignment(&self.pool, company, id)
            .await?
            .filter(|v| v.project_id == project)
            .ok_or_else(|| ApplicationError::NotFound(id.into()))?;
        if v.row_version != expected {
            return Err(ApplicationError::StaleVersion {
                current: v.row_version,
                expected,
            });
        }
        if !matches!(
            (v.status, status),
            (
                AssignmentStatus::Active,
                AssignmentStatus::Released | AssignmentStatus::Cancelled
            )
        ) {
            return Err(ApplicationError::Validation(
                "Invalid assignment transition".into(),
            ));
        }
        v.status = status;
        v.row_version += 1;
        v.updated_at = Utc::now();
        if status != AssignmentStatus::Active {
            v.released_at = Some(v.updated_at);
        }
        let status_str = status.to_string();
        let new_version = v.row_version;
        m3_transition!(
            self,
            company,
            project,
            "WorkAssignment",
            id,
            new_version,
            &status_str,
            |tx| update_work_assignment_status_tx(tx, company, id, status, expected)
        )?;
        Ok(v)
    }

    pub async fn list_work_items(
        &self,
        company_id: &CompanyId,
        project_id: Option<&str>,
    ) -> Result<Vec<WorkItem>, ApplicationError> {
        if let Some(pid) = project_id {
            Ok(list_work_items(&self.pool, company_id, pid).await?)
        } else {
            let rows = sqlx::query(
                "SELECT * FROM work_items WHERE company_id = ? ORDER BY created_at, id",
            )
            .bind(&company_id.0)
            .fetch_all(&self.pool)
            .await?;
            let mut items = Vec::new();
            for r in rows {
                use sqlx::Row;
                items.push(WorkItem {
                    id: r.get("id"),
                    company_id: CompanyId(r.get("company_id")),
                    project_id: r.get("project_id"),
                    objective_id: r.get("objective_id"),
                    parent_work_item_id: r.get("parent_work_item_id"),
                    title: r.get("title"),
                    description: r.get("description"),
                    work_type: r.get::<String, _>("logical_type").parse()?,
                    status: r.get::<String, _>("status").parse()?,
                    row_version: r.get("row_version"),
                    created_at: chrono::DateTime::parse_from_rfc3339(
                        &r.get::<String, _>("created_at"),
                    )
                    .unwrap()
                    .with_timezone(&chrono::Utc),
                    updated_at: chrono::DateTime::parse_from_rfc3339(
                        &r.get::<String, _>("updated_at"),
                    )
                    .unwrap()
                    .with_timezone(&chrono::Utc),
                });
            }
            Ok(items)
        }
    }

    pub async fn get_work_item(
        &self,
        company_id: &CompanyId,
        work_id: &str,
    ) -> Result<WorkItem, ApplicationError> {
        get_work_item(&self.pool, company_id, work_id)
            .await?
            .ok_or_else(|| ApplicationError::NotFound(work_id.to_string()))
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
