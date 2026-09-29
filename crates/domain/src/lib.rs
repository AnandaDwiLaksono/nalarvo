use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserId(pub String);

impl UserId {
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }
}

impl Default for UserId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkspaceId(pub String);

impl WorkspaceId {
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }
}

impl Default for WorkspaceId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CompanyId(pub String);

impl CompanyId {
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }
}

impl Default for CompanyId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PrincipalType {
    User,
    Agent,
    System,
}

impl fmt::Display for PrincipalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::User => write!(f, "USER"),
            Self::Agent => write!(f, "AGENT"),
            Self::System => write!(f, "SYSTEM"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PrincipalRef {
    pub principal_type: PrincipalType,
    pub principal_id: String,
}

impl PrincipalRef {
    pub fn user(user_id: impl Into<String>) -> Self {
        Self {
            principal_type: PrincipalType::User,
            principal_id: user_id.into(),
        }
    }

    pub fn system(system_id: impl Into<String>) -> Self {
        Self {
            principal_type: PrincipalType::System,
            principal_id: system_id.into(),
        }
    }

    pub fn agent(agent_id: impl Into<String>) -> Self {
        Self {
            principal_type: PrincipalType::Agent,
            principal_id: agent_id.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScopeType {
    Workspace,
    Company,
}

impl fmt::Display for ScopeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Workspace => write!(f, "WORKSPACE"),
            Self::Company => write!(f, "COMPANY"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScopeRef {
    pub scope_type: ScopeType,
    pub scope_id: String,
}

impl ScopeRef {
    pub fn workspace(workspace_id: impl Into<String>) -> Self {
        Self {
            scope_type: ScopeType::Workspace,
            scope_id: workspace_id.into(),
        }
    }

    pub fn company(company_id: impl Into<String>) -> Self {
        Self {
            scope_type: ScopeType::Company,
            scope_id: company_id.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    pub audit_id: String,
    pub action: String,
    pub principal: PrincipalRef,
    pub scope: ScopeRef,
    pub occurred_at: DateTime<Utc>,
    pub details: serde_json::Value,
}

pub trait AuditSink: Send + Sync {
    fn record(&self, record: AuditRecord);
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompanyStatus {
    Draft,
    Active,
    Paused,
    Archived,
}

impl fmt::Display for CompanyStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Draft => write!(f, "DRAFT"),
            Self::Active => write!(f, "ACTIVE"),
            Self::Paused => write!(f, "PAUSED"),
            Self::Archived => write!(f, "ARCHIVED"),
        }
    }
}

impl std::str::FromStr for CompanyStatus {
    type Err = DomainError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DRAFT" => Ok(Self::Draft),
            "ACTIVE" => Ok(Self::Active),
            "PAUSED" => Ok(Self::Paused),
            "ARCHIVED" => Ok(Self::Archived),
            other => Err(DomainError::Validation(format!(
                "Invalid company status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Company {
    pub id: CompanyId,
    pub workspace_id: WorkspaceId,
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub mission: Option<String>,
    #[serde(default)]
    pub director_user_id: Option<UserId>,
    pub status: CompanyStatus,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Company {
    pub fn create(
        workspace_id: WorkspaceId,
        name: String,
        description: Option<String>,
    ) -> Result<Self, DomainError> {
        let name = name.trim().to_string();
        if name.is_empty() {
            return Err(DomainError::Validation(
                "Company name cannot be empty".into(),
            ));
        }
        let now = Utc::now();
        Ok(Self {
            id: CompanyId::new(),
            workspace_id,
            name,
            description,
            mission: None,
            director_user_id: None,
            status: CompanyStatus::Draft,
            row_version: 1,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn set_mission(
        &mut self,
        mission: String,
        expected_version: i64,
    ) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        self.mission = Some(required("Company mission", mission)?);
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn set_director(
        &mut self,
        director: UserId,
        expected_version: i64,
    ) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if director.0.trim().is_empty() {
            return Err(DomainError::Validation("Director cannot be empty".into()));
        }
        self.director_user_id = Some(director);
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn activate(&mut self) -> Result<(), DomainError> {
        if self.director_user_id.is_none() {
            return Err(DomainError::Validation(
                "Company director is required".into(),
            ));
        }
        transition(
            &mut self.status,
            CompanyStatus::Draft,
            CompanyStatus::Active,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }

    pub fn pause(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.status,
            CompanyStatus::Active,
            CompanyStatus::Paused,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }

    pub fn resume(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.status,
            CompanyStatus::Paused,
            CompanyStatus::Active,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }

    pub fn archive(&mut self) -> Result<(), DomainError> {
        if !matches!(self.status, CompanyStatus::Draft | CompanyStatus::Paused) {
            return Err(DomainError::Validation(
                "Invalid Company archive transition".into(),
            ));
        }
        self.status = CompanyStatus::Archived;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn update_metadata(
        &mut self,
        name: String,
        description: Option<String>,
        expected_version: i64,
    ) -> Result<(), DomainError> {
        if self.row_version != expected_version {
            return Err(DomainError::StaleVersion {
                current: self.row_version,
                expected: expected_version,
            });
        }

        let name = name.trim().to_string();
        if name.is_empty() {
            return Err(DomainError::Validation(
                "Company name cannot be empty".into(),
            ));
        }

        self.name = name;
        self.description = description;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainEvent {
    pub event_id: String,
    pub event_type: String,
    pub schema_version: u32,
    pub company_id: CompanyId,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub aggregate_version: i64,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: String,
    pub causation_id: String,
    pub principal: PrincipalRef,
    pub scope: ScopeRef,
    pub payload: serde_json::Value,
}

impl DomainEvent {
    pub fn company_created(
        company: &Company,
        principal: PrincipalRef,
        correlation_id: impl Into<String>,
        causation_id: impl Into<String>,
    ) -> Self {
        let payload = serde_json::json!({
            "company_id": company.id.0,
            "workspace_id": company.workspace_id.0,
            "name": company.name,
            "status": company.status.to_string(),
        });
        Self {
            event_id: Uuid::now_v7().to_string(),
            event_type: "CompanyCreated".into(),
            schema_version: 1,
            company_id: company.id.clone(),
            aggregate_type: "Company".into(),
            aggregate_id: company.id.0.clone(),
            aggregate_version: company.row_version,
            occurred_at: company.created_at,
            correlation_id: correlation_id.into(),
            causation_id: causation_id.into(),
            principal,
            scope: ScopeRef::company(company.id.0.clone()),
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdempotencyStatus {
    Pending,
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    pub id: String,
    pub scope: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub status: IdempotencyStatus,
    pub response_body: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OutboxStatus {
    Pending,
    Published,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxMessage {
    pub id: String,
    pub domain_event_id: String,
    pub status: OutboxStatus,
    pub attempt_count: u32,
    pub available_at: DateTime<Utc>,
    pub lease_owner: Option<String>,
    pub lease_until: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerInbox {
    pub consumer_name: String,
    pub event_id: String,
    pub processed_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("Stale aggregate version: current {current}, expected {expected}")]
    StaleVersion { current: i64, expected: i64 },

    #[error("Validation failed: {0}")]
    Validation(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Scope violation: {0}")]
    ScopeViolation(String),

    #[error("Idempotency conflict: key reuse mismatch for key {0}")]
    IdempotencyKeyReuseMismatch(String),
}

// M2 domain records. Infrastructure resolves locators and validates cross-record scope.
fn required(label: &str, value: String) -> Result<String, DomainError> {
    let value = value.trim().to_owned();
    if value.is_empty() {
        Err(DomainError::Validation(format!("{label} cannot be empty")))
    } else {
        Ok(value)
    }
}

fn check_version(current: i64, expected: i64) -> Result<(), DomainError> {
    if current == expected {
        Ok(())
    } else {
        Err(DomainError::StaleVersion { current, expected })
    }
}

fn transition<S: PartialEq>(
    state: &mut S,
    from: S,
    to: S,
    version: &mut i64,
    updated_at: &mut DateTime<Utc>,
) -> Result<(), DomainError> {
    if *state != from {
        return Err(DomainError::Validation(
            "Invalid lifecycle transition".into(),
        ));
    }
    *state = to;
    *version += 1;
    *updated_at = Utc::now();
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkspaceLifecycle {
    Provisioning,
    Active,
    Suspended,
    Archived,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    pub id: WorkspaceId,
    pub owner_user_id: UserId,
    pub name: String,
    pub lifecycle: WorkspaceLifecycle,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl Workspace {
    pub fn create(owner_user_id: UserId, name: String) -> Result<Self, DomainError> {
        if owner_user_id.0.trim().is_empty() {
            return Err(DomainError::Validation("Owner cannot be empty".into()));
        }
        let now = Utc::now();
        Ok(Self {
            id: WorkspaceId::new(),
            owner_user_id,
            name: required("Workspace name", name)?,
            lifecycle: WorkspaceLifecycle::Provisioning,
            row_version: 1,
            created_at: now,
            updated_at: now,
        })
    }
    pub fn activate(&mut self) -> Result<(), DomainError> {
        if !matches!(
            self.lifecycle,
            WorkspaceLifecycle::Provisioning | WorkspaceLifecycle::Suspended
        ) {
            return Err(DomainError::Validation(
                "Invalid Workspace activation".into(),
            ));
        }
        self.lifecycle = WorkspaceLifecycle::Active;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }
    pub fn suspend(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            WorkspaceLifecycle::Active,
            WorkspaceLifecycle::Suspended,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
    pub fn archive(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            WorkspaceLifecycle::Suspended,
            WorkspaceLifecycle::Archived,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CredentialLifecycle {
    Active,
    Disabled,
    Revoked,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialRef {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub credential_type: String,
    pub secret_locator: String,
    pub label: String,
    pub lifecycle: CredentialLifecycle,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl CredentialRef {
    pub fn create(
        workspace_id: WorkspaceId,
        credential_type: &str,
        secret_locator: &str,
        label: &str,
    ) -> Result<Self, DomainError> {
        let locator = required("Secret locator", secret_locator.into())?;
        if !locator.starts_with("keychain://") || locator.contains(['?', '#', '@']) {
            return Err(DomainError::Validation(
                "Secret locator must be a keychain reference".into(),
            ));
        }
        let now = Utc::now();
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            workspace_id,
            credential_type: required("Credential type", credential_type.into())?,
            secret_locator: locator,
            label: required("Credential label", label.into())?,
            lifecycle: CredentialLifecycle::Active,
            expires_at: None,
            created_at: now,
            updated_at: now,
        })
    }
    pub fn disable(&mut self) -> Result<(), DomainError> {
        if self.lifecycle != CredentialLifecycle::Active {
            return Err(DomainError::Validation("Credential is not active".into()));
        }
        self.lifecycle = CredentialLifecycle::Disabled;
        self.updated_at = Utc::now();
        Ok(())
    }
    pub fn enable(&mut self) -> Result<(), DomainError> {
        if self.lifecycle != CredentialLifecycle::Disabled {
            return Err(DomainError::Validation(
                "Credential cannot be enabled".into(),
            ));
        }
        self.lifecycle = CredentialLifecycle::Active;
        self.updated_at = Utc::now();
        Ok(())
    }
    pub fn revoke(&mut self) -> Result<(), DomainError> {
        if matches!(
            self.lifecycle,
            CredentialLifecycle::Revoked | CredentialLifecycle::Expired
        ) {
            return Err(DomainError::Validation("Credential is terminal".into()));
        }
        self.lifecycle = CredentialLifecycle::Revoked;
        self.updated_at = Utc::now();
        Ok(())
    }
    pub fn expire(&mut self) -> Result<(), DomainError> {
        if matches!(
            self.lifecycle,
            CredentialLifecycle::Revoked | CredentialLifecycle::Expired
        ) {
            return Err(DomainError::Validation("Credential is terminal".into()));
        }
        self.lifecycle = CredentialLifecycle::Expired;
        self.updated_at = Utc::now();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderConnectionLifecycle {
    Configured,
    Enabled,
    Disabled,
    Removed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderHealth {
    Unknown,
    Healthy,
    Degraded,
    Unavailable,
}

// Typed allowlist: arbitrary JSON config could persist API keys in ordinary domain state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    pub default_model: Option<String>,
    pub supports_streaming: bool,
    pub supports_tool_calling: bool,
    pub supports_structured_output: bool,
    pub context_limit: Option<u32>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderConnection {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub provider_key: String,
    pub name: String,
    pub endpoint: Option<String>,
    pub credential_ref_id: Option<String>,
    pub config: ProviderConfig,
    pub lifecycle: ProviderConnectionLifecycle,
    pub health: ProviderHealth,
    pub last_health_check_at: Option<DateTime<Utc>>,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl ProviderConnection {
    pub fn create(
        workspace_id: WorkspaceId,
        provider_key: String,
        name: String,
        endpoint: Option<String>,
        credential_ref_id: Option<String>,
        config: ProviderConfig,
    ) -> Result<Self, DomainError> {
        if config.context_limit == Some(0) {
            return Err(DomainError::Validation(
                "Context limit must be positive".into(),
            ));
        }
        if let Some(ref endpoint) = endpoint
            && (!(endpoint.starts_with("https://")
                || endpoint.starts_with("http://localhost:")
                || endpoint.starts_with("http://127.0.0.1:"))
                || endpoint.contains('@')
                || endpoint.contains(['?', '#']))
        {
            return Err(DomainError::Validation(
                "Endpoint must be a safe HTTPS or local URL".into(),
            ));
        }
        let now = Utc::now();
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            workspace_id,
            provider_key: required("Provider key", provider_key)?,
            name: required("Connection name", name)?,
            endpoint,
            credential_ref_id,
            config,
            lifecycle: ProviderConnectionLifecycle::Configured,
            health: ProviderHealth::Unknown,
            last_health_check_at: None,
            row_version: 1,
            created_at: now,
            updated_at: now,
        })
    }
    pub fn enable(&mut self) -> Result<(), DomainError> {
        if !matches!(
            self.lifecycle,
            ProviderConnectionLifecycle::Configured | ProviderConnectionLifecycle::Disabled
        ) {
            return Err(DomainError::Validation(
                "Invalid provider enable transition".into(),
            ));
        }
        self.lifecycle = ProviderConnectionLifecycle::Enabled;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }
    pub fn disable(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            ProviderConnectionLifecycle::Enabled,
            ProviderConnectionLifecycle::Disabled,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
    pub fn remove(&mut self) -> Result<(), DomainError> {
        if !matches!(
            self.lifecycle,
            ProviderConnectionLifecycle::Configured | ProviderConnectionLifecycle::Disabled
        ) {
            return Err(DomainError::Validation(
                "Disable provider before removal".into(),
            ));
        }
        self.lifecycle = ProviderConnectionLifecycle::Removed;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }
    pub fn record_health(&mut self, health: ProviderHealth, checked_at: DateTime<Utc>) {
        self.health = health;
        self.last_health_check_at = Some(checked_at);
        self.row_version += 1;
        self.updated_at = Utc::now();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Model {
    pub id: String,
    pub provider_connection_id: String,
    pub model_identifier: String,
    pub display_name: Option<String>,
    pub capabilities: serde_json::Value,
    pub context_limit: Option<u32>,
    pub updated_at: DateTime<Utc>,
}
impl Model {
    pub fn create(
        provider_connection_id: String,
        model_identifier: String,
        display_name: Option<String>,
        capabilities: serde_json::Value,
        context_limit: Option<u32>,
    ) -> Result<Self, DomainError> {
        if context_limit == Some(0) || !capabilities.is_object() {
            return Err(DomainError::Validation("Invalid model metadata".into()));
        }
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            provider_connection_id: required("Provider connection", provider_connection_id)?,
            model_identifier: required("Model identifier", model_identifier)?,
            display_name,
            capabilities,
            context_limit,
            updated_at: Utc::now(),
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelProfile {
    pub id: String,
    pub workspace_id: WorkspaceId,
    pub name: String,
    pub primary_model_id: String,
    pub inference_configuration: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl ModelProfile {
    pub fn create(
        workspace_id: WorkspaceId,
        name: String,
        primary_model_id: String,
        inference_configuration: serde_json::Value,
    ) -> Result<Self, DomainError> {
        if !inference_configuration.is_object() {
            return Err(DomainError::Validation(
                "Invalid inference configuration".into(),
            ));
        }
        let now = Utc::now();
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            workspace_id,
            name: required("Profile name", name)?,
            primary_model_id: required("Primary model", primary_model_id)?,
            inference_configuration,
            created_at: now,
            updated_at: now,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DepartmentLifecycle {
    Draft,
    Active,
    Paused,
    Retired,
    Archived,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Department {
    pub id: String,
    pub company_id: CompanyId,
    pub name: String,
    pub purpose: Option<String>,
    pub lifecycle: DepartmentLifecycle,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl Department {
    pub fn create(
        company_id: CompanyId,
        name: String,
        purpose: Option<String>,
    ) -> Result<Self, DomainError> {
        let now = Utc::now();
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            company_id,
            name: required("Department name", name)?,
            purpose,
            lifecycle: DepartmentLifecycle::Draft,
            row_version: 1,
            created_at: now,
            updated_at: now,
        })
    }
    pub fn activate(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            DepartmentLifecycle::Draft,
            DepartmentLifecycle::Active,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
    pub fn pause(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            DepartmentLifecycle::Active,
            DepartmentLifecycle::Paused,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
    pub fn resume(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            DepartmentLifecycle::Paused,
            DepartmentLifecycle::Active,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
    pub fn retire(&mut self, active_agents: u32) -> Result<(), DomainError> {
        if active_agents != 0 {
            return Err(DomainError::Validation(
                "Department still has active Agents".into(),
            ));
        }
        transition(
            &mut self.lifecycle,
            DepartmentLifecycle::Paused,
            DepartmentLifecycle::Retired,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
    pub fn archive(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            DepartmentLifecycle::Retired,
            DepartmentLifecycle::Archived,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RoleStatus {
    Active,
    Disabled,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub id: String,
    pub company_id: CompanyId,
    pub name: String,
    pub description: Option<String>,
    pub responsibilities: String,
    pub status: RoleStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl Role {
    pub fn create(
        company_id: CompanyId,
        name: String,
        description: Option<String>,
        responsibilities: String,
    ) -> Result<Self, DomainError> {
        let now = Utc::now();
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            company_id,
            name: required("Role name", name)?,
            description,
            responsibilities: required("Responsibilities", responsibilities)?,
            status: RoleStatus::Active,
            created_at: now,
            updated_at: now,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentLifecycle {
    Created,
    Active,
    Paused,
    Retired,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentAvailability {
    Available,
    PartiallyAllocated,
    FullyAllocated,
    Unavailable,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub id: String,
    pub company_id: CompanyId,
    pub department_id: String,
    pub role_id: String,
    pub name: String,
    pub model_profile_id: Option<String>,
    pub instructions: Option<String>,
    pub lifecycle: AgentLifecycle,
    pub max_active_allocations: u32,
    pub availability_override: bool,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
impl Agent {
    pub fn create(
        company_id: CompanyId,
        department_id: String,
        role_id: String,
        name: String,
        max_active_allocations: u32,
    ) -> Result<Self, DomainError> {
        if max_active_allocations < 1 {
            return Err(DomainError::Validation(
                "Agent capacity must be positive".into(),
            ));
        }
        let now = Utc::now();
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            company_id,
            department_id: required("Department", department_id)?,
            role_id: required("Role", role_id)?,
            name: required("Agent name", name)?,
            model_profile_id: None,
            instructions: None,
            lifecycle: AgentLifecycle::Created,
            max_active_allocations,
            availability_override: false,
            row_version: 1,
            created_at: now,
            updated_at: now,
        })
    }
    pub fn activate(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            AgentLifecycle::Created,
            AgentLifecycle::Active,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
    pub fn pause(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            AgentLifecycle::Active,
            AgentLifecycle::Paused,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
    pub fn resume(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            AgentLifecycle::Paused,
            AgentLifecycle::Active,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
    pub fn retire(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.lifecycle,
            AgentLifecycle::Active,
            AgentLifecycle::Retired,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
    pub fn availability(&self, active_allocations: u32) -> AgentAvailability {
        if self.lifecycle != AgentLifecycle::Active
            || self.availability_override
            || active_allocations > self.max_active_allocations
        {
            AgentAvailability::Unavailable
        } else if active_allocations == self.max_active_allocations {
            AgentAvailability::FullyAllocated
        } else if active_allocations == 0 {
            AgentAvailability::Available
        } else {
            AgentAvailability::PartiallyAllocated
        }
    }
    pub fn set_capacity(&mut self, slots: u32, active_allocations: u32) -> Result<(), DomainError> {
        if slots == 0 || slots < active_allocations {
            return Err(DomainError::Validation(
                "Capacity below active allocations".into(),
            ));
        }
        self.max_active_allocations = slots;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_company_creation_and_optimistic_concurrency() {
        let ws_id = WorkspaceId::new();
        let mut company =
            Company::create(ws_id, "Nalarvo Inc".into(), Some("AI OS".into())).unwrap();
        assert_eq!(company.row_version, 1);
        assert_eq!(company.status, CompanyStatus::Draft);

        // Stale update fails
        let err = company
            .update_metadata("New Name".into(), None, 999)
            .unwrap_err();
        assert!(matches!(err, DomainError::StaleVersion { .. }));

        // Valid update succeeds and bumps version
        company.update_metadata("New Name".into(), None, 1).unwrap();
        assert_eq!(company.row_version, 2);
        assert_eq!(company.name, "New Name");
    }

    #[test]
    fn test_principal_and_scope_ref() {
        let p_user = PrincipalRef::user("u-123");
        assert_eq!(p_user.principal_type, PrincipalType::User);
        assert_eq!(p_user.principal_id, "u-123");

        let p_sys = PrincipalRef::system("sys-core");
        assert_eq!(p_sys.principal_type, PrincipalType::System);

        let s_co = ScopeRef::company("co-456");
        assert_eq!(s_co.scope_type, ScopeType::Company);
        assert_eq!(s_co.scope_id, "co-456");
    }
}
