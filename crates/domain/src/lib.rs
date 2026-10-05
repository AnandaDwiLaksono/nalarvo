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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProjectStatus {
    Draft,
    Staffing,
    Active,
    Paused,
    Completed,
    Cancelled,
    Archived,
}

impl fmt::Display for ProjectStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Draft => write!(f, "DRAFT"),
            Self::Staffing => write!(f, "STAFFING"),
            Self::Active => write!(f, "ACTIVE"),
            Self::Paused => write!(f, "PAUSED"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Cancelled => write!(f, "CANCELLED"),
            Self::Archived => write!(f, "ARCHIVED"),
        }
    }
}

impl std::str::FromStr for ProjectStatus {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DRAFT" => Ok(Self::Draft),
            "STAFFING" => Ok(Self::Staffing),
            "ACTIVE" => Ok(Self::Active),
            "PAUSED" => Ok(Self::Paused),
            "COMPLETED" => Ok(Self::Completed),
            "CANCELLED" => Ok(Self::Cancelled),
            "ARCHIVED" => Ok(Self::Archived),
            other => Err(DomainError::Validation(format!(
                "Invalid project status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProjectPriority {
    Low,
    Medium,
    High,
    Critical,
}

impl fmt::Display for ProjectPriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Low => write!(f, "LOW"),
            Self::Medium => write!(f, "MEDIUM"),
            Self::High => write!(f, "HIGH"),
            Self::Critical => write!(f, "CRITICAL"),
        }
    }
}

impl std::str::FromStr for ProjectPriority {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "LOW" => Ok(Self::Low),
            "MEDIUM" => Ok(Self::Medium),
            "HIGH" => Ok(Self::High),
            "CRITICAL" => Ok(Self::Critical),
            other => Err(DomainError::Validation(format!(
                "Invalid project priority: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub company_id: CompanyId,
    pub name: String,
    pub description: Option<String>,
    pub priority: ProjectPriority,
    pub owner_user_id: Option<UserId>,
    pub target_outcome: Option<String>,
    pub target_date: Option<String>,
    pub working_root_path: Option<String>,
    pub working_root_bound_at: Option<DateTime<Utc>>,
    pub status: ProjectStatus,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Project {
    pub fn create(
        company_id: CompanyId,
        name: String,
        description: Option<String>,
    ) -> Result<Self, DomainError> {
        let name = required("Project name", name)?;
        let now = Utc::now();
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            company_id,
            name,
            description,
            priority: ProjectPriority::Medium,
            owner_user_id: None,
            target_outcome: None,
            target_date: None,
            working_root_path: None,
            working_root_bound_at: None,
            status: ProjectStatus::Draft,
            row_version: 1,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn activate(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if !matches!(
            self.status,
            ProjectStatus::Draft | ProjectStatus::Staffing | ProjectStatus::Paused
        ) {
            return Err(DomainError::Validation(
                "Invalid project activation transition".into(),
            ));
        }
        self.status = ProjectStatus::Active;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn start_staffing(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if self.status != ProjectStatus::Draft {
            return Err(DomainError::Validation(
                "Invalid staffing transition".into(),
            ));
        }
        self.status = ProjectStatus::Staffing;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn pause(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if self.status != ProjectStatus::Active {
            return Err(DomainError::Validation(
                "Invalid project pause transition".into(),
            ));
        }
        self.status = ProjectStatus::Paused;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn resume(&mut self, expected_version: i64) -> Result<(), DomainError> {
        self.activate(expected_version)
    }

    pub fn complete(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if self.status != ProjectStatus::Active {
            return Err(DomainError::Validation(
                "Project must be active to complete".into(),
            ));
        }
        self.status = ProjectStatus::Completed;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn cancel(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if !matches!(
            self.status,
            ProjectStatus::Draft
                | ProjectStatus::Staffing
                | ProjectStatus::Active
                | ProjectStatus::Paused
        ) {
            return Err(DomainError::Validation(
                "Cannot cancel project from terminal or archived status".into(),
            ));
        }
        self.status = ProjectStatus::Cancelled;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn archive(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if !matches!(
            self.status,
            ProjectStatus::Completed | ProjectStatus::Cancelled
        ) {
            return Err(DomainError::Validation(
                "Only completed or cancelled projects can be archived".into(),
            ));
        }
        self.status = ProjectStatus::Archived;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn bind_working_root(
        &mut self,
        path: String,
        expected_version: i64,
    ) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        let path = required("Working root path", path)?;
        let p = std::path::Path::new(&path);
        if !p.exists() || !p.is_dir() {
            return Err(DomainError::Validation(
                "Working root directory must exist".into(),
            ));
        }
        let canonical = p
            .canonicalize()
            .map_err(|e| DomainError::Validation(format!("Invalid working root: {e}")))?;
        self.working_root_path = Some(canonical.to_string_lossy().to_string());
        self.working_root_bound_at = Some(Utc::now());
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn unbind_working_root(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        self.working_root_path = None;
        self.working_root_bound_at = None;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ObjectiveStatus {
    Draft,
    Active,
    Achieved,
    Failed,
    Cancelled,
    Archived,
}

impl fmt::Display for ObjectiveStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Draft => write!(f, "DRAFT"),
            Self::Active => write!(f, "ACTIVE"),
            Self::Achieved => write!(f, "ACHIEVED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
            Self::Archived => write!(f, "ARCHIVED"),
        }
    }
}

impl std::str::FromStr for ObjectiveStatus {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DRAFT" => Ok(Self::Draft),
            "ACTIVE" => Ok(Self::Active),
            "ACHIEVED" => Ok(Self::Achieved),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            "ARCHIVED" => Ok(Self::Archived),
            other => Err(DomainError::Validation(format!(
                "Invalid objective status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Objective {
    pub id: String,
    pub company_id: CompanyId,
    pub project_id: String,
    pub parent_objective_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub is_primary: bool,
    pub is_required: bool,
    pub status: ObjectiveStatus,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Objective {
    pub fn activate(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        transition(
            &mut self.status,
            ObjectiveStatus::Draft,
            ObjectiveStatus::Active,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }

    pub fn finish(
        &mut self,
        status: ObjectiveStatus,
        expected_version: i64,
    ) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if !matches!(
            status,
            ObjectiveStatus::Achieved | ObjectiveStatus::Failed | ObjectiveStatus::Cancelled
        ) {
            return Err(DomainError::Validation(
                "Invalid objective terminal status".into(),
            ));
        }
        transition(
            &mut self.status,
            ObjectiveStatus::Active,
            status,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }

    pub fn archive(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if !matches!(
            self.status,
            ObjectiveStatus::Achieved | ObjectiveStatus::Failed | ObjectiveStatus::Cancelled
        ) {
            return Err(DomainError::Validation(
                "Only terminal objectives can be archived".into(),
            ));
        }
        let previous = self.status;
        transition(
            &mut self.status,
            previous,
            ObjectiveStatus::Archived,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TeamStatus {
    Forming,
    Active,
    Paused,
    Disbanded,
    Archived,
}

impl fmt::Display for TeamStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Forming => write!(f, "FORMING"),
            Self::Active => write!(f, "ACTIVE"),
            Self::Paused => write!(f, "PAUSED"),
            Self::Disbanded => write!(f, "DISBANDED"),
            Self::Archived => write!(f, "ARCHIVED"),
        }
    }
}

impl std::str::FromStr for TeamStatus {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "FORMING" => Ok(Self::Forming),
            "ACTIVE" => Ok(Self::Active),
            "PAUSED" => Ok(Self::Paused),
            "DISBANDED" => Ok(Self::Disbanded),
            "ARCHIVED" => Ok(Self::Archived),
            other => Err(DomainError::Validation(format!(
                "Invalid team status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Team {
    pub id: String,
    pub company_id: CompanyId,
    pub project_id: String,
    pub name: String,
    pub is_primary: bool,
    pub status: TeamStatus,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StaffingRequirementStatus {
    Draft,
    Open,
    PartiallyFilled,
    Filled,
    Blocked,
    Cancelled,
}

impl fmt::Display for StaffingRequirementStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Draft => write!(f, "DRAFT"),
            Self::Open => write!(f, "OPEN"),
            Self::PartiallyFilled => write!(f, "PARTIALLY_FILLED"),
            Self::Filled => write!(f, "FILLED"),
            Self::Blocked => write!(f, "BLOCKED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

impl std::str::FromStr for StaffingRequirementStatus {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DRAFT" => Ok(Self::Draft),
            "OPEN" => Ok(Self::Open),
            "PARTIALLY_FILLED" => Ok(Self::PartiallyFilled),
            "FILLED" => Ok(Self::Filled),
            "BLOCKED" => Ok(Self::Blocked),
            "CANCELLED" => Ok(Self::Cancelled),
            other => Err(DomainError::Validation(format!(
                "Invalid staffing status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaffingRequirement {
    pub id: String,
    pub company_id: CompanyId,
    pub project_id: String,
    pub team_id: Option<String>,
    pub role_id: String,
    pub department_id: Option<String>,
    pub desired_count: u32,
    pub required_capability_ids: Vec<String>,
    pub status: StaffingRequirementStatus,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl StaffingRequirement {
    pub fn open(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.status,
            StaffingRequirementStatus::Draft,
            StaffingRequirementStatus::Open,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }

    pub fn block(&mut self) -> Result<(), DomainError> {
        if !matches!(
            self.status,
            StaffingRequirementStatus::Draft
                | StaffingRequirementStatus::Open
                | StaffingRequirementStatus::PartiallyFilled
                | StaffingRequirementStatus::Filled
        ) {
            return Err(DomainError::Validation(
                "Invalid staffing requirement block transition".into(),
            ));
        }
        self.status = StaffingRequirementStatus::Blocked;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn unblock(&mut self) -> Result<(), DomainError> {
        transition(
            &mut self.status,
            StaffingRequirementStatus::Blocked,
            StaffingRequirementStatus::Open,
            &mut self.row_version,
            &mut self.updated_at,
        )
    }

    pub fn cancel(&mut self) -> Result<(), DomainError> {
        if self.status == StaffingRequirementStatus::Cancelled {
            return Err(DomainError::Validation(
                "Staffing requirement is already cancelled".into(),
            ));
        }
        self.status = StaffingRequirementStatus::Cancelled;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn reconcile(&mut self, active_allocations: u32) {
        let status = match self.status {
            StaffingRequirementStatus::Draft
            | StaffingRequirementStatus::Blocked
            | StaffingRequirementStatus::Cancelled => return,
            _ if active_allocations == 0 => StaffingRequirementStatus::Open,
            _ if active_allocations < self.desired_count => {
                StaffingRequirementStatus::PartiallyFilled
            }
            _ => StaffingRequirementStatus::Filled,
        };
        if self.status != status {
            self.status = status;
            self.row_version += 1;
            self.updated_at = Utc::now();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentAllocationStatus {
    Planned,
    Active,
    Paused,
    Released,
    Cancelled,
}

impl fmt::Display for AgentAllocationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Planned => write!(f, "PLANNED"),
            Self::Active => write!(f, "ACTIVE"),
            Self::Paused => write!(f, "PAUSED"),
            Self::Released => write!(f, "RELEASED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

impl std::str::FromStr for AgentAllocationStatus {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PLANNED" => Ok(Self::Planned),
            "ACTIVE" => Ok(Self::Active),
            "PAUSED" => Ok(Self::Paused),
            "RELEASED" => Ok(Self::Released),
            "CANCELLED" => Ok(Self::Cancelled),
            other => Err(DomainError::Validation(format!(
                "Invalid allocation status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAllocation {
    pub id: String,
    pub company_id: CompanyId,
    pub project_id: String,
    pub team_id: String,
    pub agent_id: String,
    pub staffing_requirement_id: Option<String>,
    pub status: AgentAllocationStatus,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub released_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkItemType {
    Task,
    Research,
    Review,
    Deliverable,
    Decision,
    Maintenance,
    Incident,
}

impl fmt::Display for WorkItemType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task => write!(f, "TASK"),
            Self::Research => write!(f, "RESEARCH"),
            Self::Review => write!(f, "REVIEW"),
            Self::Deliverable => write!(f, "DELIVERABLE"),
            Self::Decision => write!(f, "DECISION"),
            Self::Maintenance => write!(f, "MAINTENANCE"),
            Self::Incident => write!(f, "INCIDENT"),
        }
    }
}

impl std::str::FromStr for WorkItemType {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "TASK" => Ok(Self::Task),
            "RESEARCH" => Ok(Self::Research),
            "REVIEW" => Ok(Self::Review),
            "DELIVERABLE" => Ok(Self::Deliverable),
            "DECISION" => Ok(Self::Decision),
            "MAINTENANCE" => Ok(Self::Maintenance),
            "INCIDENT" => Ok(Self::Incident),
            other => Err(DomainError::Validation(format!(
                "Invalid work item type: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkItemStatus {
    Backlog,
    Ready,
    InProgress,
    Blocked,
    WaitingApproval,
    Completed,
    Failed,
    Cancelled,
}

impl fmt::Display for WorkItemStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backlog => write!(f, "BACKLOG"),
            Self::Ready => write!(f, "READY"),
            Self::InProgress => write!(f, "IN_PROGRESS"),
            Self::Blocked => write!(f, "BLOCKED"),
            Self::WaitingApproval => write!(f, "WAITING_APPROVAL"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

impl WorkItemStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl std::str::FromStr for WorkItemStatus {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BACKLOG" => Ok(Self::Backlog),
            "READY" => Ok(Self::Ready),
            "IN_PROGRESS" => Ok(Self::InProgress),
            "BLOCKED" => Ok(Self::Blocked),
            "WAITING_APPROVAL" => Ok(Self::WaitingApproval),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            other => Err(DomainError::Validation(format!(
                "Invalid work item status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItem {
    pub id: String,
    pub company_id: CompanyId,
    pub project_id: String,
    pub objective_id: Option<String>,
    pub parent_work_item_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub work_type: WorkItemType,
    pub status: WorkItemStatus,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl WorkItem {
    pub fn create(
        company_id: CompanyId,
        project_id: String,
        title: String,
    ) -> Result<Self, DomainError> {
        let title = required("Work title", title)?;
        let now = Utc::now();
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            company_id,
            project_id,
            objective_id: None,
            parent_work_item_id: None,
            title,
            description: None,
            work_type: WorkItemType::Task,
            status: WorkItemStatus::Backlog,
            row_version: 1,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn ready(
        &mut self,
        expected_version: i64,
        has_unresolved_hard_dep: bool,
    ) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if has_unresolved_hard_dep {
            return Err(DomainError::Validation(
                "Cannot transition to READY with unresolved HARD dependencies".into(),
            ));
        }
        if self.status != WorkItemStatus::Backlog {
            return Err(DomainError::Validation(
                "Can only mark BACKLOG work item as READY".into(),
            ));
        }
        self.status = WorkItemStatus::Ready;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn start(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if self.status != WorkItemStatus::Ready {
            return Err(DomainError::Validation(
                "Can only start READY work item".into(),
            ));
        }
        self.status = WorkItemStatus::InProgress;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn complete(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if self.status != WorkItemStatus::InProgress {
            return Err(DomainError::Validation(
                "Can only complete IN_PROGRESS work item".into(),
            ));
        }
        self.status = WorkItemStatus::Completed;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn fail(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if self.status != WorkItemStatus::InProgress {
            return Err(DomainError::Validation(
                "Can only fail IN_PROGRESS work item".into(),
            ));
        }
        self.status = WorkItemStatus::Failed;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn cancel(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if self.status.is_terminal() {
            return Err(DomainError::Validation(
                "Cannot cancel terminal work item".into(),
            ));
        }
        self.status = WorkItemStatus::Cancelled;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn block(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if self.status != WorkItemStatus::InProgress {
            return Err(DomainError::Validation(
                "Can only block IN_PROGRESS work item".into(),
            ));
        }
        self.status = WorkItemStatus::Blocked;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn unblock(&mut self, expected_version: i64) -> Result<(), DomainError> {
        check_version(self.row_version, expected_version)?;
        if self.status != WorkItemStatus::Blocked {
            return Err(DomainError::Validation(
                "Can only unblock BLOCKED work item".into(),
            ));
        }
        self.status = WorkItemStatus::InProgress;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DependencyType {
    Hard,
    Soft,
}

impl fmt::Display for DependencyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hard => write!(f, "HARD"),
            Self::Soft => write!(f, "SOFT"),
        }
    }
}

impl std::str::FromStr for DependencyType {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "HARD" => Ok(Self::Hard),
            "SOFT" => Ok(Self::Soft),
            other => Err(DomainError::Validation(format!(
                "Invalid dependency type: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkDependency {
    pub id: String,
    pub company_id: CompanyId,
    pub project_id: String,
    pub work_item_id: String,
    pub depends_on_work_item_id: String,
    pub dependency_type: DependencyType,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AssignmentStatus {
    Active,
    Released,
    Cancelled,
}

impl fmt::Display for AssignmentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "ACTIVE"),
            Self::Released => write!(f, "RELEASED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

impl std::str::FromStr for AssignmentStatus {
    type Err = DomainError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ACTIVE" => Ok(Self::Active),
            "RELEASED" => Ok(Self::Released),
            "CANCELLED" => Ok(Self::Cancelled),
            other => Err(DomainError::Validation(format!(
                "Invalid assignment status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkAssignment {
    pub id: String,
    pub company_id: CompanyId,
    pub project_id: String,
    pub work_item_id: String,
    pub agent_id: String,
    pub is_primary: bool,
    pub status: AssignmentStatus,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub released_at: Option<DateTime<Utc>>,
}

pub fn would_create_cycle(existing_edges: &[(&str, &str)], from_id: &str, to_id: &str) -> bool {
    if from_id == to_id {
        return true;
    }
    use std::collections::{HashMap, HashSet, VecDeque};
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for (u, v) in existing_edges {
        adj.entry(u).or_default().push(v);
    }
    adj.entry(from_id).or_default().push(to_id);

    let mut q = VecDeque::new();
    let mut visited = HashSet::new();
    q.push_back(to_id);
    visited.insert(to_id);

    while let Some(curr) = q.pop_front() {
        if curr == from_id {
            return true;
        }
        if let Some(nexts) = adj.get(curr) {
            for &n in nexts {
                if visited.insert(n) {
                    q.push_back(n);
                }
            }
        }
    }
    false
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunStatus {
    Queued,
    Running,
    Paused,
    WaitingApproval,
    WaitingDependency,
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::TimedOut | Self::Cancelled
        )
    }
}

impl fmt::Display for RunStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Queued => "QUEUED",
            Self::Running => "RUNNING",
            Self::Paused => "PAUSED",
            Self::WaitingApproval => "WAITING_APPROVAL",
            Self::WaitingDependency => "WAITING_DEPENDENCY",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::TimedOut => "TIMED_OUT",
            Self::Cancelled => "CANCELLED",
        };
        f.write_str(value)
    }
}

impl std::str::FromStr for RunStatus {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "QUEUED" => Ok(Self::Queued),
            "RUNNING" => Ok(Self::Running),
            "PAUSED" => Ok(Self::Paused),
            "WAITING_APPROVAL" => Ok(Self::WaitingApproval),
            "WAITING_DEPENDENCY" => Ok(Self::WaitingDependency),
            "SUCCEEDED" => Ok(Self::Succeeded),
            "FAILED" => Ok(Self::Failed),
            "TIMED_OUT" => Ok(Self::TimedOut),
            "CANCELLED" => Ok(Self::Cancelled),
            other => Err(DomainError::Validation(format!(
                "Invalid run status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: String,
    pub company_id: CompanyId,
    pub project_id: String,
    pub work_item_id: String,
    pub assignment_id: Option<String>,
    pub executing_agent_id: String,
    pub status: RunStatus,
    pub trigger_type: String,
    pub attempt_number: u32,
    pub retry_of_run_id: Option<String>,
    pub model_profile_version_id: Option<String>,
    pub requested_by: PrincipalRef,
    pub queued_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failure_class: Option<String>,
    pub failure_detail: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub row_version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Run {
    pub fn create(
        company_id: CompanyId,
        project_id: String,
        work_item_id: String,
        executing_agent_id: String,
        trigger_type: String,
        requested_by: PrincipalRef,
        correlation_id: String,
    ) -> Result<Self, DomainError> {
        if company_id.0.trim().is_empty() || requested_by.principal_id.trim().is_empty() {
            return Err(DomainError::Validation(
                "Run company and requester cannot be empty".into(),
            ));
        }
        let now = Utc::now();
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            company_id,
            project_id: required("Run project", project_id)?,
            work_item_id: required("Run work item", work_item_id)?,
            assignment_id: None,
            executing_agent_id: required("Run executing agent", executing_agent_id)?,
            status: RunStatus::Queued,
            trigger_type: required("Run trigger type", trigger_type)?,
            attempt_number: 1,
            retry_of_run_id: None,
            model_profile_version_id: None,
            requested_by,
            queued_at: now,
            started_at: None,
            completed_at: None,
            failure_class: None,
            failure_detail: None,
            correlation_id: required("Run correlation", correlation_id)?,
            causation_id: None,
            row_version: 1,
            created_at: now,
            updated_at: now,
        })
    }

    pub fn start(&mut self) -> Result<(), DomainError> {
        self.transition(RunStatus::Queued, RunStatus::Running)?;
        self.started_at = Some(self.updated_at);
        Ok(())
    }

    pub fn pause(&mut self) -> Result<(), DomainError> {
        self.transition(RunStatus::Running, RunStatus::Paused)
    }

    pub fn wait_for_approval(&mut self) -> Result<(), DomainError> {
        self.transition(RunStatus::Running, RunStatus::WaitingApproval)
    }

    pub fn wait_for_dependency(&mut self) -> Result<(), DomainError> {
        self.transition(RunStatus::Running, RunStatus::WaitingDependency)
    }

    pub fn resume(&mut self) -> Result<(), DomainError> {
        if !matches!(
            self.status,
            RunStatus::Paused | RunStatus::WaitingApproval | RunStatus::WaitingDependency
        ) {
            return Err(DomainError::Validation(
                "Only paused or waiting runs can resume".into(),
            ));
        }
        self.status = RunStatus::Running;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    pub fn cancel(&mut self) -> Result<(), DomainError> {
        if !matches!(
            self.status,
            RunStatus::Queued
                | RunStatus::Running
                | RunStatus::Paused
                | RunStatus::WaitingApproval
                | RunStatus::WaitingDependency
        ) {
            return Err(DomainError::Validation("Cannot cancel terminal run".into()));
        }
        self.status = RunStatus::Cancelled;
        self.finish();
        Ok(())
    }

    pub fn succeed(&mut self) -> Result<(), DomainError> {
        self.terminal(RunStatus::Succeeded, None, None)
    }

    pub fn fail(
        &mut self,
        failure_class: String,
        failure_detail: String,
    ) -> Result<(), DomainError> {
        self.terminal(
            RunStatus::Failed,
            Some(required("Run failure class", failure_class)?),
            Some(required("Run failure detail", failure_detail)?),
        )
    }

    pub fn reject_admission(
        &mut self,
        failure_class: String,
        failure_detail: String,
    ) -> Result<(), DomainError> {
        if self.status != RunStatus::Queued {
            return Err(DomainError::Validation(
                "Only queued runs can be rejected at admission".into(),
            ));
        }
        self.failure_class = Some(required("Run failure class", failure_class)?);
        self.failure_detail = Some(required("Run failure detail", failure_detail)?);
        self.status = RunStatus::Failed;
        self.finish();
        Ok(())
    }

    pub fn time_out(
        &mut self,
        failure_class: String,
        failure_detail: String,
    ) -> Result<(), DomainError> {
        self.terminal(
            RunStatus::TimedOut,
            Some(required("Run timeout class", failure_class)?),
            Some(required("Run timeout detail", failure_detail)?),
        )
    }

    pub fn retry(&self) -> Result<Self, DomainError> {
        if !matches!(self.status, RunStatus::Failed | RunStatus::TimedOut) {
            return Err(DomainError::Validation(
                "Only failed or timed out runs can be retried".into(),
            ));
        }
        let now = Utc::now();
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            company_id: self.company_id.clone(),
            project_id: self.project_id.clone(),
            work_item_id: self.work_item_id.clone(),
            assignment_id: self.assignment_id.clone(),
            executing_agent_id: self.executing_agent_id.clone(),
            status: RunStatus::Queued,
            trigger_type: self.trigger_type.clone(),
            attempt_number: self
                .attempt_number
                .checked_add(1)
                .ok_or_else(|| DomainError::Validation("Run attempt number overflow".into()))?,
            retry_of_run_id: Some(self.id.clone()),
            model_profile_version_id: self.model_profile_version_id.clone(),
            requested_by: self.requested_by.clone(),
            queued_at: now,
            started_at: None,
            completed_at: None,
            failure_class: None,
            failure_detail: None,
            correlation_id: self.correlation_id.clone(),
            causation_id: Some(self.id.clone()),
            row_version: 1,
            created_at: now,
            updated_at: now,
        })
    }

    fn terminal(
        &mut self,
        status: RunStatus,
        failure_class: Option<String>,
        failure_detail: Option<String>,
    ) -> Result<(), DomainError> {
        if self.status != RunStatus::Running {
            return Err(DomainError::Validation(
                "Only running runs can reach a terminal outcome".into(),
            ));
        }
        self.status = status;
        self.failure_class = failure_class;
        self.failure_detail = failure_detail;
        self.finish();
        Ok(())
    }

    fn transition(&mut self, from: RunStatus, to: RunStatus) -> Result<(), DomainError> {
        if self.status != from {
            return Err(DomainError::Validation(
                "Invalid run lifecycle transition".into(),
            ));
        }
        self.status = to;
        self.row_version += 1;
        self.updated_at = Utc::now();
        Ok(())
    }

    fn finish(&mut self) {
        let now = Utc::now();
        self.completed_at = Some(now);
        self.row_version += 1;
        self.updated_at = now;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionStepStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Skipped,
}

impl ExecutionStepStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Skipped
        )
    }
}

impl fmt::Display for ExecutionStepStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::Skipped => "SKIPPED",
        };
        f.write_str(value)
    }
}

impl std::str::FromStr for ExecutionStepStatus {
    type Err = DomainError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "SUCCEEDED" => Ok(Self::Succeeded),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            "SKIPPED" => Ok(Self::Skipped),
            other => Err(DomainError::Validation(format!(
                "Invalid execution step status: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionStep {
    pub id: String,
    pub company_id: CompanyId,
    pub run_id: String,
    pub sequence_no: u32,
    pub step_type: String,
    pub status: ExecutionStepStatus,
    pub parent_step_id: Option<String>,
    pub input_metadata: Option<serde_json::Value>,
    pub output_metadata: Option<serde_json::Value>,
    pub failure_class: Option<String>,
    pub failure_detail: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl ExecutionStep {
    pub fn create(
        company_id: CompanyId,
        run_id: String,
        sequence_no: u32,
        step_type: String,
    ) -> Result<Self, DomainError> {
        if company_id.0.trim().is_empty() || sequence_no == 0 {
            return Err(DomainError::Validation(
                "Execution step company and sequence must be valid".into(),
            ));
        }
        Ok(Self {
            id: Uuid::now_v7().to_string(),
            company_id,
            run_id: required("Execution step run", run_id)?,
            sequence_no,
            step_type: required("Execution step type", step_type)?,
            status: ExecutionStepStatus::Pending,
            parent_step_id: None,
            input_metadata: None,
            output_metadata: None,
            failure_class: None,
            failure_detail: None,
            started_at: None,
            completed_at: None,
            created_at: Utc::now(),
        })
    }

    pub fn start(&mut self) -> Result<(), DomainError> {
        if self.status != ExecutionStepStatus::Pending {
            return Err(DomainError::Validation(
                "Can only start pending execution step".into(),
            ));
        }
        self.status = ExecutionStepStatus::Running;
        self.started_at = Some(Utc::now());
        Ok(())
    }

    pub fn succeed(&mut self, output_metadata: serde_json::Value) -> Result<(), DomainError> {
        self.finish(
            ExecutionStepStatus::Succeeded,
            Some(output_metadata),
            None,
            None,
        )
    }

    pub fn fail(
        &mut self,
        failure_class: String,
        failure_detail: String,
    ) -> Result<(), DomainError> {
        self.finish(
            ExecutionStepStatus::Failed,
            None,
            Some(required("Execution step failure class", failure_class)?),
            Some(required("Execution step failure detail", failure_detail)?),
        )
    }

    pub fn cancel(&mut self) -> Result<(), DomainError> {
        if self.status.is_terminal() {
            return Err(DomainError::Validation(
                "Cannot cancel terminal execution step".into(),
            ));
        }
        self.status = ExecutionStepStatus::Cancelled;
        self.completed_at = Some(Utc::now());
        Ok(())
    }

    pub fn skip(&mut self) -> Result<(), DomainError> {
        if self.status != ExecutionStepStatus::Pending {
            return Err(DomainError::Validation(
                "Only pending execution steps can be skipped".into(),
            ));
        }
        self.status = ExecutionStepStatus::Skipped;
        self.completed_at = Some(Utc::now());
        Ok(())
    }

    fn finish(
        &mut self,
        status: ExecutionStepStatus,
        output_metadata: Option<serde_json::Value>,
        failure_class: Option<String>,
        failure_detail: Option<String>,
    ) -> Result<(), DomainError> {
        if self.status != ExecutionStepStatus::Running {
            return Err(DomainError::Validation(
                "Only running execution steps can finish".into(),
            ));
        }
        self.status = status;
        self.output_metadata = output_metadata;
        self.failure_class = failure_class;
        self.failure_detail = failure_detail;
        self.completed_at = Some(Utc::now());
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
