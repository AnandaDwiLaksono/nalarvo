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
            status: CompanyStatus::Draft,
            row_version: 1,
            created_at: now,
            updated_at: now,
        })
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
