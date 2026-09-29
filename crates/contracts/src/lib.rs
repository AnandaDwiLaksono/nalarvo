use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub service: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompanyDto {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub description: Option<String>,
    pub mission: Option<String>,
    pub director_user_id: Option<String>,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateCompanyRequest {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateCompanyRequest {
    pub name: String,
    pub description: Option<String>,
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompanyListResponse {
    pub companies: Vec<CompanyDto>,
}

pub mod error_codes {
    pub const UNAUTHORIZED: &str = "UNAUTHORIZED";
    pub const FORBIDDEN: &str = "FORBIDDEN";
    pub const NOT_FOUND: &str = "NOT_FOUND";
    pub const VALIDATION_FAILED: &str = "VALIDATION_FAILED";
    pub const CONFLICT: &str = "CONFLICT";
    pub const STALE_VERSION: &str = "STALE_VERSION";
    pub const IDEMPOTENCY_KEY_REUSE_MISMATCH: &str = "IDEMPOTENCY_KEY_REUSE_MISMATCH";
    pub const INTERNAL_ERROR: &str = "INTERNAL_ERROR";
    pub const MIGRATION_ERROR: &str = "MIGRATION_ERROR";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub error: ErrorDetail,
}

impl ErrorEnvelope {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: ErrorDetail {
                code: code.into(),
                message: message.into(),
                details: None,
            },
        }
    }

    pub fn with_details(
        code: impl Into<String>,
        message: impl Into<String>,
        details: serde_json::Value,
    ) -> Self {
        Self {
            error: ErrorDetail {
                code: code.into(),
                message: message.into(),
                details: Some(details),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrincipalDto {
    pub principal_type: String,
    pub principal_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeDto {
    pub scope_type: String,
    pub scope_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SseEventEnvelope {
    pub event_id: String,
    pub event_type: String,
    pub schema_version: u32,
    pub company_id: String,
    pub principal: PrincipalDto,
    pub scope: ScopeDto,
    pub occurred_at: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceDto {
    pub id: String,
    pub owner_user_id: String,
    pub name: String,
    pub slug: String,
    pub is_personal: bool,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceListResponse {
    pub workspaces: Vec<WorkspaceDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRefDto {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmitCredentialRequest {
    pub name: String,
    pub secret: String,
}

impl SubmitCredentialRequest {
    pub fn new(name: String, secret: String) -> Self {
        Self { name, secret }
    }
}

impl std::fmt::Debug for SubmitCredentialRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmitCredentialRequest")
            .field("name", &self.name)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderConnectionDto {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub provider_kind: String,
    pub credential_ref_id: Option<String>,
    pub status: String,
    pub health: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderListResponse {
    pub providers: Vec<ProviderConnectionDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateProviderConnectionRequest {
    pub name: String,
    pub provider_kind: String,
    pub credential_ref_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateProviderConnectionRequest {
    pub name: String,
    pub credential_ref_id: Option<String>,
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderLifecycleRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderTestResponse {
    pub provider: ProviderConnectionDto,
    pub healthy: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelDto {
    pub id: String,
    pub workspace_id: String,
    pub provider_connection_id: String,
    pub model_key: String,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelListResponse {
    pub models: Vec<ModelDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelProfileDto {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub current_version: Option<i64>,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelProfileVersionDto {
    pub profile_id: String,
    pub workspace_id: String,
    pub version: i64,
    pub model_id: String,
    pub config_json: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelProfileVersionRequest {
    pub model_id: String,
    pub config_json: String,
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompanyResourceGrantDto {
    pub company_id: String,
    pub workspace_id: String,
    pub model_profile_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompanyLifecycleRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepartmentDto {
    pub id: String,
    pub company_id: String,
    pub name: String,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepartmentListResponse {
    pub departments: Vec<DepartmentDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleDto {
    pub id: String,
    pub company_id: String,
    pub name: String,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleListResponse {
    pub roles: Vec<RoleDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepartmentRoleDto {
    pub company_id: String,
    pub department_id: String,
    pub role_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateDepartmentRequest {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateRoleRequest {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignDepartmentRoleRequest {
    pub role_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDto {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentListResponse {
    pub agents: Vec<AgentDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateAgentRequest {
    pub name: String,
    pub primary_department_id: String,
    pub role_id: String,
    pub model_profile_id: Option<String>,
    pub capacity: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateAgentRequest {
    pub name: String,
    pub primary_department_id: String,
    pub role_id: String,
    pub model_profile_id: Option<String>,
    pub capacity: i64,
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkforceLifecycleRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialListResponse {
    pub credentials: Vec<CredentialRefDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAvailabilityResponse {
    pub availability: String,
}
