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
