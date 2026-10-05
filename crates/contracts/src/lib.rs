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

fn default_priority() -> String {
    "NORMAL".into()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectDto {
    pub id: String,
    pub company_id: String,
    pub name: String,
    pub description: Option<String>,
    #[serde(default = "default_priority")]
    pub priority: String,
    #[serde(default)]
    pub owner_user_id: Option<String>,
    #[serde(default)]
    pub target_outcome: Option<String>,
    #[serde(default)]
    pub target_date: Option<String>,
    pub working_root_path: Option<String>,
    pub working_root_bound_at: Option<String>,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateProjectRequest {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectListResponse {
    pub projects: Vec<ProjectDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectLifecycleRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindProjectWorkingRootRequest {
    pub path: String,
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnbindProjectWorkingRootRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveDto {
    pub id: String,
    pub company_id: String,
    pub project_id: String,
    pub parent_objective_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub is_primary: bool,
    pub is_required: bool,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveListResponse {
    pub objectives: Vec<ObjectiveDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateObjectiveRequest {
    pub parent_objective_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub is_primary: bool,
    pub is_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectiveLifecycleRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamDto {
    pub id: String,
    pub company_id: String,
    pub project_id: String,
    pub name: String,
    pub is_primary: bool,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamListResponse {
    pub teams: Vec<TeamDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTeamRequest {
    pub name: String,
    pub is_primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamLifecycleRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaffingRequirementDto {
    pub id: String,
    pub company_id: String,
    pub project_id: String,
    pub team_id: Option<String>,
    pub role_id: String,
    pub department_id: Option<String>,
    pub desired_count: u32,
    pub required_capability_ids: Vec<String>,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaffingRequirementListResponse {
    pub staffing_requirements: Vec<StaffingRequirementDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStaffingRequirementRequest {
    pub team_id: Option<String>,
    pub role_id: String,
    pub department_id: Option<String>,
    pub desired_count: u32,
    pub required_capability_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StaffingLifecycleRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAllocationDto {
    pub id: String,
    pub company_id: String,
    pub project_id: String,
    pub team_id: String,
    pub agent_id: String,
    pub staffing_requirement_id: Option<String>,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
    pub released_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentAllocationListResponse {
    pub allocations: Vec<AgentAllocationDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateAgentAllocationRequest {
    pub team_id: String,
    pub agent_id: String,
    pub staffing_requirement_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AllocationLifecycleRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItemDto {
    pub id: String,
    pub company_id: String,
    pub project_id: String,
    pub objective_id: Option<String>,
    pub parent_work_item_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub work_type: String,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkListResponse {
    pub work_items: Vec<WorkItemDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWorkItemRequest {
    pub objective_id: Option<String>,
    pub parent_work_item_id: Option<String>,
    pub title: String,
    pub description: Option<String>,
    pub work_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkItemLifecycleRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkDependencyDto {
    pub id: String,
    pub company_id: String,
    pub project_id: String,
    pub work_item_id: String,
    pub depends_on_work_item_id: String,
    pub dependency_type: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkDependencyListResponse {
    pub dependencies: Vec<WorkDependencyDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWorkDependencyRequest {
    pub depends_on_work_item_id: String,
    pub dependency_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkAssignmentDto {
    pub id: String,
    pub company_id: String,
    pub project_id: String,
    pub work_item_id: String,
    pub agent_id: String,
    pub agent_allocation_id: String,
    pub is_primary: bool,
    pub status: String,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
    pub released_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkAssignmentListResponse {
    pub assignments: Vec<WorkAssignmentDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateWorkAssignmentRequest {
    pub agent_id: String,
    pub agent_allocation_id: String,
    pub is_primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentLifecycleRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunDto {
    pub id: String,
    pub company_id: String,
    pub project_id: String,
    pub work_item_id: String,
    pub assignment_id: Option<String>,
    pub executing_agent_id: String,
    pub lifecycle_state: String,
    pub trigger_type: String,
    pub attempt_number: i64,
    pub retry_of_run_id: Option<String>,
    pub model_profile_version_id: Option<String>,
    pub requested_by_type: String,
    pub requested_by_id: String,
    pub queued_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub failure_class: Option<String>,
    pub failure_detail: Option<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub row_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateRunRequest {
    pub work_item_id: String,
    pub assignment_id: Option<String>,
    pub executing_agent_id: String,
    pub trigger_type: String,
    pub retry_of_run_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunListResponse {
    pub runs: Vec<RunDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunShowResponse {
    pub run: RunDto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRunRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PauseRunRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeRunRequest {
    pub expected_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelRunRequest {
    pub expected_version: i64,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandAcceptedResponse {
    pub run_id: String,
    pub command: String,
    pub accepted_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionStepDto {
    pub id: String,
    pub company_id: String,
    pub run_id: String,
    pub sequence_no: i64,
    pub step_type: String,
    pub lifecycle_state: String,
    pub parent_step_id: Option<String>,
    pub input_metadata: Option<serde_json::Value>,
    pub output_metadata: Option<serde_json::Value>,
    pub failure_class: Option<String>,
    pub failure_detail: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionStepListResponse {
    pub steps: Vec<ExecutionStepDto>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeResultDto {
    pub id: String,
    pub company_id: String,
    pub run_id: String,
    pub run_status: String,
    pub result_summary: String,
    pub output_payload: Option<serde_json::Value>,
    pub output_metadata: Option<serde_json::Value>,
    pub resource_usage_summary: Option<serde_json::Value>,
    pub failure_class: Option<String>,
    pub failure_detail: Option<String>,
    pub warnings: Vec<String>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeResultResponse {
    pub result: RuntimeResultDto,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageRecordDto {
    pub id: String,
    pub workspace_id: String,
    pub company_id: String,
    pub project_id: String,
    pub work_item_id: String,
    pub agent_id: String,
    pub run_id: String,
    pub step_id: Option<String>,
    pub provider_connection_id: String,
    pub model_id: String,
    pub usage_type: String,
    pub quantity: i64,
    pub unit: String,
    pub estimated_cost: Option<f64>,
    pub occurred_at: String,
    pub metadata: Option<serde_json::Value>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageRecordListResponse {
    pub usage_records: Vec<UsageRecordDto>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimelineEventDto {
    pub event_id: String,
    pub run_id: String,
    pub sequence_no: i64,
    pub event_type: String,
    pub occurred_at: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimelineResponse {
    pub events: Vec<TimelineEventDto>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    tag = "event_type",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
pub enum NormalizedModelEvent {
    Started {
        run_id: String,
        step_id: String,
        invocation_index: i64,
    },
    OutputDelta {
        run_id: String,
        step_id: String,
        text: String,
    },
    Usage {
        run_id: String,
        step_id: String,
        input_tokens: i64,
        output_tokens: i64,
    },
    Completed {
        run_id: String,
        step_id: String,
        finish_reason: String,
    },
    Failed {
        run_id: String,
        step_id: String,
        failure_class: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerExecutionRequest {
    pub run_id: String,
    pub company_id: String,
    pub project_id: String,
    pub work_item_id: String,
    pub executing_agent_id: String,
    pub model_profile_version_id: Option<String>,
    pub correlation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerLeaseRequest {
    pub run_id: String,
    pub worker_principal_id: String,
    pub lease_duration_seconds: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLeaseResponse {
    pub lease_id: String,
    pub run_id: String,
    pub lease_version: i64,
    pub expires_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerHeartbeatRequest {
    pub run_id: String,
    pub lease_id: String,
    pub lease_version: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerStepRequest {
    pub run_id: String,
    pub lease_id: String,
    pub lease_version: i64,
    pub sequence_no: i64,
    pub step_type: String,
    pub lifecycle_state: String,
    pub parent_step_id: Option<String>,
    pub input_metadata: Option<serde_json::Value>,
    pub output_metadata: Option<serde_json::Value>,
    pub failure_class: Option<String>,
    pub failure_detail: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerCheckpointRequest {
    pub run_id: String,
    pub lease_id: String,
    pub lease_version: i64,
    pub checkpoint_version: i64,
    pub run_state: String,
    pub last_completed_step: Option<i64>,
    pub active_step: Option<i64>,
    pub execution_phase: String,
    pub context_refs: Vec<String>,
    pub continuation_metadata: Option<serde_json::Value>,
    pub usage_snapshot: Option<serde_json::Value>,
    pub safe_to_resume: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerResultRequest {
    pub run_id: String,
    pub lease_id: String,
    pub lease_version: i64,
    pub run_status: String,
    pub result_summary: String,
    pub output_payload: Option<serde_json::Value>,
    pub output_metadata: Option<serde_json::Value>,
    pub resource_usage_summary: Option<serde_json::Value>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerTerminalRequest {
    pub run_id: String,
    pub lease_id: String,
    pub lease_version: i64,
    pub lifecycle_state: String,
    pub failure_class: Option<String>,
    pub failure_detail: Option<String>,
    pub completed_at: String,
}
