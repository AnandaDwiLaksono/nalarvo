//! M7 orchestration transport; authority always resolved in Core.
use nalarvo_domain::{AgentAllocation, Project, Run};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestrationRequest {
    pub team_id: String,
    pub idempotency_key: String,
    pub command: OrchestrationCommand,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum OrchestrationCommand {
    StaffTeam,
    ScheduleWork,
    ConfigureWork {
        work_item_id: String,
        requirements: WorkRequirements,
    },
    GenerateProjectPlan {
        planner_role: String,
        reason: String,
    },
    CollectPlanProposal {
        source_run_id: String,
        source_version_id: String,
    },
    DecidePlan {
        proposal_id: String,
        apply: bool,
        confirmed: bool,
        reason: String,
    },
    EvaluateWork {
        evaluation: EvaluationInput,
    },
    ReviseWork {
        previous_run_id: String,
        instructions: String,
        expected_version: i64,
    },
    CompleteProject {
        expected_version: i64,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkRequirements {
    pub role_id: String,
    pub priority: i64,
    pub required: bool,
    pub acceptance_contains: String,
    pub input_version_ids: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationInput {
    pub run_id: String,
    pub artifact_version_id: Option<String>,
    pub human_outcome: Option<EvaluationOutcome>,
    pub model_run_id: Option<String>,
    pub reason: String,
    pub accept: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EvaluationOutcome {
    Pass,
    Fail,
    NeedsRevision,
    Unsure,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaffingShortage {
    pub requirement_id: String,
    pub role_id: String,
    pub desired: u32,
    pub filled: u32,
    pub missing: u32,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "result")]
pub enum OrchestrationResponse {
    Staffing {
        allocations: Vec<AgentAllocation>,
        shortages: Vec<StaffingShortage>,
        team_active: bool,
    },
    Runs(Vec<Run>),
    Configured,
    Proposal(String),
    PlanApplied(Vec<String>),
    Evaluation {
        id: String,
        outcome: EvaluationOutcome,
        accepted: bool,
    },
    Revision(Run),
    Completed(Project),
}
