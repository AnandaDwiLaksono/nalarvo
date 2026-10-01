use nalarvo_contracts::*;
use serde_json::{Value, json};

fn round_trip<T>(value: Value) -> Value
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    serde_json::to_value(serde_json::from_value::<T>(value).unwrap()).unwrap()
}

#[test]
fn project_contracts_cover_metadata_lifecycle_and_working_root() {
    let project = round_trip::<ProjectDto>(json!({
        "id":"p","company_id":"c","name":"Build","description":null,
        "priority":"HIGH","owner_user_id":"u","target_outcome":"Ship",
        "target_date":"2026-12-31","working_root_path":"C:/work",
        "working_root_bound_at":"now","status":"STAFFING","row_version":2,
        "created_at":"now","updated_at":"now"
    }));
    assert_eq!(project["priority"], "HIGH");
    assert_eq!(
        round_trip::<ProjectListResponse>(json!({"projects":[project]}))["projects"][0]["id"],
        "p"
    );
    round_trip::<CreateProjectRequest>(json!({"name":"Build","description":null}));
    round_trip::<ProjectLifecycleRequest>(json!({"expected_version":2}));
    round_trip::<BindProjectWorkingRootRequest>(json!({
        "path":"C:/work","expected_version":2
    }));
}

#[test]
fn objective_team_staffing_and_allocation_contracts_are_first_class() {
    let objective = round_trip::<ObjectiveDto>(json!({
        "id":"o","company_id":"c","project_id":"p","parent_objective_id":null,
        "title":"Ship","description":null,"is_primary":true,"is_required":true,
        "status":"DRAFT","row_version":1,"created_at":"now","updated_at":"now"
    }));
    assert_eq!(objective["is_primary"], true);
    round_trip::<ObjectiveListResponse>(json!({"objectives":[objective]}));
    round_trip::<CreateObjectiveRequest>(json!({
        "parent_objective_id":null,"title":"Ship","description":null,
        "is_primary":true,"is_required":true
    }));
    round_trip::<ObjectiveLifecycleRequest>(json!({"expected_version":1}));

    let team = round_trip::<TeamDto>(json!({
        "id":"t","company_id":"c","project_id":"p","name":"Core",
        "is_primary":true,"status":"FORMING","row_version":1,
        "created_at":"now","updated_at":"now"
    }));
    round_trip::<TeamListResponse>(json!({"teams":[team]}));
    round_trip::<CreateTeamRequest>(json!({"name":"Core","is_primary":true}));
    round_trip::<TeamLifecycleRequest>(json!({"expected_version":1}));

    let staffing = round_trip::<StaffingRequirementDto>(json!({
        "id":"s","company_id":"c","project_id":"p","team_id":"t",
        "role_id":"r","department_id":"d","desired_count":2,
        "required_capability_ids":["cap"],"status":"DRAFT","row_version":1,
        "created_at":"now","updated_at":"now"
    }));
    assert_eq!(staffing["status"], "DRAFT");
    round_trip::<StaffingRequirementListResponse>(json!({"staffing_requirements":[staffing]}));
    round_trip::<CreateStaffingRequirementRequest>(json!({
        "team_id":"t","role_id":"r","department_id":"d","desired_count":2,
        "required_capability_ids":["cap"]
    }));
    round_trip::<StaffingLifecycleRequest>(json!({"expected_version":1}));

    let allocation = round_trip::<AgentAllocationDto>(json!({
        "id":"a","company_id":"c","project_id":"p","team_id":"t",
        "agent_id":"agent","staffing_requirement_id":"s","status":"PLANNED",
        "row_version":1,"created_at":"now","updated_at":"now","released_at":null
    }));
    round_trip::<AgentAllocationListResponse>(json!({"allocations":[allocation]}));
    round_trip::<CreateAgentAllocationRequest>(json!({
        "team_id":"t","agent_id":"agent","staffing_requirement_id":"s"
    }));
    round_trip::<AllocationLifecycleRequest>(json!({"expected_version":1}));
}

#[test]
fn scoped_work_dependency_and_assignment_contracts_are_complete() {
    let work = round_trip::<WorkItemDto>(json!({
        "id":"w","company_id":"c","project_id":"p","objective_id":"o",
        "parent_work_item_id":null,"title":"Task","description":null,
        "work_type":"TASK","status":"BACKLOG","row_version":1,
        "created_at":"now","updated_at":"now"
    }));
    round_trip::<WorkListResponse>(json!({"work_items":[work]}));
    round_trip::<CreateWorkItemRequest>(json!({
        "objective_id":"o","parent_work_item_id":null,"title":"Task",
        "description":null,"work_type":"TASK"
    }));
    round_trip::<WorkItemLifecycleRequest>(json!({"expected_version":1}));

    let dependency = round_trip::<WorkDependencyDto>(json!({
        "id":"dep","company_id":"c","project_id":"p","work_item_id":"w",
        "depends_on_work_item_id":"w0","dependency_type":"HARD","created_at":"now"
    }));
    round_trip::<WorkDependencyListResponse>(json!({"dependencies":[dependency]}));
    round_trip::<CreateWorkDependencyRequest>(json!({
        "depends_on_work_item_id":"w0","dependency_type":"HARD"
    }));

    let assignment = round_trip::<WorkAssignmentDto>(json!({
        "id":"as","company_id":"c","project_id":"p","work_item_id":"w",
        "agent_id":"agent","agent_allocation_id":"alloc","is_primary":true,
        "status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now",
        "released_at":null
    }));
    assert_eq!(assignment["agent_allocation_id"], "alloc");
    round_trip::<WorkAssignmentListResponse>(json!({"assignments":[assignment]}));
    round_trip::<CreateWorkAssignmentRequest>(json!({
        "agent_id":"agent","agent_allocation_id":"alloc","is_primary":true
    }));
    round_trip::<AssignmentLifecycleRequest>(json!({"expected_version":1}));
}
