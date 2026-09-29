use nalarvo_contracts::*;
use serde_json::{Value, json};

#[test]
fn workspace_resources_round_trip_without_readable_secret() {
    let workspace: WorkspaceDto = serde_json::from_value(json!({"id":"w","owner_user_id":"u","name":"Personal","slug":"personal","is_personal":true,"status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now"})).unwrap();
    assert_eq!(workspace.id, "w");
    let provider: ProviderConnectionDto = serde_json::from_value(json!({"id":"p","workspace_id":"w","name":"Provider","provider_kind":"openai","credential_ref_id":"c","status":"ACTIVE","health":"UNKNOWN","row_version":1,"created_at":"now","updated_at":"now"})).unwrap();
    assert_eq!(provider.credential_ref_id.as_deref(), Some("c"));
    let credential: CredentialRefDto = serde_json::from_value(json!({"id":"c","workspace_id":"w","name":"Key","status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now"})).unwrap();
    let value = serde_json::to_value(&credential).unwrap();
    assert!(value.get("secret").is_none());
    assert!(value.get("secret_locator").is_none());
    let model: ModelDto = serde_json::from_value(json!({"id":"m","workspace_id":"w","provider_connection_id":"p","model_key":"gpt-test","status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now"})).unwrap();
    assert_eq!(model.provider_connection_id, "p");
    let profile: ModelProfileDto = serde_json::from_value(json!({"id":"pr","workspace_id":"w","name":"Default","current_version":1,"status":"ACTIVE","row_version":2,"created_at":"now","updated_at":"now"})).unwrap();
    assert_eq!(profile.current_version, Some(1));
    let version: ModelProfileVersionDto = serde_json::from_value(json!({"profile_id":"pr","workspace_id":"w","version":1,"model_id":"m","config_json":"{}","created_at":"now"})).unwrap();
    assert_eq!(version.model_id, "m");
    let grant: CompanyResourceGrantDto = serde_json::from_value(
        json!({"company_id":"co","workspace_id":"w","model_profile_id":"pr","created_at":"now"}),
    )
    .unwrap();
    assert_eq!(grant.company_id, "co");
}

#[test]
fn secret_submission_is_write_only_and_debug_redacted() {
    let req = SubmitCredentialRequest::new("Key".into(), "canary-secret-do-not-log".into());
    assert_eq!(
        serde_json::to_value(&req).unwrap(),
        json!({"name":"Key","secret":"canary-secret-do-not-log"})
    );
    assert!(!format!("{req:?}").contains("canary-secret-do-not-log"));
    let _: CreateProviderConnectionRequest = serde_json::from_value(
        json!({"name":"Primary","provider_kind":"openai","credential_ref_id":null}),
    )
    .unwrap();
    let _: UpdateProviderConnectionRequest = serde_json::from_value(
        json!({"name":"Primary","credential_ref_id":null,"expected_version":1}),
    )
    .unwrap();
    let _: ModelProfileVersionRequest =
        serde_json::from_value(json!({"model_id":"m","config_json":"{}","expected_version":1}))
            .unwrap();
}

#[test]
fn workforce_and_lifecycle_are_company_scoped_and_versioned() {
    let _: CompanyLifecycleRequest = serde_json::from_value(json!({"expected_version":1})).unwrap();
    let department: DepartmentDto = serde_json::from_value(json!({"id":"d","company_id":"co","name":"Engineering","status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now"})).unwrap();
    let role: RoleDto = serde_json::from_value(json!({"id":"r","company_id":"co","name":"Engineer","status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now"})).unwrap();
    let assignment: DepartmentRoleDto = serde_json::from_value(
        json!({"company_id":"co","department_id":"d","role_id":"r","created_at":"now"}),
    )
    .unwrap();
    assert_eq!(assignment.department_id, department.id);
    assert_eq!(assignment.role_id, role.id);
    let agent: AgentDto = serde_json::from_value(json!({"id":"a","company_id":"co","name":"Ada","primary_department_id":"d","role_id":"r","model_profile_id":null,"capacity":2,"status":"ACTIVE","row_version":1,"created_at":"now","updated_at":"now"})).unwrap();
    assert_eq!(agent.capacity, 2);
    assert_eq!(
        serde_json::to_value(AgentListResponse {
            agents: vec![agent]
        })
        .unwrap()["agents"][0]["company_id"],
        "co"
    );
    let _: CreateDepartmentRequest = serde_json::from_value(json!({"name":"Engineering"})).unwrap();
    let _: CreateRoleRequest = serde_json::from_value(json!({"name":"Engineer"})).unwrap();
    let _: AssignDepartmentRoleRequest = serde_json::from_value(json!({"role_id":"r"})).unwrap();
    let _: CreateAgentRequest = serde_json::from_value(json!({"name":"Ada","primary_department_id":"d","role_id":"r","model_profile_id":null,"capacity":2})).unwrap();
    let _: UpdateAgentRequest = serde_json::from_value(json!({"name":"Ada","primary_department_id":"d","role_id":"r","model_profile_id":null,"capacity":2,"expected_version":1})).unwrap();
    let _: WorkforceLifecycleRequest =
        serde_json::from_value(json!({"expected_version":1})).unwrap();
    let _: Value = serde_json::to_value(ProviderListResponse { providers: vec![] }).unwrap();
}
