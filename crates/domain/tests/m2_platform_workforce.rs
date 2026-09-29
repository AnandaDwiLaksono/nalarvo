use nalarvo_domain::*;
use serde_json::json;

#[test]
fn workspace_provisions_and_transitions_without_invalid_reactivation() {
    let mut workspace = Workspace::create(UserId("u-1".into()), "Personal".into()).unwrap();
    assert_eq!(workspace.lifecycle, WorkspaceLifecycle::Provisioning);
    workspace.activate().unwrap();
    workspace.suspend().unwrap();
    workspace.activate().unwrap();
    workspace.suspend().unwrap();
    assert!(workspace.archive().is_ok());
    assert!(workspace.activate().is_err());
}

#[test]
fn credential_ref_stores_locator_and_safe_metadata_never_secret_value() {
    let mut credential = CredentialRef::create(
        WorkspaceId("w-1".into()),
        "openai",
        "keychain://nalarvo/openai",
        "API key",
    )
    .unwrap();
    let serialized = serde_json::to_string(&credential).unwrap();
    assert!(!serialized.contains("sk-secret"));
    assert!(serialized.contains("keychain://nalarvo/openai"));
    assert_eq!(credential.lifecycle, CredentialLifecycle::Active);
    assert!(credential.disable().is_ok());
    assert!(credential.revoke().is_ok());
    assert!(credential.enable().is_err());
}

#[test]
fn provider_health_and_lifecycle_are_independent_and_config_is_non_secret() {
    let mut connection = ProviderConnection::create(
        WorkspaceId("w-1".into()),
        "OpenAI-compatible".into(),
        "Primary".into(),
        Some("https://api.example.test/v1".into()),
        None,
        ProviderConfig {
            default_model: Some("model-x".into()),
            supports_streaming: true,
            supports_tool_calling: false,
            supports_structured_output: true,
            context_limit: Some(8192),
        },
    )
    .unwrap();
    connection.enable().unwrap();
    connection.record_health(ProviderHealth::Healthy, chrono::Utc::now());
    assert_eq!(connection.lifecycle, ProviderConnectionLifecycle::Enabled);
    assert_eq!(connection.health, ProviderHealth::Healthy);
    assert!(
        !serde_json::to_string(&connection)
            .unwrap()
            .contains("secret")
    );
    connection.disable().unwrap();
    assert_eq!(connection.health, ProviderHealth::Healthy);
}

#[test]
fn model_and_profile_preserve_provider_reference_and_configuration() {
    let model = Model::create(
        "conn-1".into(),
        "gpt-test".into(),
        Some("Test".into()),
        json!({"tools":true}),
        Some(4096),
    )
    .unwrap();
    let profile = ModelProfile::create(
        WorkspaceId("w-1".into()),
        "Default".into(),
        model.id.clone(),
        json!({"temperature":0.2}),
    )
    .unwrap();
    assert_eq!(profile.primary_model_id, model.id);
    assert_eq!(model.model_identifier, "gpt-test");
}

#[test]
fn company_mission_director_and_semantic_transitions() {
    let mut company = Company::create(WorkspaceId("w-1".into()), "Co".into(), None).unwrap();
    company.set_mission("Build responsibly".into(), 1).unwrap();
    company.set_director(UserId("u-1".into()), 2).unwrap();
    assert_eq!(company.mission.as_deref(), Some("Build responsibly"));
    assert_eq!(company.director_user_id.as_ref().unwrap().0, "u-1");
    company.activate().unwrap();
    company.pause().unwrap();
    company.resume().unwrap();
    company.pause().unwrap();
    company.archive().unwrap();
    assert!(company.resume().is_err());
}

#[test]
fn workforce_lifecycle_capacity_and_availability_are_explicit() {
    let mut department =
        Department::create(CompanyId("co-1".into()), "Engineering".into(), None).unwrap();
    department.activate().unwrap();
    let role = Role::create(
        CompanyId("co-1".into()),
        "Engineer".into(),
        None,
        "Build systems".into(),
    )
    .unwrap();
    let mut agent = Agent::create(
        CompanyId("co-1".into()),
        department.id.clone(),
        role.id.clone(),
        "Ada".into(),
        2,
    )
    .unwrap();
    assert_eq!(agent.availability(0), AgentAvailability::Unavailable);
    agent.activate().unwrap();
    assert_eq!(agent.availability(0), AgentAvailability::Available);
    assert_eq!(agent.availability(1), AgentAvailability::PartiallyAllocated);
    assert_eq!(agent.availability(2), AgentAvailability::FullyAllocated);
    assert_eq!(agent.availability(3), AgentAvailability::Unavailable);
    assert_eq!(agent.availability(1), AgentAvailability::PartiallyAllocated);
    agent.pause().unwrap();
    assert_eq!(agent.availability(0), AgentAvailability::Unavailable);
    agent.resume().unwrap();
    agent.retire().unwrap();
    assert!(agent.resume().is_err());
    assert_eq!(department.lifecycle, DepartmentLifecycle::Active);
}
