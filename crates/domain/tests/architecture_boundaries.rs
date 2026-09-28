use nalarvo_domain::*;

#[test]
fn test_domain_pure_invariants() {
    let ws_id = WorkspaceId::new();
    let company = Company::create(ws_id.clone(), "Clean Domain Corp".into(), None).unwrap();

    assert_eq!(company.workspace_id, ws_id);
    assert_eq!(company.status, CompanyStatus::Draft);
    assert_eq!(company.row_version, 1);

    let event = DomainEvent::company_created(&company, PrincipalRef::user("u-test"), "c-1", "c-1");
    assert_eq!(event.event_type, "CompanyCreated");
    assert_eq!(event.schema_version, 1);
    assert_eq!(event.company_id, company.id);
    assert_eq!(event.aggregate_version, 1);
    assert_eq!(event.principal.principal_type, PrincipalType::User);
    assert_eq!(event.principal.principal_id, "u-test");
    assert_eq!(event.scope.scope_type, ScopeType::Company);
    assert_eq!(event.scope.scope_id, company.id.0);
}
