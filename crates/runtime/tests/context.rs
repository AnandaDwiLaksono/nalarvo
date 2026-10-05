use nalarvo_runtime::context::{ContextInput, build_context};

#[test]
fn context_is_minimum_scoped_and_excludes_secrets_and_foreign_company_data() {
    let input = ContextInput {
        company_id: "company-a".into(),
        project_id: "project-a".into(),
        project_name: "Project A".into(),
        project_instructions: "Return a concise status.".into(),
        objective_summary: Some("Objective A".into()),
        team_name: "Team A".into(),
        agent_name: "Agent A".into(),
        role_name: "Engineer".into(),
        department_name: "Engineering".into(),
        work_item_title: "Read-only summary".into(),
        work_item_description: "Summarize the assigned source.".into(),
        acceptance_criteria: "Safe result.".into(),
        assignment_summary: "Assigned to Agent A.".into(),
        requested_output: "Plain text".into(),
        safe_execution_metadata: "run=run-a".into(),
        excluded_values: vec!["SECRET_CANARY".into(), "COMPANY_B_MARKER".into()],
    };

    let context = build_context(&input).unwrap();
    assert!(context.contains("company-a"));
    assert!(context.contains("Project A"));
    assert!(!context.contains("SECRET_CANARY"));
    assert!(!context.contains("COMPANY_B_MARKER"));
}

#[test]
fn context_rejects_blank_scope_or_oversize_result() {
    let mut input = ContextInput::minimal("company-a", "project-a", "work");
    input.company_id = " ".into();
    assert!(build_context(&input).is_err());
}
