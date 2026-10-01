#[test]
fn m3_invoke_command_names_are_registered() {
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs")).unwrap();
    for command in [
        "core_list_projects",
        "core_create_project",
        "core_get_project",
        "core_activate_project",
        "core_project_lifecycle",
        "core_bind_project_working_root",
        "core_unbind_project_working_root",
        "core_list_objectives",
        "core_create_objective",
        "core_objective_lifecycle",
        "core_list_teams",
        "core_create_team",
        "core_team_lifecycle",
        "core_list_staffing_requirements",
        "core_create_staffing_requirement",
        "core_staffing_lifecycle",
        "core_list_allocations",
        "core_create_allocation",
        "core_allocation_lifecycle",
        "core_list_work_items",
        "core_create_work_item",
        "core_get_work_item",
        "core_work_lifecycle",
        "core_list_dependencies",
        "core_create_dependency",
        "core_delete_dependency",
        "core_list_assignments",
        "core_create_assignment",
        "core_assignment_lifecycle",
    ] {
        assert!(source.contains(command), "missing {command}");
    }
}

#[test]
fn agent_availability_comes_from_core_not_a_desktop_default() {
    let source =
        std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/src/lib.rs")).unwrap();
    assert!(source.contains("nalarvo_client::get_agent_availability"));
    assert!(!source.contains("Some(\"AVAILABLE\".into())"));
}
