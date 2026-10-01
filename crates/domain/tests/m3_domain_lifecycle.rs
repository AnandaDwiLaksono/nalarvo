use nalarvo_domain::*;

#[test]
fn test_objective_and_team_statuses() {
    assert_eq!(
        "DRAFT".parse::<ObjectiveStatus>().unwrap(),
        ObjectiveStatus::Draft
    );
    assert_eq!(
        "ACTIVE".parse::<ObjectiveStatus>().unwrap(),
        ObjectiveStatus::Active
    );
    assert_eq!(
        "ACHIEVED".parse::<ObjectiveStatus>().unwrap(),
        ObjectiveStatus::Achieved
    );
    assert_eq!(
        "FAILED".parse::<ObjectiveStatus>().unwrap(),
        ObjectiveStatus::Failed
    );
    assert_eq!(
        "CANCELLED".parse::<ObjectiveStatus>().unwrap(),
        ObjectiveStatus::Cancelled
    );
    assert_eq!(
        "ARCHIVED".parse::<ObjectiveStatus>().unwrap(),
        ObjectiveStatus::Archived
    );

    assert_eq!(
        "FORMING".parse::<TeamStatus>().unwrap(),
        TeamStatus::Forming
    );
    assert_eq!("ACTIVE".parse::<TeamStatus>().unwrap(), TeamStatus::Active);
    assert_eq!("PAUSED".parse::<TeamStatus>().unwrap(), TeamStatus::Paused);
    assert_eq!(
        "DISBANDED".parse::<TeamStatus>().unwrap(),
        TeamStatus::Disbanded
    );
    assert_eq!(
        "ARCHIVED".parse::<TeamStatus>().unwrap(),
        TeamStatus::Archived
    );
}

#[test]
fn staffing_requirement_uses_canonical_lifecycle() {
    let company_id = CompanyId::new();
    let now = chrono::Utc::now();
    let mut requirement = StaffingRequirement {
        id: "staffing-1".into(),
        company_id,
        project_id: "project-1".into(),
        team_id: None,
        role_id: "role-1".into(),
        department_id: None,
        desired_count: 2,
        required_capability_ids: vec![],
        status: StaffingRequirementStatus::Draft,
        row_version: 1,
        created_at: now,
        updated_at: now,
    };

    assert_eq!(
        "PARTIALLY_FILLED"
            .parse::<StaffingRequirementStatus>()
            .unwrap(),
        StaffingRequirementStatus::PartiallyFilled
    );
    assert_eq!(
        "FILLED".parse::<StaffingRequirementStatus>().unwrap(),
        StaffingRequirementStatus::Filled
    );
    requirement.reconcile(2);
    assert_eq!(requirement.status, StaffingRequirementStatus::Draft);

    requirement.open().unwrap();
    requirement.reconcile(1);
    assert_eq!(
        requirement.status,
        StaffingRequirementStatus::PartiallyFilled
    );
    requirement.reconcile(2);
    assert_eq!(requirement.status, StaffingRequirementStatus::Filled);

    requirement.block().unwrap();
    requirement.reconcile(0);
    assert_eq!(requirement.status, StaffingRequirementStatus::Blocked);
    requirement.unblock().unwrap();
    assert_eq!(requirement.status, StaffingRequirementStatus::Open);

    requirement.cancel().unwrap();
    requirement.reconcile(2);
    assert_eq!(requirement.status, StaffingRequirementStatus::Cancelled);
    assert!(requirement.open().is_err());
}

#[test]
fn test_project_lifecycle_transitions() {
    let company_id = CompanyId::new();
    let mut project = Project::create(company_id, "Core Platform".into(), None).unwrap();
    assert_eq!(project.priority, ProjectPriority::Medium);
    assert!(project.owner_user_id.is_none());
    assert!(project.target_outcome.is_none());
    assert!(project.target_date.is_none());
    project.priority = ProjectPriority::High;
    project.target_outcome = Some("Ship usable project work".into());
    assert_eq!(project.priority.to_string(), "HIGH");
    assert_eq!(
        project.target_outcome.as_deref(),
        Some("Ship usable project work")
    );
    assert_eq!(project.status, ProjectStatus::Draft);
    assert_eq!(project.row_version, 1);

    // Can transition DRAFT -> ACTIVE directly
    project.activate(1).unwrap();
    assert_eq!(project.status, ProjectStatus::Active);
    assert_eq!(project.row_version, 2);

    // ACTIVE -> PAUSED -> ACTIVE
    project.pause(2).unwrap();
    assert_eq!(project.status, ProjectStatus::Paused);
    project.resume(3).unwrap();
    assert_eq!(project.status, ProjectStatus::Active);

    // Cancellation from ACTIVE
    let mut cancel_proj = project.clone();
    cancel_proj.cancel(4).unwrap();
    assert_eq!(cancel_proj.status, ProjectStatus::Cancelled);

    // Complete from ACTIVE
    project.complete(4).unwrap();
    assert_eq!(project.status, ProjectStatus::Completed);
    assert!(project.activate(5).is_err());
    assert!(project.pause(5).is_err());
}
