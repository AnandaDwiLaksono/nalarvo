use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use nalarvo_application::ApplicationContext;
use nalarvo_contracts::{
    AgentAllocationDto, CreateAgentAllocationRequest, CreateObjectiveRequest, CreateProjectRequest,
    CreateStaffingRequirementRequest, CreateTeamRequest, CreateWorkAssignmentRequest,
    CreateWorkDependencyRequest, CreateWorkItemRequest, ObjectiveDto, ObjectiveListResponse,
    ProjectDto, StaffingRequirementDto, TeamDto, WorkAssignmentDto, WorkDependencyDto, WorkItemDto,
};
use nalarvo_local_api::router_with_app;
use std::sync::Arc;
use tower::ServiceExt;

const TEST_TOKEN: &str = "test-token-123456";

async fn setup_app() -> axum::Router {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_api.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    router_with_app(Arc::<str>::from(TEST_TOKEN), Some(app_ctx))
}

#[tokio::test]
async fn test_m3_all_routes_and_isolation_matrices() {
    let app = setup_app().await;

    // 1. Create company A and company B
    let co_a = serde_json::json!({"name": "Company A"});
    let req_co_a = Request::builder()
        .method("POST")
        .uri("/api/v1/workspaces/0191e4b8-0002-7000-8000-000000000001/companies")
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&co_a).unwrap()))
        .unwrap();
    let res_co_a = app.clone().oneshot(req_co_a).await.unwrap();
    let body_co_a = res_co_a.into_body().collect().await.unwrap().to_bytes();
    let co_a_json: serde_json::Value = serde_json::from_slice(&body_co_a).unwrap();
    let company_a = co_a_json["id"].as_str().unwrap();

    let co_b = serde_json::json!({"name": "Company B"});
    let req_co_b = Request::builder()
        .method("POST")
        .uri("/api/v1/workspaces/0191e4b8-0002-7000-8000-000000000001/companies")
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&co_b).unwrap()))
        .unwrap();
    let res_co_b = app.clone().oneshot(req_co_b).await.unwrap();
    let body_co_b = res_co_b.into_body().collect().await.unwrap().to_bytes();
    let co_b_json: serde_json::Value = serde_json::from_slice(&body_co_b).unwrap();
    let company_b = co_b_json["id"].as_str().unwrap();

    // Seed department, role, agent for Company A
    let dept_req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/companies/{company_a}/departments"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({"name": "Eng"})).unwrap(),
        ))
        .unwrap();
    let dept_res = app.clone().oneshot(dept_req).await.unwrap();
    let dept_json: serde_json::Value =
        serde_json::from_slice(&dept_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let dept_id = dept_json["id"].as_str().unwrap();

    let role_req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/companies/{company_a}/roles"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({"name": "Dev", "department_id": dept_id}))
                .unwrap(),
        ))
        .unwrap();
    let role_res = app.clone().oneshot(role_req).await.unwrap();
    let role_json: serde_json::Value =
        serde_json::from_slice(&role_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let role_id = role_json["id"].as_str().unwrap();

    let agent_req = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/companies/{company_a}/agents"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({
                "name": "Agent1",
                "primary_department_id": dept_id,
                "role_id": role_id,
                "model_profile_id": None::<String>,
                "capacity": 10
            }))
            .unwrap(),
        ))
        .unwrap();
    let agent_res = app.clone().oneshot(agent_req).await.unwrap();
    let agent_json: serde_json::Value =
        serde_json::from_slice(&agent_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let agent_id = agent_json["id"].as_str().unwrap();

    // 2. Create Project in Company A
    let create_p = CreateProjectRequest {
        name: "Project A1".into(),
        description: None,
    };
    let req_p = Request::builder()
        .method("POST")
        .uri(format!("/api/v1/companies/{company_a}/projects"))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&create_p).unwrap()))
        .unwrap();
    let res_p = app.clone().oneshot(req_p).await.unwrap();
    assert_eq!(res_p.status(), StatusCode::CREATED);
    let project_a1: ProjectDto =
        serde_json::from_slice(&res_p.into_body().collect().await.unwrap().to_bytes()).unwrap();

    // Activate Project A1
    let req_act_p = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}:activate",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({"expected_version": 1})).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req_act_p).await.unwrap().status(),
        StatusCode::OK
    );

    // 3. Objectives Route Surface
    let obj_create = CreateObjectiveRequest {
        parent_objective_id: None,
        title: "Objective 1".into(),
        description: Some("Objective 1 desc".into()),
        is_primary: true,
        is_required: true,
    };
    let req_obj = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/objectives",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("Idempotency-Key", "obj-key-1")
        .body(Body::from(serde_json::to_vec(&obj_create).unwrap()))
        .unwrap();
    let res_obj = app.clone().oneshot(req_obj).await.unwrap();
    assert_eq!(res_obj.status(), StatusCode::CREATED);
    let obj_1: ObjectiveDto =
        serde_json::from_slice(&res_obj.into_body().collect().await.unwrap().to_bytes()).unwrap();

    // List & Get Objectives
    let req_list_obj = Request::builder()
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/objectives",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let res_list_obj = app.clone().oneshot(req_list_obj).await.unwrap();
    let obj_list: ObjectiveListResponse =
        serde_json::from_slice(&res_list_obj.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert_eq!(obj_list.objectives.len(), 1);

    // 4. Team Route Surface
    let team_create = CreateTeamRequest {
        name: "Team Eng".into(),
        is_primary: true,
    };
    let req_team = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/teams",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&team_create).unwrap()))
        .unwrap();
    let res_team = app.clone().oneshot(req_team).await.unwrap();
    assert_eq!(res_team.status(), StatusCode::CREATED);
    let team_1: TeamDto =
        serde_json::from_slice(&res_team.into_body().collect().await.unwrap().to_bytes()).unwrap();

    // Activate Team
    let req_act_team = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/teams/{}:activate",
            project_a1.id, team_1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({"expected_version": 1})).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req_act_team).await.unwrap().status(),
        StatusCode::OK
    );

    // 5. Staffing Requirement Route Surface
    let staffing_create = CreateStaffingRequirementRequest {
        team_id: Some(team_1.id.clone()),
        role_id: role_id.into(),
        department_id: Some(dept_id.into()),
        desired_count: 1,
        required_capability_ids: vec![],
    };
    let req_staff = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/staffing-requirements",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&staffing_create).unwrap()))
        .unwrap();
    let res_staff = app.clone().oneshot(req_staff).await.unwrap();
    assert_eq!(res_staff.status(), StatusCode::CREATED);
    let staff_1: StaffingRequirementDto =
        serde_json::from_slice(&res_staff.into_body().collect().await.unwrap().to_bytes()).unwrap();

    // 6. Agent Allocation Route Surface
    let alloc_create = CreateAgentAllocationRequest {
        team_id: team_1.id.clone(),
        agent_id: agent_id.into(),
        staffing_requirement_id: Some(staff_1.id.clone()),
    };
    let req_alloc = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/allocations",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&alloc_create).unwrap()))
        .unwrap();
    let res_alloc = app.clone().oneshot(req_alloc).await.unwrap();
    assert_eq!(res_alloc.status(), StatusCode::CREATED);
    let alloc_1: AgentAllocationDto =
        serde_json::from_slice(&res_alloc.into_body().collect().await.unwrap().to_bytes()).unwrap();

    // Activate Allocation
    let req_act_alloc = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/allocations/{}:activate",
            project_a1.id, alloc_1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({"expected_version": 1})).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req_act_alloc).await.unwrap().status(),
        StatusCode::OK
    );

    // 7. Work Items, Work Dependencies, and Work Assignments
    let work_create1 = CreateWorkItemRequest {
        objective_id: Some(obj_1.id.clone()),
        parent_work_item_id: None,
        title: "Work 1".into(),
        description: None,
        work_type: "TASK".into(),
    };
    let req_w1 = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/work",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&work_create1).unwrap()))
        .unwrap();
    let res_w1 = app.clone().oneshot(req_w1).await.unwrap();
    assert_eq!(res_w1.status(), StatusCode::CREATED);
    let work_1: WorkItemDto =
        serde_json::from_slice(&res_w1.into_body().collect().await.unwrap().to_bytes()).unwrap();

    let work_create2 = CreateWorkItemRequest {
        objective_id: Some(obj_1.id.clone()),
        parent_work_item_id: None,
        title: "Work 2".into(),
        description: None,
        work_type: "TASK".into(),
    };
    let req_w2 = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/work",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&work_create2).unwrap()))
        .unwrap();
    let res_w2 = app.clone().oneshot(req_w2).await.unwrap();
    assert_eq!(res_w2.status(), StatusCode::CREATED);
    let work_2: WorkItemDto =
        serde_json::from_slice(&res_w2.into_body().collect().await.unwrap().to_bytes()).unwrap();

    // Dependency
    let dep_create = CreateWorkDependencyRequest {
        depends_on_work_item_id: work_1.id.clone(),
        dependency_type: "HARD".into(),
    };
    let req_dep = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/work/{}/dependencies",
            project_a1.id, work_2.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&dep_create).unwrap()))
        .unwrap();
    let res_dep = app.clone().oneshot(req_dep).await.unwrap();
    assert_eq!(res_dep.status(), StatusCode::CREATED);
    let dep_1: WorkDependencyDto =
        serde_json::from_slice(&res_dep.into_body().collect().await.unwrap().to_bytes()).unwrap();

    // Assignment
    let assign_create = CreateWorkAssignmentRequest {
        agent_id: agent_id.into(),
        agent_allocation_id: alloc_1.id.clone(),
        is_primary: true,
    };
    let req_assign = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/work/{}/assignments",
            project_a1.id, work_1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&assign_create).unwrap()))
        .unwrap();
    let res_assign = app.clone().oneshot(req_assign).await.unwrap();
    assert_eq!(res_assign.status(), StatusCode::CREATED);
    let assign_1: WorkAssignmentDto =
        serde_json::from_slice(&res_assign.into_body().collect().await.unwrap().to_bytes())
            .unwrap();

    // Release Assignment
    let req_rel_assign = Request::builder()
        .method("POST")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/work/{}/assignments/{}:release",
            project_a1.id, work_1.id, assign_1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({"expected_version": 1})).unwrap(),
        ))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req_rel_assign).await.unwrap().status(),
        StatusCode::OK
    );

    // Delete Dependency
    let req_del_dep = Request::builder()
        .method("DELETE")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/work/{}/dependencies/{}",
            project_a1.id, work_2.id, dep_1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req_del_dep).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );

    // 8. Working Root Bind/Unbind
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();
    let req_bind_root = Request::builder()
        .method("PUT")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/working-root",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({
                "path": root,
                "expected_version": 2
            }))
            .unwrap(),
        ))
        .unwrap();
    let res_bind_root = app.clone().oneshot(req_bind_root).await.unwrap();
    assert_eq!(res_bind_root.status(), StatusCode::OK);
    let proj_bound: ProjectDto = serde_json::from_slice(
        &res_bind_root
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes(),
    )
    .unwrap();
    assert_eq!(proj_bound.working_root_path.as_deref(), Some(root.as_str()));

    let req_unbind_root = Request::builder()
        .method("DELETE")
        .uri(format!(
            "/api/v1/companies/{company_a}/projects/{}/working-root",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&serde_json::json!({
                "expected_version": proj_bound.row_version
            }))
            .unwrap(),
        ))
        .unwrap();
    let res_unbind_root = app.clone().oneshot(req_unbind_root).await.unwrap();
    assert_eq!(res_unbind_root.status(), StatusCode::OK);
    let proj_unbound: ProjectDto = serde_json::from_slice(
        &res_unbind_root
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes(),
    )
    .unwrap();
    assert!(proj_unbound.working_root_path.is_none());

    // 9. Isolation Matrix Checks (Cross-Company 404)
    let req_cross_co = Request::builder()
        .uri(format!(
            "/api/v1/companies/{company_b}/projects/{}",
            project_a1.id
        ))
        .header(header::AUTHORIZATION, format!("Bearer {TEST_TOKEN}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(req_cross_co).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );
}
