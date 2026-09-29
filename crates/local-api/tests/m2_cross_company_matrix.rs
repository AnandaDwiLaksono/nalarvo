use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use nalarvo_application::{ApplicationContext, CreateCompanyCommand};
use nalarvo_domain::WorkspaceId;
use nalarvo_local_api::router_with_app;
use serde_json::json;
use tempfile::tempdir;
use tower::ServiceExt;

#[tokio::test]
async fn test_cross_company_substitution_matrix() {
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("cross_company.db");
    let db_url = format!("sqlite://{}", db_path.display());

    let app_ctx = ApplicationContext::init(&db_url).await.unwrap();
    let workspace_id = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());

    let app = router_with_app("test-token", Some(app_ctx.clone()));

    // Create Company A
    let comp_a = app_ctx
        .create_company(CreateCompanyCommand {
            workspace_id: workspace_id.clone(),
            name: "Company A".into(),
            description: None,
            principal: None,
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();

    // Create Company B
    let comp_b = app_ctx
        .create_company(CreateCompanyCommand {
            workspace_id: workspace_id.clone(),
            name: "Company B".into(),
            description: None,
            principal: None,
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();

    // Entities in Company A
    let dept_a = app_ctx
        .create_department(&comp_a.id, "Dept A")
        .await
        .unwrap();
    let role_a = app_ctx.create_role(&comp_a.id, "Role A").await.unwrap();
    let agent_a = app_ctx
        .create_agent(&comp_a.id, "Agent A", &dept_a.id, &role_a.id, None, 1)
        .await
        .unwrap();

    // Entities in Company B
    let dept_b = app_ctx
        .create_department(&comp_b.id, "Dept B")
        .await
        .unwrap();
    let role_b = app_ctx.create_role(&comp_b.id, "Role B").await.unwrap();
    let agent_b = app_ctx
        .create_agent(&comp_b.id, "Agent B", &dept_b.id, &role_b.id, None, 1)
        .await
        .unwrap();

    // Count initial domain events
    let initial_events_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_events")
        .fetch_one(&app_ctx.pool)
        .await
        .unwrap();

    // Full Matrix of Direct & Structural Substitution Attacks
    let attack_cases = vec![
        // 1. Department Direct Substitution
        (
            "GET",
            format!(
                "/api/v1/companies/{}/departments/{}",
                comp_a.id.0, dept_b.id
            ),
            json!(null),
            "Company A accessing Company B department",
        ),
        (
            "PATCH",
            format!(
                "/api/v1/companies/{}/departments/{}",
                comp_a.id.0, dept_b.id
            ),
            json!({"name": "Hacked", "expected_version": dept_b.row_version}),
            "Company A updating Company B department",
        ),
        (
            "POST",
            format!(
                "/api/v1/companies/{}/departments/{}:retire",
                comp_a.id.0, dept_b.id
            ),
            json!(null),
            "Company A retiring Company B department",
        ),
        // 2. Role Direct Substitution
        (
            "GET",
            format!("/api/v1/companies/{}/roles/{}", comp_a.id.0, role_b.id),
            json!(null),
            "Company A accessing Company B role",
        ),
        (
            "PATCH",
            format!("/api/v1/companies/{}/roles/{}", comp_a.id.0, role_b.id),
            json!({"name": "Hacked", "expected_version": role_b.row_version}),
            "Company A updating Company B role",
        ),
        // 3. Agent Direct Substitution
        (
            "GET",
            format!("/api/v1/companies/{}/agents/{}", comp_a.id.0, agent_b.id),
            json!(null),
            "Company A accessing Company B agent",
        ),
        (
            "PATCH",
            format!("/api/v1/companies/{}/agents/{}", comp_a.id.0, agent_b.id),
            json!({
                "name": "Hacked",
                "primary_department_id": dept_a.id,
                "role_id": role_a.id,
                "model_profile_id": null,
                "capacity": 2,
                "expected_version": agent_b.row_version
            }),
            "Company A updating Company B agent",
        ),
        (
            "POST",
            format!(
                "/api/v1/companies/{}/agents/{}:activate",
                comp_a.id.0, agent_b.id
            ),
            json!({"expected_version": agent_b.row_version}),
            "Company A activating Company B agent",
        ),
        (
            "GET",
            format!(
                "/api/v1/companies/{}/agents/{}/availability",
                comp_a.id.0, agent_b.id
            ),
            json!(null),
            "Company A reading Company B agent availability",
        ),
        // 4. Structural Substitution: Creating Agent in Company A referencing Company B Dept/Role
        (
            "POST",
            format!("/api/v1/companies/{}/agents", comp_a.id.0),
            json!({
                "name": "Cross Agent Dept",
                "primary_department_id": dept_b.id, // Dept B in Company A!
                "role_id": role_a.id,
                "model_profile_id": null,
                "capacity": 1
            }),
            "Company A creating agent with Company B department",
        ),
        (
            "POST",
            format!("/api/v1/companies/{}/agents", comp_a.id.0),
            json!({
                "name": "Cross Agent Role",
                "primary_department_id": dept_a.id,
                "role_id": role_b.id, // Role B in Company A!
                "model_profile_id": null,
                "capacity": 1
            }),
            "Company A creating agent with Company B role",
        ),
        // 5. Structural Substitution: Updating Agent in Company A referencing Company B Dept/Role
        (
            "PATCH",
            format!("/api/v1/companies/{}/agents/{}", comp_a.id.0, agent_a.id),
            json!({
                "name": "Updated Cross Agent Dept",
                "primary_department_id": dept_b.id, // Dept B in Company A!
                "role_id": role_a.id,
                "model_profile_id": null,
                "capacity": 1,
                "expected_version": agent_a.row_version
            }),
            "Company A updating agent with Company B department",
        ),
        (
            "PATCH",
            format!("/api/v1/companies/{}/agents/{}", comp_a.id.0, agent_a.id),
            json!({
                "name": "Updated Cross Agent Role",
                "primary_department_id": dept_a.id,
                "role_id": role_b.id, // Role B in Company A!
                "model_profile_id": null,
                "capacity": 1,
                "expected_version": agent_a.row_version
            }),
            "Company A updating agent with Company B role",
        ),
    ];

    for (method, uri, body, desc) in attack_cases {
        let req_builder = Request::builder()
            .method(method)
            .uri(&uri)
            .header("Authorization", "Bearer test-token");

        let response = if body.is_null() {
            app.clone()
                .oneshot(req_builder.body(Body::empty()).unwrap())
                .await
                .unwrap()
        } else {
            app.clone()
                .oneshot(
                    req_builder
                        .header("Content-Type", "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap()
        };

        // Assert 1: Request is rejected (NOT_FOUND, BAD_REQUEST, or UNPROCESSABLE_ENTITY)
        assert!(
            response.status() == StatusCode::NOT_FOUND
                || response.status() == StatusCode::BAD_REQUEST
                || response.status() == StatusCode::UNPROCESSABLE_ENTITY,
            "Attack must be rejected with 404/400/422! Got: {} for {}",
            response.status(),
            desc
        );

        // Assert 2: No Company B data leaked in response
        let resp_body = response.into_body().collect().await.unwrap().to_bytes();
        let resp_text = String::from_utf8_lossy(&resp_body);
        assert!(
            !resp_text.contains("Dept B") && !resp_text.contains("Role B"),
            "Response leaked Company B data on attack: {}",
            desc
        );
    }

    // Assert 3: No mutation occurred on Company B entities
    let fresh_dept_b = app_ctx
        .get_department(&comp_b.id, &dept_b.id)
        .await
        .unwrap();
    assert_eq!(fresh_dept_b.name, "Dept B");
    assert_eq!(fresh_dept_b.status, "ACTIVE");
    assert_eq!(fresh_dept_b.row_version, dept_b.row_version);

    let fresh_role_b = app_ctx.get_role(&comp_b.id, &role_b.id).await.unwrap();
    assert_eq!(fresh_role_b.name, "Role B");
    assert_eq!(fresh_role_b.status, "ACTIVE");
    assert_eq!(fresh_role_b.row_version, role_b.row_version);

    let fresh_agent_b = app_ctx.get_agent(&comp_b.id, &agent_b.id).await.unwrap();
    assert_eq!(fresh_agent_b.name, "Agent B");
    assert_eq!(fresh_agent_b.status, "ACTIVE");
    assert_eq!(fresh_agent_b.row_version, agent_b.row_version);

    // Assert 4: No success DomainEvents were emitted during attack matrix
    let final_events_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM domain_events")
        .fetch_one(&app_ctx.pool)
        .await
        .unwrap();
    assert_eq!(
        final_events_count, initial_events_count,
        "No success DomainEvents should be emitted during attack matrix!"
    );
}
