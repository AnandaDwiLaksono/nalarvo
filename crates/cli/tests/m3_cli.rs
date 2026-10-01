use axum::{
    Json, Router,
    extract::Json as ExtractJson,
    routing::{get, post},
};
use nalarvo_contracts::{CreateProjectRequest, ProjectLifecycleRequest};
use serde_json::json;
use tokio::{net::TcpListener, process::Command};

async fn cli(base: &str, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_nalarvo"))
        .args(["--daemon-url", base, "--token", "test-token"])
        .args(args)
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[tokio::test]
async fn project_and_project_scoped_work_commands_call_daemon() {
    let project = || json!({"id":"p1","company_id":"comp1","name":"Project One","description":null,"working_root_path":null,"working_root_bound_at":null,"status":"DRAFT","row_version":1,"created_at":"now","updated_at":"now"});
    let work = || json!({"id":"w1","company_id":"comp1","project_id":"p1","objective_id":null,"parent_work_item_id":null,"title":"Work One","description":null,"work_type":"TASK","status":"BACKLOG","row_version":1,"created_at":"now","updated_at":"now"});
    let app = Router::new()
        .route(
            "/api/v1/companies/comp1/projects",
            get(move || async move { Json(json!({"projects":[project()]})) })
                .post(|ExtractJson(req): ExtractJson<CreateProjectRequest>| async move {
                    assert_eq!(req.name, "Project Two");
                    Json(json!({"id":"p2","company_id":"comp1","name":req.name,"description":req.description,"working_root_path":null,"working_root_bound_at":null,"status":"DRAFT","row_version":1,"created_at":"now","updated_at":"now"}))
                }),
        )
        .route(
            "/api/v1/companies/comp1/projects/p1",
            get(move || async move { Json(project()) }),
        )
        .route(
            "/api/v1/companies/comp1/projects/p1:activate",
            post(|ExtractJson(req): ExtractJson<ProjectLifecycleRequest>| async move {
                assert_eq!(req.expected_version, 1);
                Json(json!({"id":"p1","company_id":"comp1","name":"Project One","description":null,"working_root_path":null,"working_root_bound_at":null,"status":"ACTIVE","row_version":2,"created_at":"now","updated_at":"now"}))
            }),
        )
        .route(
            "/api/v1/companies/comp1/projects/p1/work",
            get(move || async move { Json(json!({"work_items":[work()]})) }),
        )
        .route(
            "/api/v1/companies/comp1/projects/p1/work/w1",
            get(move || async move { Json(work()) }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    assert!(
        cli(&base, &["project", "list", "--company", "comp1"])
            .await
            .contains("Project One")
    );
    assert!(
        cli(
            &base,
            &[
                "project",
                "create",
                "--company",
                "comp1",
                "--name",
                "Project Two"
            ]
        )
        .await
        .contains("p2")
    );
    assert!(
        cli(&base, &["project", "show", "p1", "--company", "comp1"])
            .await
            .contains("Project One")
    );
    assert!(
        cli(
            &base,
            &[
                "project",
                "activate",
                "p1",
                "--company",
                "comp1",
                "--expected-version",
                "1"
            ]
        )
        .await
        .contains("ACTIVE")
    );
    assert!(
        cli(
            &base,
            &["work", "list", "--company", "comp1", "--project", "p1"]
        )
        .await
        .contains("Work One")
    );
    assert!(
        cli(
            &base,
            &[
                "work",
                "show",
                "w1",
                "--company",
                "comp1",
                "--project",
                "p1"
            ]
        )
        .await
        .contains("Work One")
    );
}
