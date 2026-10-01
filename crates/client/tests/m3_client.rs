use axum::{
    Json, Router,
    extract::{Path, Query},
    routing::get,
};
use nalarvo_client::{
    activate_objective, create_objective, get_project, get_project_work_item, list_objectives,
    list_project_work_items, list_projects,
};
use nalarvo_contracts::{CreateObjectiveRequest, ObjectiveLifecycleRequest};
use serde::Deserialize;
use serde_json::{Value, json};

fn objective(status: &str, version: i64) -> Value {
    json!({"id":"o","company_id":"co","project_id":"p","parent_objective_id":null,
        "title":"Ship","description":null,"is_primary":true,"is_required":true,
        "status":status,"row_version":version,"created_at":"now","updated_at":"now"})
}

#[tokio::test]
async fn objective_client_round_trips_scoped_creation_listing_and_action() {
    use axum::routing::post;
    let app = Router::new()
        .route("/api/v1/companies/{company_id}/projects/{project_id}/objectives",
            get(|Path((co, project)): Path<(String, String)>| async move {
                assert_eq!((co.as_str(), project.as_str()), ("co", "p"));
                Json(json!({"objectives":[objective("DRAFT", 1)]}))
            }).post(|Path((co, project)): Path<(String, String)>, Json(body): Json<Value>| async move {
                assert_eq!((co.as_str(), project.as_str()), ("co", "p"));
                assert_eq!(body, json!({"parent_objective_id":null,"title":"Ship","description":null,"is_primary":true,"is_required":true}));
                Json(objective("DRAFT", 1))
            }))
        .route("/api/v1/companies/{company_id}/projects/{project_id}/objectives/{id}",
            post(|Path((co, project, action)): Path<(String, String, String)>, Json(body): Json<Value>| async move {
                assert_eq!((co.as_str(), project.as_str(), action.as_str()), ("co", "p", "o:activate"));
                assert_eq!(body, json!({"expected_version":1}));
                Json(objective("ACTIVE", 2))
            }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    assert_eq!(
        list_objectives(&base, "token", "co", "p")
            .await
            .unwrap()
            .objectives[0]
            .id,
        "o"
    );
    let req = CreateObjectiveRequest {
        parent_objective_id: None,
        title: "Ship".into(),
        description: None,
        is_primary: true,
        is_required: true,
    };
    assert_eq!(
        create_objective(&base, "token", "co", "p", &req)
            .await
            .unwrap()
            .id,
        "o"
    );
    assert_eq!(
        activate_objective(
            &base,
            "token",
            "co",
            "p",
            "o",
            &ObjectiveLifecycleRequest {
                expected_version: 1
            }
        )
        .await
        .unwrap()
        .row_version,
        2
    );
}

use tokio::net::TcpListener;

#[tokio::test]
async fn typed_project_and_work_queries_use_scoped_routes() {
    #[derive(Deserialize)]
    struct WorkQuery {
        project_id: Option<String>,
    }

    let app = Router::new()
        .route(
            "/api/v1/companies/{company_id}/projects",
            get(|Path(company): Path<String>| async move {
                assert_eq!(company, "co");
                Json(json!({"projects":[]}))
            }),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}",
            get(
                |Path((company, project)): Path<(String, String)>| async move {
                    assert_eq!((company.as_str(), project.as_str()), ("co", "p"));
                    Json(json!({"id":"p","company_id":"co","name":"Build","description":null,"working_root_path":null,"working_root_bound_at":null,"status":"DRAFT","row_version":1,"created_at":"now","updated_at":"now"}))
                },
            ),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/work",
            get(
                |Path((company, project)): Path<(String, String)>,
                 Query(query): Query<WorkQuery>| async move {
                    assert_eq!((company.as_str(), project.as_str()), ("co", "p"));
                    assert!(query.project_id.is_none());
                    Json(json!({"work_items":[]}))
                },
            ),
        )
        .route(
            "/api/v1/companies/{company_id}/projects/{project_id}/work/{work_id}",
            get(
                |Path((company, project, work)): Path<(String, String, String)>| async move {
                    assert_eq!((company.as_str(), project.as_str(), work.as_str()), ("co", "p", "w"));
                    Json(json!({"id":"w","company_id":"co","project_id":"p","objective_id":null,"parent_work_item_id":null,"title":"Build","description":null,"work_type":"TASK","status":"BACKLOG","row_version":1,"created_at":"now","updated_at":"now"}))
                },
            ),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    assert!(
        list_projects(&base, "token", "co")
            .await
            .unwrap()
            .projects
            .is_empty()
    );
    assert_eq!(
        get_project(&base, "token", "co", "p").await.unwrap().id,
        "p"
    );
    assert!(
        list_project_work_items(&base, "token", "co", "p")
            .await
            .unwrap()
            .work_items
            .is_empty()
    );
    assert_eq!(
        get_project_work_item(&base, "token", "co", "p", "w")
            .await
            .unwrap()
            .id,
        "w"
    );
}
