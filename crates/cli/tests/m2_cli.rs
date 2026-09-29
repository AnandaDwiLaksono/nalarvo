use axum::{Json, Router, routing::get};
use nalarvo_contracts::{AgentDto, AgentListResponse, ProviderConnectionDto, ProviderListResponse};
use tokio::net::TcpListener;

#[tokio::test]
async fn test_cli_client_endpoints_for_providers_and_agents() {
    let app = Router::new()
        .route(
            "/api/v1/workspace/providers",
            get(|| async {
                Json(ProviderListResponse {
                    providers: vec![ProviderConnectionDto {
                        id: "p1".into(),
                        workspace_id: "ws1".into(),
                        name: "OpenAI Main".into(),
                        provider_kind: "openai".into(),
                        credential_ref_id: Some("cred1".into()),
                        status: "ACTIVE".into(),
                        health: "HEALTHY".into(),
                        row_version: 1,
                        created_at: "2026-09-29T10:00:00Z".into(),
                        updated_at: "2026-09-29T10:00:00Z".into(),
                    }],
                })
            }),
        )
        .route(
            "/api/v1/companies/comp1/agents",
            get(|| async {
                Json(AgentListResponse {
                    agents: vec![AgentDto {
                        id: "ag1".into(),
                        company_id: "comp1".into(),
                        name: "Agent One".into(),
                        primary_department_id: "dept1".into(),
                        role_id: "role1".into(),
                        model_profile_id: None,
                        capacity: 5,
                        status: "ACTIVE".into(),
                        row_version: 1,
                        created_at: "2026-09-29T10:00:00Z".into(),
                        updated_at: "2026-09-29T10:00:00Z".into(),
                    }],
                })
            }),
        )
        .route(
            "/api/v1/companies/comp1/agents/ag1",
            get(|| async {
                Json(AgentDto {
                    id: "ag1".into(),
                    company_id: "comp1".into(),
                    name: "Agent One".into(),
                    primary_department_id: "dept1".into(),
                    role_id: "role1".into(),
                    model_profile_id: None,
                    capacity: 5,
                    status: "ACTIVE".into(),
                    row_version: 1,
                    created_at: "2026-09-29T10:00:00Z".into(),
                    updated_at: "2026-09-29T10:00:00Z".into(),
                })
            }),
        );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let daemon_url = format!("http://{}", addr);
    let token = "test-token";

    // Exercise provider list
    let providers = nalarvo_client::list_providers(&daemon_url, token)
        .await
        .unwrap();
    assert_eq!(providers.providers.len(), 1);
    assert_eq!(providers.providers[0].name, "OpenAI Main");
    assert_eq!(providers.providers[0].health, "HEALTHY");

    // Exercise agent list
    let agents = nalarvo_client::list_agents(&daemon_url, token, "comp1")
        .await
        .unwrap();
    assert_eq!(agents.agents.len(), 1);
    assert_eq!(agents.agents[0].name, "Agent One");

    // Exercise agent show
    let agent = nalarvo_client::get_agent(&daemon_url, token, "comp1", "ag1")
        .await
        .unwrap();
    assert_eq!(agent.id, "ag1");
    assert_eq!(agent.name, "Agent One");
    assert_eq!(agent.capacity, 5);
}
