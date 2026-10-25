use nalarvo_application::{
    ApplicationContext, CreateCompanyCommand, content_store::LocalContentStore,
};
use nalarvo_contracts::m6::*;
use nalarvo_domain::WorkspaceId;
use std::sync::Arc;
#[tokio::test]
async fn typed_http_artifact_search_isolation_and_auth() {
    let dir = tempfile::tempdir().unwrap();
    let app = ApplicationContext::init(&format!("sqlite://{}", dir.path().join("db").display()))
        .await
        .unwrap()
        .with_content_store(Arc::new(
            LocalContentStore::open(&dir.path().join("content")).unwrap(),
        ));
    let company = |name: &str| CreateCompanyCommand {
        workspace_id: WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into()),
        name: name.into(),
        description: None,
        principal: None,
        idempotency_key: None,
        correlation_id: None,
        causation_id: None,
    };
    let a = app.create_company(company("A")).await.unwrap();
    let b = app.create_company(company("B")).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = nalarvo_local_api::router_with_app("test-token", Some(app.clone()));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let input = ArtifactInput {
        title: "guide".into(),
        artifact_type: "DOCUMENT".into(),
        project_id: None,
        work_item_id: None,
        run_id: None,
        agent_id: None,
        sensitivity: Sensitivity::Internal,
        media_type: "text/plain".into(),
        content: "one".into(),
    };
    let v1 = nalarvo_client::m6::create_artifact(&base, "test-token", &a.id.0, &input)
        .await
        .unwrap();
    let mut input = input;
    input.content = "two".into();
    let v2 = nalarvo_client::m6::add_artifact_version(
        &base,
        "test-token",
        &a.id.0,
        &v1.artifact_id,
        &input,
    )
    .await
    .unwrap();
    assert_eq!(
        nalarvo_client::m6::artifact_versions(&base, "test-token", &a.id.0, &v1.artifact_id)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        nalarvo_client::m6::artifact_content(&base, "test-token", &a.id.0, &v1.artifact_id, &v1.id)
            .await
            .unwrap()
            .content,
        "one"
    );
    assert_eq!(
        nalarvo_client::m6::artifact_content(&base, "test-token", &a.id.0, &v2.artifact_id, &v2.id)
            .await
            .unwrap()
            .content,
        "two"
    );
    assert!(
        nalarvo_client::m6::artifact_versions(&base, "test-token", &b.id.0, &v1.artifact_id)
            .await
            .is_err()
    );
    assert!(
        nalarvo_client::m6::list_artifacts(&base, "wrong", &a.id.0)
            .await
            .is_err()
    );
    let source = nalarvo_client::m6::create_source(
        &base,
        "test-token",
        &a.id.0,
        &SourceInput {
            title: "guide".into(),
            project_id: None,
        },
    )
    .await
    .unwrap();
    let k = nalarvo_client::m6::create_knowledge(
        &base,
        "test-token",
        &a.id.0,
        &InformationInput {
            title: "sapphire".into(),
            content: "sapphire guide".into(),
            scope: InformationScope::Company,
            project_id: None,
            agent_id: None,
            sensitivity: Sensitivity::Internal,
            source_id: Some(source.id),
            source_run_id: None,
        },
    )
    .await
    .unwrap();
    nalarvo_client::m6::transition_information(
        &base,
        "test-token",
        &a.id.0,
        "KNOWLEDGE",
        &k.id,
        &InformationTransition {
            lifecycle_state: InformationLifecycle::Active,
            expected_version: 1,
        },
    )
    .await
    .unwrap();
    let query = SearchRequest {
        query: "sapphire".into(),
        project_id: None,
        agent_id: None,
    };
    assert_eq!(
        nalarvo_client::m6::search_information(&base, "test-token", &a.id.0, &query)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        nalarvo_client::m6::search_information(&base, "test-token", &b.id.0, &query)
            .await
            .unwrap()
            .is_empty()
    );
    let bad = reqwest::Client::new()
        .get(format!("{base}/api/v1/content/{}", v1.content_ref.hash))
        .bearer_auth("test-token")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 404);
    for _ in 0..103 {
        app.add_artifact_version(
            &a.id.0,
            &v1.artifact_id,
            nalarvo_application::m6::ArtifactInput {
                title: input.title.clone(),
                artifact_type: input.artifact_type.clone(),
                project_id: None,
                work_item_id: None,
                run_id: None,
                agent_id: None,
                sensitivity: Sensitivity::Internal,
                media_type: input.media_type.clone(),
                content: input.content.clone(),
            },
        )
        .await
        .unwrap();
    }
    let first =
        nalarvo_client::m6::artifact_versions(&base, "test-token", &a.id.0, &v1.artifact_id)
            .await
            .unwrap();
    assert_eq!(first.len(), 100, "server must enforce a SQL page bound");
    let second = nalarvo_client::m6::artifact_versions_page(
        &base,
        "test-token",
        &a.id.0,
        &v1.artifact_id,
        &ArtifactVersionQuery {
            after_version: first.last().unwrap().version_no,
            limit: 100,
        },
    )
    .await
    .unwrap();
    assert_eq!(second.len(), 5);
    let numbers: Vec<_> = first.iter().chain(&second).map(|v| v.version_no).collect();
    assert_eq!(numbers, (1..=105).collect::<Vec<_>>());
    assert!(
        nalarvo_client::m6::artifact_versions_page(
            &base,
            "test-token",
            &a.id.0,
            &v1.artifact_id,
            &ArtifactVersionQuery {
                after_version: 105,
                limit: 100
            }
        )
        .await
        .unwrap()
        .is_empty()
    );
    let one = nalarvo_client::m6::artifact_versions_page(
        &base,
        "test-token",
        &a.id.0,
        &v1.artifact_id,
        &ArtifactVersionQuery {
            after_version: 100,
            limit: 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].version_no, 101);
    for query in [
        "limit=0",
        "limit=-1",
        "limit=101",
        "limit=9223372036854775808",
        "after_version=-1",
        "after_version=x",
        "limit=x",
        "unknown=1",
    ] {
        let status = reqwest::Client::new()
            .get(format!(
                "{base}/api/v1/companies/{}/artifacts/{}/versions?{query}",
                a.id.0, v1.artifact_id
            ))
            .bearer_auth("test-token")
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(status, 400, "accepted {query}");
    }
    assert!(
        app.artifact_versions_page(&a.id.0, &v1.artifact_id, -1, 100)
            .await
            .is_err()
    );
    assert!(
        app.artifact_versions_page(&a.id.0, &v1.artifact_id, 0, 101)
            .await
            .is_err()
    );
    assert!(
        nalarvo_client::m6::artifact_versions_page(
            &base,
            "test-token",
            &b.id.0,
            &v1.artifact_id,
            &ArtifactVersionQuery::default()
        )
        .await
        .is_err()
    );
    assert!(
        nalarvo_client::m6::artifact_content(&base, "test-token", &b.id.0, &v1.artifact_id, &v1.id)
            .await
            .is_err()
    );
    // A poisoned unrelated history row must not be decoded during direct version downloads.
    assert!(
        sqlx::query(
            "UPDATE artifact_versions SET provenance='{}' WHERE artifact_id=? AND version_no=105"
        )
        .bind(&v1.artifact_id)
        .execute(&app.pool)
        .await
        .is_err()
    );
    // Test-only corruption after proving the production immutable trigger rejects it.
    sqlx::query("DROP TRIGGER artifact_version_immutable")
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE artifact_versions SET provenance='1e999' WHERE artifact_id=? AND version_no=105",
    )
    .bind(&v1.artifact_id)
    .execute(&app.pool)
    .await
    .unwrap();
    assert!(
        app.artifact_versions_page(&a.id.0, &v1.artifact_id, 100, 100)
            .await
            .is_err()
    );
    for (v, expected) in [(&v1, "one"), (&v2, "two")] {
        assert_eq!(
            nalarvo_client::m6::artifact_content(
                &base,
                "test-token",
                &a.id.0,
                &v.artifact_id,
                &v.id
            )
            .await
            .unwrap()
            .content,
            expected
        );
    }
    server.abort();
}
