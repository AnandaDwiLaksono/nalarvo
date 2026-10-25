use nalarvo_application::{
    ApplicationContext, CreateCompanyCommand,
    content_store::LocalContentStore,
    m6_information::{InformationInput, RetrievalScope, SourceInput},
};
use nalarvo_domain::{
    WorkspaceId,
    information::{InformationLifecycle, InformationScope, Sensitivity},
};
use std::sync::Arc;

#[tokio::test]
async fn lifecycle_fts_scope_secret_consistency_restart() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("db").display());
    let store = Arc::new(LocalContentStore::open(&dir.path().join("content")).unwrap());
    let app = ApplicationContext::init(&url)
        .await
        .unwrap()
        .with_content_store(store.clone());
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
    let source = app
        .create_knowledge_source(
            &a.id.0,
            SourceInput {
                title: "Manual".into(),
                project_id: None,
            },
        )
        .await
        .unwrap();
    let scope = RetrievalScope {
        project_id: None,
        agent_id: None,
    };
    let input = InformationInput {
        title: "shipping".into(),
        content: "shipping sapphire guide".into(),
        scope: InformationScope::Company,
        project_id: None,
        agent_id: None,
        sensitivity: Sensitivity::Internal,
        source_id: Some(source.id.clone()),
        source_run_id: None,
    };
    let draft = app.create_knowledge(&a.id.0, input.clone()).await.unwrap();
    assert!(
        app.search_information(&a.id.0, &scope, "sapphire")
            .await
            .unwrap()
            .is_empty()
    );
    let active = app
        .transition_information(
            &a.id.0,
            "KNOWLEDGE",
            &draft.id,
            InformationLifecycle::Active,
            draft.row_version,
        )
        .await
        .unwrap();
    assert_eq!(
        app.search_information(&a.id.0, &scope, "sapphire")
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        app.search_information(&b.id.0, &scope, "sapphire")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        app.get_information(&b.id.0, "KNOWLEDGE", &draft.id)
            .await
            .is_err()
    );
    assert!(app.create_knowledge(&b.id.0, input.clone()).await.is_err());
    let mut update = input.clone();
    update.content = "shipping emerald replacement".into();
    let revised = app
        .revise_knowledge(&a.id.0, &draft.id, update, active.row_version)
        .await
        .unwrap();
    assert_eq!(revised.version_no, 2);
    assert!(
        app.search_information(&a.id.0, &scope, "sapphire")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        app.search_information(&a.id.0, &scope, "emerald")
            .await
            .unwrap()
            .len(),
        1
    );
    app.transition_information(
        &a.id.0,
        "KNOWLEDGE",
        &draft.id,
        InformationLifecycle::Retired,
        revised.row_version,
    )
    .await
    .unwrap();
    assert!(
        app.search_information(&a.id.0, &scope, "emerald")
            .await
            .unwrap()
            .is_empty()
    );
    let mut memory = input.clone();
    memory.source_id = None;
    memory.content = "shipping remembered sapphire".into();
    let candidate = app.capture_memory_candidate(&a.id.0, memory).await.unwrap();
    assert_eq!(candidate.lifecycle_state, InformationLifecycle::Draft);
    assert!(
        app.search_information(&a.id.0, &scope, "sapphire")
            .await
            .unwrap()
            .is_empty()
    );
    app.transition_information(
        &a.id.0,
        "MEMORY",
        &candidate.id,
        InformationLifecycle::Active,
        1,
    )
    .await
    .unwrap();
    let mut secret = input.clone();
    secret.sensitivity = Sensitivity::Secret;
    secret.content = "shipping PRIVATE_CANARY".into();
    let secret = app.create_knowledge(&a.id.0, secret).await.unwrap();
    app.transition_information(
        &a.id.0,
        "KNOWLEDGE",
        &secret.id,
        InformationLifecycle::Active,
        1,
    )
    .await
    .unwrap();
    let mut unsafe_text = input.clone();
    unsafe_text.content = "shipping password=canary-not-real".into();
    assert!(app.create_knowledge(&a.id.0, unsafe_text).await.is_err());
    let mut invalid = input;
    invalid.scope = InformationScope::Project;
    assert!(app.create_knowledge(&a.id.0, invalid).await.is_err());
    assert!(serde_json::from_str::<InformationScope>("\"ALL\"").is_err());
    assert!(serde_json::from_str::<Sensitivity>("\"UNKNOWN\"").is_err());
    assert!(serde_json::from_str::<InformationLifecycle>("\"UNKNOWN\"").is_err());
    assert!(
        sqlx::query("UPDATE memory_records SET visibility='ALL' WHERE id=?")
            .bind(&candidate.id)
            .execute(&app.pool)
            .await
            .is_err()
    );
    let old_versions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM knowledge_item_versions WHERE entity_id=?")
            .bind(&draft.id)
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(old_versions, 2);
    assert!(
        sqlx::query("UPDATE knowledge_item_versions SET content_hash='broken' WHERE entity_id=?")
            .bind(&draft.id)
            .execute(&app.pool)
            .await
            .is_err()
    );
    // A stale/poisoned derived index cannot resurrect retired canonical information.
    sqlx::query("INSERT INTO information_fts(company_id,kind,entity_id,version_id,title,body) VALUES(?,'KNOWLEDGE',?,?, 'sapphire','sapphire stale')").bind(&a.id.0).bind(&draft.id).bind(&draft.version_id).execute(&app.pool).await.unwrap();
    let visible = app
        .search_information(&a.id.0, &scope, "sapphire")
        .await
        .unwrap();
    assert!(visible.iter().all(|r| r.id == candidate.id));
    assert!(
        app.search_information(
            &a.id.0,
            &RetrievalScope {
                project_id: Some("foreign".into()),
                agent_id: None
            },
            "shipping"
        )
        .await
        .is_err()
    );
    app.rebuild_information_index(&a.id.0).await.unwrap();
    assert!(
        app.search_information(&a.id.0, &scope, "PRIVATE_CANARY")
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        app.search_information(&a.id.0, &scope, "sapphire")
            .await
            .unwrap()
            .len(),
        1
    );
    app.pool.close().await;
    let app = ApplicationContext::init(&url)
        .await
        .unwrap()
        .with_content_store(store);
    assert_eq!(
        app.search_information(&a.id.0, &scope, "sapphire")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        app.get_information(&a.id.0, "MEMORY", &candidate.id)
            .await
            .unwrap()
            .content_ref,
        candidate.content_ref
    );
}
