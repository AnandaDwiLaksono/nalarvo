use nalarvo_application::{
    ApplicationContext, CreateCompanyCommand,
    content_store::{ContentStore, LocalContentStore},
    m6::ArtifactInput,
};
use nalarvo_domain::{WorkspaceId, information::Sensitivity};
use std::sync::Arc;

#[tokio::test]
async fn immutable_versions_integrity_isolation_atomicity_restart() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("state.db").display());
    let store = Arc::new(LocalContentStore::open(&dir.path().join("content")).unwrap());
    assert!(!store.permits_project_root(dir.path()));
    assert!(!store.permits_project_root(&dir.path().join("content")));
    assert!(LocalContentStore::open(std::path::Path::new("relative")).is_err());
    let app = ApplicationContext::init(&url)
        .await
        .unwrap()
        .with_content_store(store.clone());
    let ws = WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    let company = |workspace_id, name: &str| CreateCompanyCommand {
        workspace_id,
        name: name.into(),
        description: None,
        principal: None,
        idempotency_key: None,
        correlation_id: None,
        causation_id: None,
    };
    let a = app.create_company(company(ws.clone(), "A")).await.unwrap();
    let b = app.create_company(company(ws, "B")).await.unwrap();
    let input = ArtifactInput {
        title: "Report".into(),
        artifact_type: "DOCUMENT".into(),
        project_id: None,
        work_item_id: None,
        run_id: None,
        agent_id: None,
        sensitivity: Sensitivity::Internal,
        media_type: "text/plain".into(),
        content: "first".into(),
    };
    let v1 = app.create_artifact(&a.id.0, input.clone()).await.unwrap();
    let mut next = input.clone();
    next.content = "second".into();
    let v2 = app
        .add_artifact_version(&a.id.0, &v1.artifact_id, next)
        .await
        .unwrap();
    assert_eq!((v1.version_no, v2.version_no), (1, 2));
    assert_eq!(
        app.read_artifact_content(&a.id.0, &v1.artifact_id, &v1.id)
            .await
            .unwrap(),
        b"first"
    );
    assert_eq!(
        app.read_artifact_content(&a.id.0, &v2.artifact_id, &v2.id)
            .await
            .unwrap(),
        b"second"
    );
    assert!(
        app.artifact_versions(&b.id.0, &v1.artifact_id)
            .await
            .is_err()
    );
    assert!(
        app.read_artifact_content(&b.id.0, &v1.artifact_id, &v1.id)
            .await
            .is_err()
    );
    let mut bad = input.clone();
    bad.project_id = Some("foreign".into());
    assert!(app.create_artifact(&a.id.0, bad).await.is_err());
    assert!(
        store
            .read(&nalarvo_domain::information::ContentRef {
                hash: "../state.db".into(),
                size_bytes: 1
            })
            .is_err()
    );
    sqlx::query("CREATE TRIGGER fail_version BEFORE INSERT ON artifact_versions BEGIN SELECT RAISE(ABORT,'injected'); END").execute(&app.pool).await.unwrap();
    let mut orphan_input = input.clone();
    orphan_input.content = "uncommitted orphan".into();
    assert!(app.create_artifact(&a.id.0, orphan_input).await.is_err());
    let registered: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM content_objects")
        .fetch_one(&app.pool)
        .await
        .unwrap();
    assert_eq!(registered, 2);
    assert_eq!(app.list_artifacts(&a.id.0).await.unwrap().len(), 1);
    sqlx::query("DROP TRIGGER fail_version")
        .execute(&app.pool)
        .await
        .unwrap();
    assert!(
        sqlx::query("UPDATE artifact_versions SET version_no=9 WHERE id=?")
            .bind(&v1.id)
            .execute(&app.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("DELETE FROM content_objects WHERE company_id=? AND hash=?")
            .bind(&a.id.0)
            .bind(&v1.content_ref.hash)
            .execute(&app.pool)
            .await
            .is_err()
    );
    let (third, fourth) = tokio::join!(
        app.add_artifact_version(&a.id.0, &v1.artifact_id, input.clone()),
        app.add_artifact_version(&a.id.0, &v1.artifact_id, input)
    );
    let mut numbers = vec![third.unwrap().version_no, fourth.unwrap().version_no];
    numbers.sort();
    assert_eq!(numbers, vec![3, 4]);
    app.pool.close().await;
    let reopened = ApplicationContext::init(&url)
        .await
        .unwrap()
        .with_content_store(store.clone());
    assert_eq!(
        reopened
            .artifact_versions(&a.id.0, &v1.artifact_id)
            .await
            .unwrap()
            .len(),
        4
    );
    assert_eq!(store.read(&v1.content_ref).unwrap(), b"first");
    #[cfg(windows)]
    {
        // Handle-pinned root cannot be moved; linked blobs never bypass the store.
        assert!(std::fs::rename(dir.path().join("content"), dir.path().join("moved")).is_err());
        let alias = dir.path().join("alias");
        match std::fs::hard_link(
            dir.path().join("content").join(&v1.content_ref.hash),
            &alias,
        ) {
            Ok(()) => {
                assert!(store.read(&v1.content_ref).is_err());
                std::fs::remove_file(alias).unwrap();
            }
            Err(_) => {
                assert!(!alias.exists());
                assert_eq!(store.read(&v1.content_ref).unwrap(), b"first");
            }
        }
    }
    std::fs::remove_file(dir.path().join("content").join(&v2.content_ref.hash)).unwrap();
    assert!(
        reopened
            .read_artifact_content(&a.id.0, &v2.artifact_id, &v2.id)
            .await
            .is_err()
    );
    // Simulate disk corruption; never return unverified bytes.
    std::fs::write(
        dir.path().join("content").join(&v1.content_ref.hash),
        b"wrong",
    )
    .unwrap();
    assert!(
        reopened
            .read_artifact_content(&a.id.0, &v1.artifact_id, &v1.id)
            .await
            .is_err()
    );
}
