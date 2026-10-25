use nalarvo_application::{
    ApplicationContext, CreateCompanyCommand,
    content_store::{ContentStore, LocalContentStore},
    m6::ArtifactInput,
};
use nalarvo_domain::{
    WorkspaceId,
    information::{ContentRef, Sensitivity},
};
use std::{io, path::Path, sync::Arc};
struct FailedDisk;
impl ContentStore for FailedDisk {
    fn put(&self, _: &[u8]) -> io::Result<ContentRef> {
        Err(io::Error::other("injected disk failure"))
    }
    fn read(&self, _: &ContentRef) -> io::Result<Vec<u8>> {
        Err(io::Error::other("injected disk failure"))
    }
    fn permits_project_root(&self, _: &Path) -> bool {
        false
    }
}
#[tokio::test]
async fn disk_failure_has_no_metadata_and_recovery_retains_committed_content() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}", dir.path().join("db").display());
    let app = ApplicationContext::init(&url)
        .await
        .unwrap()
        .with_content_store(Arc::new(FailedDisk));
    let c = app
        .create_company(CreateCompanyCommand {
            workspace_id: WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into()),
            name: "Disk".into(),
            description: None,
            principal: None,
            idempotency_key: None,
            correlation_id: None,
            causation_id: None,
        })
        .await
        .unwrap();
    let input = ArtifactInput {
        title: "Report".into(),
        artifact_type: "DOCUMENT".into(),
        project_id: None,
        work_item_id: None,
        run_id: None,
        agent_id: None,
        sensitivity: Sensitivity::Internal,
        media_type: "text/plain".into(),
        content: "recoverable".into(),
    };
    assert!(app.create_artifact(&c.id.0, input.clone()).await.is_err());
    for table in ["content_objects", "artifacts", "artifact_versions"] {
        let n: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&app.pool)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }
    let store = Arc::new(LocalContentStore::open(&dir.path().join("content")).unwrap());
    let app = app.with_content_store(store.clone());
    let v = app.create_artifact(&c.id.0, input).await.unwrap();
    std::fs::write(
        dir.path()
            .join("content")
            .join(".nalarvo-write-crash-residue"),
        b"partial",
    )
    .unwrap();
    app.pool.close().await;
    drop(app);
    drop(store);
    let store = Arc::new(LocalContentStore::open(&dir.path().join("content")).unwrap());
    let app = ApplicationContext::init(&url)
        .await
        .unwrap()
        .with_content_store(store);
    assert_eq!(
        app.read_artifact_content(&c.id.0, &v.artifact_id, &v.id)
            .await
            .unwrap(),
        b"recoverable"
    );
    // Crash residue is unreachable; recovery intentionally does not run destructive GC.
    assert_eq!(app.list_artifacts(&c.id.0).await.unwrap().len(), 1);
    assert!(
        app.read_artifact_content(&c.id.0, &v.artifact_id, ".nalarvo-write-crash-residue")
            .await
            .is_err()
    );
}
