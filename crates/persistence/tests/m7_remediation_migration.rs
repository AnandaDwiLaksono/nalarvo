use nalarvo_persistence::{create_pool, run_migrations};
use sqlx::migrate::Migrator;

#[tokio::test]
async fn m18_upgrade_preserves_legacy_unknown_accounting_and_terminal_history() {
    let root = tempfile::tempdir().unwrap();
    let legacy = root.path().join("legacy");
    std::fs::create_dir(&legacy).unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".sql") && name.as_str() < "0019" {
            std::fs::copy(entry.path(), legacy.join(name)).unwrap();
        }
    }
    let pool = create_pool(&format!(
        "sqlite://{}",
        root.path().join("upgrade.db").display()
    ))
    .await
    .unwrap();
    Migrator::new(legacy)
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();
    let before: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(before.last().unwrap().0, 18);
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@test','U','t','t')",
        "INSERT INTO workspaces(id,name,slug,owner_user_id,is_personal,created_at,updated_at) VALUES('w','W','w','u',1,'t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t')",
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('p','c','P','MEDIUM','COMPLETED','t','t')",
        "INSERT INTO work_items(id,company_id,project_id,title,logical_type,status,created_at,updated_at) VALUES('wi','c','p','W','TASK','COMPLETED','t','t')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES('dept','c','D','t','t')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES('role','c','R','t','t')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES('c','dept','role','t')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES('agent','c','A','dept','role',1,'t','t')",
        "INSERT INTO runs(id,company_id,project_id,work_item_id,executing_agent_id,lifecycle_state,trigger_type,attempt_number,requested_by_type,requested_by_id,queued_at,correlation_id,created_at,updated_at) VALUES('r','c','p','wi','agent','FAILED','MANUAL',1,'USER','u','t','r','t','t')",
        "INSERT INTO credential_refs(id,workspace_id,name,secret_locator,created_at,updated_at) VALUES('cr','w','CR','secretstore://synthetic','t','t')",
        "INSERT INTO provider_connections(id,workspace_id,credential_ref_id,name,provider_kind,status,created_at,updated_at) VALUES('pc','w','cr','PC','OPENAI_COMPATIBLE','DISABLED','t','t')",
        "INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,reserved_amount,created_at,updated_at) VALUES('b','w','c','B',10,1,'t','t')",
        "INSERT INTO budget_limits(id,budget_id,resource_type,limit_amount,reserved_amount) VALUES('bl','b','MODEL_CALL',10,1)",
        "INSERT INTO resource_reservations(id,budget_id,workspace_id,company_id,resource_type,amount,status,created_at) VALUES('model:inv:b','b','w','c','MODEL_CALL',1,'HELD','t')",
        "INSERT INTO context_snapshots(id,company_id,run_id,invocation_id,references_json,domain_versions,model_binding,prompt_hash,builder_version,estimated_tokens,created_at) VALUES('s','c','r','inv','[]','{}','{\"provider_id\":\"pc\"}','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','M6.1',0,'t')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    run_migrations(&pool).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let after: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(&after[..before.len()], before);
    assert_eq!(after.last().unwrap().0, 19);
    assert_eq!(sqlx::query_as::<_,(String,String,Option<String>)>("SELECT d.status,d.run_id,d.step_id FROM model_dispatches d JOIN resource_reservations r ON r.model_dispatch_id=d.id").fetch_one(&pool).await.unwrap(),("OUTCOME_UNKNOWN".into(),"r".into(),None));
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>("SELECT reserved_amount,spent_amount FROM budgets")
            .fetch_one(&pool)
            .await
            .unwrap(),
        (1.0, 0.0)
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM projects")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "COMPLETED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT lifecycle_state FROM runs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "FAILED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM usage_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "ok"
    );
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .unwrap()
            .is_empty()
    );
    pool.close().await;
}
