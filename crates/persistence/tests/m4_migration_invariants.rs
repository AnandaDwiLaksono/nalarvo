use nalarvo_persistence::{create_pool, run_migrations};
use sqlx::Row;
use tempfile::tempdir;

#[tokio::test]
async fn m4_migrations_create_durable_runtime_tables_without_m5_tables() {
    let dir = tempdir().unwrap();
    let pool = create_pool(&format!("sqlite://{}", dir.path().join("m4.db").display()))
        .await
        .unwrap();
    run_migrations(&pool).await.unwrap();

    for table in [
        "runs",
        "execution_steps",
        "model_invocations",
        "runtime_checkpoints",
        "run_execution_leases",
        "runtime_results",
        "durable_jobs",
        "usage_records",
    ] {
        let exists: i64 =
            sqlx::query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?")
                .bind(table)
                .fetch_one(&pool)
                .await
                .unwrap()
                .get(0);
        assert_eq!(exists, 1, "missing {table}");
    }
    for table in [
        "actions",
        "permission_definitions",
        "permission_grants",
        "policies",
        "authority_decisions",
        "approval_requests",
        "approval_decisions",
        "tool_invocations",
    ] {
        let exists: i64 =
            sqlx::query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?")
                .bind(table)
                .fetch_one(&pool)
                .await
                .unwrap()
                .get(0);
        assert_eq!(exists, 0, "M5/M6 table leaked into M4: {table}");
    }
}
