use nalarvo_persistence::create_pool;
use sqlx::{Row, migrate::Migrator};
use tempfile::tempdir;

#[tokio::test]
async fn m4_migrations_create_durable_runtime_tables_without_m5_tables() {
    let dir = tempdir().unwrap();
    let pool = create_pool(&format!("sqlite://{}", dir.path().join("m4.db").display()))
        .await
        .unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let legacy = dir.path().join("m4");
    std::fs::create_dir(&legacy).unwrap();
    let mut selected = Vec::new();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".sql") && name.as_str() < "0012" {
            selected.push(name.clone());
            std::fs::copy(entry.path(), legacy.join(name)).unwrap();
        }
    }
    selected.sort();
    assert_eq!(
        selected.iter().map(|name| &name[..4]).collect::<Vec<_>>(),
        [
            "0001", "0002", "0003", "0004", "0005", "0006", "0007", "0009", "0010", "0011"
        ]
    );
    Migrator::new(legacy)
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();

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
        "policy_versions",
        "authority_decisions",
        "authority_decision_policies",
        "authority_decision_grants",
        "approval_requests",
        "approval_decisions",
        "tool_invocations",
        "budgets",
        "budget_limits",
        "resource_reservations",
        "audit_records",
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
