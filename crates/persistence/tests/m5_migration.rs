use nalarvo_persistence::{create_pool, run_migrations};
use sqlx::{Row, migrate::Migrator};
use tempfile::tempdir;

#[tokio::test]
async fn historical_0012_upgrades_without_rewriting_checksums_and_unknown_sql_is_rejected() {
    use sha2::{Digest, Sha384};
    // Historical SQL is pinned independently of the current embedded migrator.
    let current = include_str!("../../../migrations/0012_governance.sql");
    let historical = current.replace("details TEXT NOT NULL);\n", "details TEXT NOT NULL); \n");
    let checksum = Sha384::digest(historical.as_bytes()).to_vec();
    assert_eq!(
        hex::encode(&checksum),
        "d34ec05d9c6f9edcfd7ffdd9924b5aa54f4de5997b0711c9a626b77634e1772ff055619376cda5e7757f5083a7706617"
    );
    for known in [true, false] {
        let root = tempdir().unwrap();
        let legacy = root.path().join("legacy");
        std::fs::create_dir(&legacy).unwrap();
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".sql") && name.as_str() < "0012" {
                std::fs::copy(entry.path(), legacy.join(name)).unwrap();
            }
        }
        let sql = if known {
            historical.clone()
        } else {
            format!("{historical}-- unknown migration bytes\n")
        };
        std::fs::write(legacy.join("0012_governance.sql"), sql).unwrap();
        let url = format!("sqlite://{}", root.path().join("historical.db").display());
        let pool = create_pool(&url).await.unwrap();
        Migrator::new(legacy)
            .await
            .unwrap()
            .run(&pool)
            .await
            .unwrap();
        for q in [
            "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','t','t')",
            "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES('w','u','W','w','t','t')",
            "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t')",
            "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('p','c','P','MEDIUM','ACTIVE','t','t')",
            "INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,reserved_amount,spent_amount,created_at,updated_at) VALUES('b','w','c','B',10,2,3,'t','t')",
            "INSERT INTO budget_limits(id,budget_id,resource_type,limit_amount,reserved_amount,spent_amount) VALUES('l','b','TOOL_CALL',10,2,3)",
            "INSERT INTO resource_reservations(id,budget_id,workspace_id,company_id,resource_type,amount,created_at) VALUES('r','b','w','c','TOOL_CALL',2,'t')",
        ] {
            sqlx::query(q).execute(&pool).await.unwrap();
        }
        let before: Vec<(i64, Vec<u8>, bool)> = sqlx::query_as(
            "SELECT version,checksum,success FROM _sqlx_migrations ORDER BY version",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(before.last().unwrap().0, 12);
        assert_eq!(before.last().unwrap().1 == checksum, known);
        let mut data = Vec::new();
        for table in [
            "users",
            "workspaces",
            "companies",
            "projects",
            "budgets",
            "budget_limits",
            "resource_reservations",
        ] {
            let columns: Vec<String> = sqlx::query(&format!("PRAGMA table_info({table})"))
                .fetch_all(&pool)
                .await
                .unwrap()
                .iter()
                .map(|r| format!("quote(\"{}\")", r.get::<String, _>("name")))
                .collect();
            let query = format!(
                "SELECT {} FROM {table} ORDER BY id",
                columns.join(" || '|' || ")
            );
            let rows: Vec<String> = sqlx::query_scalar(&query).fetch_all(&pool).await.unwrap();
            data.push((query, rows));
        }
        pool.close().await;
        let pool = create_pool(&url).await.unwrap();
        let result = run_migrations(&pool).await;
        let after: Vec<(i64, Vec<u8>, bool)> = sqlx::query_as(
            "SELECT version,checksum,success FROM _sqlx_migrations ORDER BY version",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        if known {
            result.unwrap();
            assert_eq!(&after[..before.len()], before);
            assert_eq!(
                after
                    .iter()
                    .skip(before.len())
                    .map(|r| r.0)
                    .collect::<Vec<_>>(),
                [13, 14, 15, 16, 17]
            );
            run_migrations(&pool).await.unwrap();
            let project: Option<String> =
                sqlx::query_scalar("SELECT project_id FROM budgets WHERE id='b'")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(project, None);
            let expiry: Option<String> =
                sqlx::query_scalar("SELECT expires_at FROM approval_requests LIMIT 1")
                    .fetch_optional(&pool)
                    .await
                    .unwrap()
                    .flatten();
            assert_eq!(expiry, None);
        } else {
            assert!(matches!(
                result,
                Err(nalarvo_persistence::PersistenceError::Migration(
                    sqlx::migrate::MigrateError::VersionMismatch(12)
                ))
            ));
            assert_eq!(after, before);
        }
        for (query, rows) in data {
            let preserved: Vec<String> = sqlx::query_scalar(&query).fetch_all(&pool).await.unwrap();
            assert_eq!(preserved, rows, "{query}");
        }
        assert_eq!(
            sqlx::query_as::<_, (f64, f64, f64)>(
                "SELECT total_limit,reserved_amount,spent_amount FROM budgets WHERE id='b'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            (10.0, 2.0, 3.0)
        );
        assert_eq!(
            sqlx::query_as::<_, (f64, f64)>(
                "SELECT reserved_amount,spent_amount FROM budget_limits WHERE id='l'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            (2.0, 3.0)
        );
        assert_eq!(
            sqlx::query_as::<_, (String, f64)>(
                "SELECT status,amount FROM resource_reservations WHERE id='r'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            ("HELD".into(), 2.0)
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
    }
}

const TABLES: &[&str] = &[
    "permission_definitions",
    "permission_grants",
    "policies",
    "policy_versions",
    "authority_decisions",
    "authority_decision_policies",
    "authority_decision_grants",
    "actions",
    "approval_requests",
    "approval_decisions",
    "tool_invocations",
    "budgets",
    "budget_limits",
    "resource_reservations",
    "audit_records",
];

#[tokio::test]
async fn project_scope_upgrade_preserves_existing_budget_and_accounting() {
    let root = tempdir().unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let legacy = root.path().join("legacy");
    std::fs::create_dir(&legacy).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".sql") && name.as_str() < "0014" {
            std::fs::copy(entry.path(), legacy.join(name)).unwrap();
        }
    }
    let pool = create_pool("sqlite::memory:").await.unwrap();
    Migrator::new(legacy)
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','t','t')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES('w','u','W','w','t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t')",
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('p','c','P','MEDIUM','ACTIVE','t','t')",
        "INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,reserved_amount,spent_amount,created_at,updated_at) VALUES('b','w','c','B',10,2,3,'t','t')",
        "INSERT INTO budget_limits(id,budget_id,resource_type,limit_amount,reserved_amount,spent_amount) VALUES('l','b','TOOL_CALL',10,2,3)",
        "INSERT INTO resource_reservations(id,budget_id,workspace_id,company_id,resource_type,amount,created_at) VALUES('r','b','w','c','TOOL_CALL',2,'t')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    let before: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    run_migrations(&pool).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let after: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(&after[..before.len()], before);
    assert_eq!(
        after[before.len()..]
            .iter()
            .map(|(v, _)| *v)
            .collect::<Vec<_>>(),
        vec![14, 15, 16, 17]
    );
    let budget: (Option<String>, f64, f64, f64) = sqlx::query_as(
        "SELECT project_id,total_limit,reserved_amount,spent_amount FROM budgets WHERE id='b'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(budget, (None, 10.0, 2.0, 3.0));
    assert_eq!(
        sqlx::query_as::<_, (f64, f64)>(
            "SELECT reserved_amount,spent_amount FROM budget_limits WHERE id='l'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        (2.0, 3.0)
    );
    assert_eq!(
        sqlx::query_as::<_, (String, f64)>(
            "SELECT status,amount FROM resource_reservations WHERE id='r'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        ("HELD".into(), 2.0)
    );
    assert!(
        sqlx::query("UPDATE budgets SET project_id='missing' WHERE id='b'")
            .execute(&pool)
            .await
            .is_err()
    );
    sqlx::query("UPDATE budgets SET project_id='p' WHERE id='b'")
        .execute(&pool)
        .await
        .unwrap();
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(integrity, "ok");
    assert!(
        sqlx::query("PRAGMA foreign_key_check")
            .fetch_all(&pool)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn fresh_and_m4_upgrade_preserve_order_checksums_and_data() {
    let root = tempdir().unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    let legacy = root.path().join("legacy");
    std::fs::create_dir(&legacy).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".sql") && name.as_str() < "0012" {
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
    sqlx::query("INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@test','U','t','t')").execute(&pool).await.unwrap();
    let prior: Vec<(i64, Vec<u8>)> =
        sqlx::query("SELECT version, checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
    assert_eq!(
        prior
            .iter()
            .map(|(version, _)| *version)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5, 6, 7, 9, 10, 11]
    );
    for table in TABLES {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?")
                .bind(table)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 0, "M5 table leaked into M4 upgrade: {table}");
    }
    run_migrations(&pool).await.unwrap();
    run_migrations(&pool).await.unwrap();
    let applied: Vec<(i64, Vec<u8>)> =
        sqlx::query("SELECT version, checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| (row.get(0), row.get(1)))
            .collect();
    assert_eq!(&applied[..prior.len()], prior);
    assert_eq!(
        applied[prior.len()..]
            .iter()
            .map(|(v, _)| *v)
            .collect::<Vec<_>>(),
        vec![12, 13, 14, 15, 16, 17]
    );
    assert_eq!(applied.last().unwrap().0, 17);
    let name: String = sqlx::query_scalar("SELECT email FROM users WHERE id='u'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(name, "u@test");
    for table in TABLES {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?")
                .bind(table)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count, 1, "missing {table}");
    }
    let fresh = create_pool(&format!(
        "sqlite://{}",
        root.path().join("fresh.db").display()
    ))
    .await
    .unwrap();
    run_migrations(&fresh).await.unwrap();
    let versions: Vec<i64> =
        sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&fresh)
            .await
            .unwrap();
    assert_eq!(
        versions,
        applied.iter().map(|(v, _)| *v).collect::<Vec<_>>()
    );
    for table in TABLES {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?")
                .bind(table)
                .fetch_one(&fresh)
                .await
                .unwrap();
        assert_eq!(count, 1, "fresh migration missing {table}");
    }
}
