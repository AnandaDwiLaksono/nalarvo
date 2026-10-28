use nalarvo_domain::{UserId, WorkspaceId};
use nalarvo_persistence::{create_pool, run_migrations};
use sqlx::migrate::Migrator;

#[tokio::test]
async fn m6_upgrade_0018_matches_fresh_and_preserves_data_checksums_and_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("m6");
    std::fs::create_dir(&legacy).unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".sql") && name.as_str() < "0018" {
            std::fs::copy(entry.path(), legacy.join(name)).unwrap();
        }
    }
    let pool = create_pool(&format!(
        "sqlite://{}",
        dir.path().join("upgrade.db").display()
    ))
    .await
    .unwrap();
    Migrator::new(legacy)
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();
    nalarvo_persistence::bootstrap_personal_workspace(
        &pool,
        &UserId("owner".into()),
        &WorkspaceId("ws".into()),
        "migration@example.invalid",
        "Owner",
    )
    .await
    .unwrap();
    for sql in [
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES ('c','ws','M6','ACTIVE','t','t')",
        "INSERT INTO projects(id,company_id,name,status,created_at,updated_at) VALUES ('p','c','M6 Project','ACTIVE','t','t')",
        "INSERT INTO work_items(id,company_id,project_id,title,logical_type,status,created_at,updated_at) VALUES ('w','c','p','Accepted M6','TASK','COMPLETED','t','t')",
        "INSERT INTO content_objects(company_id,hash,size_bytes,created_at) VALUES ('c','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',4,'t')",
        "INSERT INTO artifacts(id,company_id,title,artifact_type,project_id,work_item_id,created_at,updated_at) VALUES ('a','c','M6 result','DOCUMENT','p','w','t','t')",
        "INSERT INTO artifact_versions(id,artifact_id,company_id,version_no,content_hash,media_type,sensitivity,provenance,created_at) VALUES ('v','a','c',1,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','text/plain','INTERNAL','{}','t')",
        "UPDATE artifacts SET current_version_id='v' WHERE id='a'",
    ] {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }
    let queries = [
        "SELECT * FROM companies",
        "SELECT * FROM projects",
        "SELECT * FROM work_items",
        "SELECT * FROM artifacts",
        "SELECT * FROM artifact_versions",
        "SELECT * FROM content_objects",
    ];
    // SQLite quote() preserves all column values including NULL and blobs without fixture-specific projections.
    async fn snapshot(pool: &sqlx::SqlitePool, query: &str) -> Vec<Vec<String>> {
        use sqlx::{Column, Row};
        let rows = sqlx::query(query).fetch_all(pool).await.unwrap();
        let mut values = Vec::new();
        for row in rows {
            let columns = row
                .columns()
                .iter()
                .map(|c| format!("quote({})", c.name()))
                .collect::<Vec<_>>()
                .join(",");
            let table = query.strip_prefix("SELECT * FROM ").unwrap();
            for quoted in sqlx::query(&format!("SELECT {columns} FROM {table}"))
                .fetch_all(pool)
                .await
                .unwrap()
            {
                values.push(
                    (0..quoted.len())
                        .map(|i| quoted.get::<String, _>(i))
                        .collect(),
                );
            }
        }
        values
    }
    let mut before = Vec::new();
    for query in queries {
        before.push(snapshot(&pool, query).await);
    }
    let checksums: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(checksums.len(), 16);
    run_migrations(&pool).await.unwrap();
    run_migrations(&pool).await.unwrap();
    assert_eq!(
        checksums,
        sqlx::query_as::<_, (i64, Vec<u8>)>(
            "SELECT version,checksum FROM _sqlx_migrations WHERE version<18 ORDER BY version"
        )
        .fetch_all(&pool)
        .await
        .unwrap()
    );
    for (i, query) in queries.iter().enumerate() {
        assert_eq!(
            before[i],
            snapshot(&pool, query).await,
            "M6 data changed: {query}"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT version FROM _sqlx_migrations WHERE version=18 AND success=1"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        18
    );
    let fresh = create_pool(&format!(
        "sqlite://{}",
        dir.path().join("fresh.db").display()
    ))
    .await
    .unwrap();
    run_migrations(&fresh).await.unwrap();
    let schema =
        "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name";
    assert_eq!(
        sqlx::query_as::<_, (String, String, Option<String>)>(schema)
            .fetch_all(&pool)
            .await
            .unwrap(),
        sqlx::query_as::<_, (String, String, Option<String>)>(schema)
            .fetch_all(&fresh)
            .await
            .unwrap()
    );
    for db in [&pool, &fresh] {
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                .fetch_one(db)
                .await
                .unwrap(),
            1
        );
        assert!(
            sqlx::query("PRAGMA foreign_key_check")
                .fetch_all(db)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("PRAGMA integrity_check")
                .fetch_one(db)
                .await
                .unwrap(),
            "ok"
        );
    }
    pool.close().await;
    fresh.close().await;
}
