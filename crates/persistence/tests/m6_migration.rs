use nalarvo_persistence::{create_pool, run_migrations};
use sqlx::{Row, migrate::Migrator};
#[tokio::test]
async fn m5_upgrade_fresh_equivalence_preserves_every_historical_checksum() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join("m5");
    std::fs::create_dir(&legacy).unwrap();
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations");
    for e in std::fs::read_dir(source).unwrap() {
        let e = e.unwrap();
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with(".sql") && name.as_str() < "0015" {
            std::fs::copy(e.path(), legacy.join(name)).unwrap();
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
    let before: Vec<(i64, Vec<u8>)> =
        sqlx::query("SELECT version,checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
    assert_eq!(
        before.iter().map(|(v, _)| *v).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14]
    );
    run_migrations(&pool).await.unwrap();
    let after: Vec<(i64, Vec<u8>)> = sqlx::query(
        "SELECT version,checksum FROM _sqlx_migrations WHERE version<=14 ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect();
    assert_eq!(before, after);
    let fresh = create_pool(&format!(
        "sqlite://{}",
        dir.path().join("fresh.db").display()
    ))
    .await
    .unwrap();
    run_migrations(&fresh).await.unwrap();
    let schema =
        "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name";
    let decode = |rows: Vec<sqlx::sqlite::SqliteRow>| {
        rows.iter()
            .map(|r| {
                (
                    r.get::<String, _>(0),
                    r.get::<String, _>(1),
                    r.get::<Option<String>, _>(2),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        decode(sqlx::query(schema).fetch_all(&pool).await.unwrap()),
        decode(sqlx::query(schema).fetch_all(&fresh).await.unwrap())
    );
    let added: Vec<i64> = sqlx::query_scalar(
        "SELECT version FROM _sqlx_migrations WHERE version>14 ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(added, vec![15, 16, 17, 18, 19]);
    let fk: Vec<sqlx::sqlite::SqliteRow> = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(fk.is_empty());
}
