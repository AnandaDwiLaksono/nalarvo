use nalarvo_application::ApplicationContext;
use nalarvo_runtime::core_dispatch::TrustedRunContext;

pub async fn setup() -> (ApplicationContext, tempfile::TempDir, TrustedRunContext) {
    let root = tempfile::tempdir().unwrap();
    let pool = nalarvo_persistence::create_pool(&format!(
        "sqlite://{}",
        root.path().join("core.db").display()
    ))
    .await
    .unwrap();
    nalarvo_persistence::run_migrations(&pool).await.unwrap();
    for q in [
        "INSERT INTO users(id,email,full_name,created_at,updated_at) VALUES('u','u@x','U','t','t')",
        "INSERT INTO workspaces(id,owner_user_id,name,slug,created_at,updated_at) VALUES('w','u','W','w','t','t')",
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','w','C','ACTIVE','t','t')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES('d','c','D','t','t')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES('role','c','R','t','t')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES('c','d','role','t')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES('a','c','A','d','role',1,'t','t')",
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('p','c','P','MEDIUM','ACTIVE','t','t')",
        "INSERT INTO teams(id,company_id,project_id,name,status,created_at,updated_at) VALUES('team','c','p','T','ACTIVE','t','t')",
        "INSERT INTO agent_allocations(id,company_id,project_id,team_id,agent_id,status,created_at,updated_at) VALUES('alloc','c','p','team','a','ACTIVE','t','t')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES('wi','c','p','W','IN_PROGRESS','TASK','t','t')",
        "INSERT INTO assignments(id,company_id,project_id,work_item_id,agent_id,agent_allocation_id,status,assigned_at) VALUES('assignment','c','p','wi','a','alloc','ACTIVE','t')",
        "INSERT INTO runs(id,company_id,project_id,work_item_id,executing_agent_id,lifecycle_state,trigger_type,attempt_number,requested_by_type,requested_by_id,queued_at,correlation_id,row_version,created_at,updated_at) VALUES('r','c','p','wi','a','RUNNING','MANUAL',1,'USER','u','2026-01-01T00:00:00Z','corr',1,'2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO run_execution_leases(id,run_id,worker_principal_id,lease_version,acquired_at,heartbeat_at,expires_at,created_at) VALUES('l','r','worker',1,'t','t','2099-01-01T00:00:00Z','t')",
        "INSERT INTO permission_definitions(id,workspace_id,name,resource,operation,created_at) VALUES('pd','w','filesystem.write','filesystem','write','t')",
        "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('pg','w','c','pd','SYSTEM','worker','PROJECT','p','ALLOW','t')",
        "INSERT INTO provider_connections(id,workspace_id,provider_kind,name,status,row_version,created_at,updated_at) VALUES('pc','w','mock','Mock','ENABLED',1,'t','t')",
    ] {
        sqlx::query(q).execute(&pool).await.unwrap();
    }
    sqlx::query("UPDATE projects SET working_root_path=? WHERE id='p'")
        .bind(root.path().to_str().unwrap())
        .execute(&pool)
        .await
        .unwrap();
    for table in ["projects", "work_items"] {
        sqlx::query(&format!("UPDATE {table} SET created_at='2026-01-01T00:00:00Z', updated_at='2026-01-01T00:00:00Z'"))
            .execute(&pool).await.unwrap();
    }
    let ctx = TrustedRunContext {
        run_id: "r".into(),
        company_id: "c".into(),
        project_id: "p".into(),
        work_item_id: "wi".into(),
        agent_id: "a".into(),
        lease_id: "l".into(),
        lease_version: 1,
        worker_principal_id: "worker".into(),
        live_lease_version: None,
    };
    let lease = nalarvo_persistence::m4::ExecutionLease {
        id: ctx.lease_id.clone(),
        run_id: ctx.run_id.clone(),
        worker_principal_id: ctx.worker_principal_id.clone(),
        lease_version: 1,
        expires_at: chrono::Utc::now(),
    };
    let step = nalarvo_persistence::m4::resolve_tool_step(
        &pool,
        &lease,
        &nalarvo_domain::CompanyId("c".into()),
    )
    .await
    .unwrap();
    sqlx::query("UPDATE execution_steps SET input_metadata=? WHERE id=?")
        .bind(serde_json::json!({"provider_connection_id":"pc"}).to_string())
        .bind(step)
        .execute(&pool)
        .await
        .unwrap();
    (ApplicationContext::new(pool), root, ctx)
}

pub fn executable() -> &'static std::path::Path {
    static EXE: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    EXE.get_or_init(|| {
        let root = tempfile::tempdir().unwrap().keep();
        let executable = root.join("trusted-fixture.exe");
        assert!(
            std::process::Command::new("rustc")
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/support/shell_fixture.rs"
                ))
                .args(["--edition=2024", "-o"])
                .arg(&executable)
                .status()
                .unwrap()
                .success()
        );
        executable
    })
}

pub fn policy(args: &[String], timeout: u64) -> nalarvo_runtime::shell::ShellPolicy {
    nalarvo_runtime::shell::ShellPolicy::from_json(
        &serde_json::json!([
            {"executable": executable(), "args": args, "timeout_ms": timeout}
        ])
        .to_string(),
    )
    .unwrap()
}

pub fn proposal(args: &[String]) -> nalarvo_runtime::worker::ToolProposal {
    let exe = executable().to_str().unwrap();
    nalarvo_runtime::worker::ToolProposal {
        call_id: Some("shell-command".into()),
        tool_name: "shell.execute".into(),
        target: exe.into(),
        arguments: serde_json::json!({"executable":exe,"args":args}),
    }
}

pub async fn grant(app: &ApplicationContext) {
    sqlx::query("UPDATE permission_definitions SET name='shell.execute',resource='shell',operation='execute' WHERE id='pd'")
        .execute(&app.pool).await.unwrap();
}
