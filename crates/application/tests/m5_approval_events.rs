use nalarvo_application::ApplicationContext;
use nalarvo_domain::PrincipalRef;
use nalarvo_persistence::run_migrations;
use sqlx::SqlitePool;

async fn setup() -> SqlitePool {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    let user = nalarvo_domain::UserId("0191e4b8-0001-7000-8000-000000000001".into());
    let workspace = nalarvo_domain::WorkspaceId("0191e4b8-0002-7000-8000-000000000001".into());
    nalarvo_persistence::bootstrap_personal_workspace(
        &pool,
        &user,
        &workspace,
        "u@example.com",
        "Owner",
    )
    .await
    .unwrap();
    // Decision fixtures must carry real proposal provenance and current authority.
    let fingerprint =
        ApplicationContext::compute_action_fingerprint("c", None, "filesystem.write", "file", None);
    for statement in [
        "INSERT INTO companies(id,workspace_id,name,status,created_at,updated_at) VALUES('c','0191e4b8-0002-7000-8000-000000000001','C','ACTIVE','t','t')",
        "INSERT INTO departments(id,company_id,name,created_at,updated_at) VALUES('d','c','D','t','t')",
        "INSERT INTO roles(id,company_id,name,created_at,updated_at) VALUES('role','c','R','t','t')",
        "INSERT INTO department_roles(company_id,department_id,role_id,created_at) VALUES('c','d','role','t')",
        "INSERT INTO agents(id,company_id,name,primary_department_id,role_id,capacity,created_at,updated_at) VALUES('agent','c','A','d','role',1,'t','t')",
        "INSERT INTO projects(id,company_id,name,priority,status,created_at,updated_at) VALUES('p','c','P','MEDIUM','ACTIVE','t','t')",
        "INSERT INTO work_items(id,company_id,project_id,title,status,logical_type,created_at,updated_at) VALUES('wi','c','p','W','IN_PROGRESS','TASK','t','t')",
        "INSERT INTO runs(id,company_id,project_id,work_item_id,executing_agent_id,lifecycle_state,trigger_type,attempt_number,requested_by_type,requested_by_id,queued_at,correlation_id,created_at,updated_at) VALUES('run','c','p','wi','agent','WAITING_APPROVAL','MANUAL',1,'USER','user','2026-01-01T00:00:00Z','corr','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
        "INSERT INTO execution_steps(id,company_id,run_id,sequence_no,step_type,lifecycle_state,created_at) VALUES('step','c','run',1,'TOOL_OPERATION','RUNNING','t')",
        "INSERT INTO actions(id,workspace_id,company_id,run_id,step_id,requested_by_type,requested_by_id,action_type,target,authorization,status,created_at,updated_at) VALUES('action','0191e4b8-0002-7000-8000-000000000001','c','run','step','AGENT','agent','filesystem.write','file','APPROVAL_REQUIRED','PENDING','t','t')",
        "INSERT INTO approval_requests(id,action_id,workspace_id,company_id,requested_by_id,status,requested_at) VALUES('approval','action','0191e4b8-0002-7000-8000-000000000001','c','agent','PENDING','t')",
        "INSERT INTO runtime_checkpoints(id,company_id,run_id,checkpoint_version,run_state,execution_phase,continuation_metadata,safe_to_resume,created_at) VALUES('checkpoint','c','run',1,'WAITING_APPROVAL','TOOL_OPERATION','{\"action_id\":\"action\",\"approval_id\":\"approval\"}',1,'t')",
        "INSERT INTO durable_jobs(id,job_type,company_id,run_id,payload,available_at,status,correlation_id,created_at,updated_at) VALUES('job','EXECUTE_RUN','c','run','{}','t','WAITING_APPROVAL','corr','t','t')",
        "INSERT INTO permission_definitions(id,workspace_id,name,resource,operation,created_at) VALUES('pd','0191e4b8-0002-7000-8000-000000000001','filesystem.write','filesystem','write','t')",
        "INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('pg','0191e4b8-0002-7000-8000-000000000001','c','pd','AGENT','agent','COMPANY','c','ALLOW','t')",
    ] {
        sqlx::query(statement).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO domain_events(id,event_type,schema_version,company_id,aggregate_type,aggregate_id,aggregate_version,principal_type,principal_id,scope_type,scope_id,payload,occurred_at,created_at,correlation_id,causation_id) VALUES('proposed','ActionProposed',1,'c','Action','action',1,'AGENT','agent','COMPANY','c',?,'t','t','corr','cause')")
        .bind(serde_json::json!({"fingerprint":fingerprint}).to_string()).execute(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn approval_terminal_transitions_emit_atomic_truthful_events() {
    for expire in [true, false] {
        let pool = setup().await;
        let app = ApplicationContext::new(pool.clone());
        if expire {
            app.expire_approval("c", "approval", Some("timeout".into()))
                .await
                .unwrap();
        } else {
            app.cancel_approval(
                "c",
                "approval",
                PrincipalRef::user("approver"),
                Some("stop".into()),
            )
            .await
            .unwrap();
        }
        let expected = if expire {
            ["ApprovalExpired", "ActionExpired", "RunCancelled"]
        } else {
            ["ApprovalCancelled", "ActionCancelled", "RunCancelled"]
        };
        let events: Vec<(String, String, String, String, String)> = sqlx::query_as(
            "SELECT e.event_type,e.aggregate_type,e.aggregate_id,e.principal_type,e.payload FROM domain_events e JOIN outbox_messages o ON o.domain_event_id=e.id WHERE e.event_type IN ('ApprovalExpired','ApprovalCancelled','ActionExpired','ActionCancelled','RunCancelled') ORDER BY e.rowid"
        ).fetch_all(&pool).await.unwrap();
        assert_eq!(
            events.iter().map(|e| e.0.as_str()).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            (events[0].1.as_str(), events[0].2.as_str()),
            ("Approval", "approval")
        );
        assert_eq!(
            (events[1].1.as_str(), events[1].2.as_str()),
            ("Action", "action")
        );
        assert_eq!((events[2].1.as_str(), events[2].2.as_str()), ("Run", "run"));
        assert_eq!(events[0].3, if expire { "SYSTEM" } else { "USER" });
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&events[0].4).unwrap()["reason"],
            if expire { "timeout" } else { "stop" }
        );
        let audit: Vec<String> =
            sqlx::query_scalar("SELECT action FROM audit_records ORDER BY rowid")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(audit, expected);
        assert_eq!(
            sqlx::query_scalar::<_, String>(
                "SELECT run_status FROM runtime_results WHERE run_id='run'"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            "CANCELLED"
        );
        let states: (String, String, String, String, String, i64, String, i64) = sqlx::query_as(
            "SELECT p.status,a.authorization,a.status,r.lifecycle_state,j.status,c.safe_to_resume,s.lifecycle_state,r.row_version FROM approval_requests p JOIN actions a ON a.id=p.action_id JOIN runs r ON r.id=a.run_id JOIN durable_jobs j ON j.run_id=r.id JOIN runtime_checkpoints c ON c.run_id=r.id JOIN execution_steps s ON s.id=a.step_id WHERE p.id='approval'"
        ).fetch_one(&pool).await.unwrap();
        assert_eq!(states.0, if expire { "EXPIRED" } else { "CANCELLED" });
        assert_eq!(states.1, if expire { "EXPIRED" } else { "CANCELLED" });
        assert_eq!(states.2, if expire { "FAILED" } else { "CANCELLED" });
        assert_eq!(states.3, "CANCELLED");
        assert_eq!(states.4, "CANCELLED");
        assert_eq!(states.5, 0);
        assert_eq!(states.6, "CANCELLED");
        assert_eq!(states.7, 2);
        assert!(if expire {
            app.expire_approval("c", "approval", None).await.is_err()
        } else {
            app.cancel_approval("c", "approval", PrincipalRef::user("approver"), None)
                .await
                .is_err()
        });
        assert_eq!(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM domain_events WHERE aggregate_id IN ('approval','action','run')")
            .fetch_one(&pool).await.unwrap(), 4);
    }
}

#[tokio::test]
async fn non_runtime_approval_closes_without_run_event() {
    for expire in [true, false] {
        let pool = setup().await;
        for statement in [
            "INSERT INTO actions(id,workspace_id,company_id,requested_by_type,requested_by_id,action_type,target,authorization,status,created_at,updated_at) VALUES('standalone','0191e4b8-0002-7000-8000-000000000001','c','USER','u','other','item','APPROVAL_REQUIRED','PENDING','t','t')",
            "INSERT INTO approval_requests(id,action_id,workspace_id,company_id,requested_by_id,status,requested_at) VALUES('standalone-approval','standalone','0191e4b8-0002-7000-8000-000000000001','c','u','PENDING','t')",
        ] {
            sqlx::query(statement).execute(&pool).await.unwrap();
        }
        let app = ApplicationContext::new(pool.clone());
        if expire {
            app.expire_approval("c", "standalone-approval", None)
                .await
                .unwrap();
        } else {
            app.cancel_approval("c", "standalone-approval", PrincipalRef::user("u"), None)
                .await
                .unwrap();
        }
        let events: Vec<String> = sqlx::query_scalar("SELECT event_type FROM domain_events WHERE aggregate_id IN ('standalone','standalone-approval') ORDER BY rowid")
            .fetch_all(&pool).await.unwrap();
        assert_eq!(
            events,
            if expire {
                vec!["ApprovalExpired", "ActionExpired"]
            } else {
                vec!["ApprovalCancelled", "ActionCancelled"]
            }
        );
        let run: String = sqlx::query_scalar("SELECT lifecycle_state FROM runs WHERE id='run'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(run, "WAITING_APPROVAL");
    }
}

#[tokio::test]
async fn changed_run_rolls_back_approval_and_events() {
    let pool = setup().await;
    sqlx::query("UPDATE runs SET lifecycle_state='CANCELLED' WHERE id='run'")
        .execute(&pool)
        .await
        .unwrap();
    let app = ApplicationContext::new(pool.clone());
    assert!(app.expire_approval("c", "approval", None).await.is_err());
    assert!(
        app.cancel_approval("c", "approval", PrincipalRef::user("approver"), None)
            .await
            .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM approval_requests WHERE id='approval'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "PENDING"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM domain_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn decision_events_audits_and_cancelled_result_are_atomic() {
    for approved in [true, false] {
        let pool = setup().await;
        let fingerprint = ApplicationContext::compute_action_fingerprint(
            "c",
            None,
            "filesystem.write",
            "file",
            None,
        );
        sqlx::query("UPDATE runtime_checkpoints SET continuation_metadata=json_set(continuation_metadata,'$.fingerprint',?) WHERE run_id='run'")
            .bind(&fingerprint).execute(&pool).await.unwrap();
        let app = ApplicationContext::new(pool.clone());
        app.decide_action_approval(
            "c",
            "approval",
            PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001"),
            &fingerprint,
            approved,
            None,
        )
        .await
        .unwrap();
        let expected = if approved {
            [
                "ActionProposed",
                "ApprovalGranted",
                "ActionApproved",
                "RunQueued",
            ]
        } else {
            [
                "ActionProposed",
                "ApprovalRejected",
                "ActionRejected",
                "RunCancelled",
            ]
        };
        let events: Vec<String> =
            sqlx::query_scalar("SELECT event_type FROM domain_events ORDER BY rowid")
                .fetch_all(&pool)
                .await
                .unwrap();
        let audits: Vec<String> =
            sqlx::query_scalar("SELECT action FROM audit_records ORDER BY rowid")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(events, expected);
        for action in expected[1..3]
            .iter()
            .copied()
            .chain((!approved).then_some("RunCancelled"))
        {
            assert!(
                audits.contains(&action.to_string()),
                "missing audit: {action}"
            );
        }
        let result: Option<String> =
            sqlx::query_scalar("SELECT run_status FROM runtime_results WHERE run_id='run'")
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert_eq!(
            result.as_deref(),
            if approved { None } else { Some("CANCELLED") }
        );
    }
}

#[tokio::test]
async fn missing_job_rolls_back_decision_and_events() {
    let pool = setup().await;
    let fingerprint =
        ApplicationContext::compute_action_fingerprint("c", None, "filesystem.write", "file", None);
    sqlx::query("UPDATE runtime_checkpoints SET continuation_metadata=json_set(continuation_metadata,'$.fingerprint',?) WHERE run_id='run'")
        .bind(&fingerprint).execute(&pool).await.unwrap();
    sqlx::query("DELETE FROM durable_jobs WHERE run_id='run'")
        .execute(&pool)
        .await
        .unwrap();
    let app = ApplicationContext::new(pool.clone());
    assert!(
        app.decide_action_approval(
            "c",
            "approval",
            PrincipalRef::user("0191e4b8-0001-7000-8000-000000000001"),
            &fingerprint,
            false,
            None
        )
        .await
        .is_err()
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM approval_requests WHERE id='approval'")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "PENDING"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM domain_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}
