use nalarvo_application::{ApplicationContext, CommandMeta, ProposeActionCommand};
use nalarvo_contracts::DecideApprovalRequest;
use nalarvo_domain::PrincipalRef;
use serde_json::json;

#[tokio::test]
async fn inbox_fingerprint_round_trip_rejects_stale_expired_and_foreign_decisions() {
    let app = ApplicationContext::init("sqlite::memory:").await.unwrap();
    let workspace = "0191e4b8-0002-7000-8000-000000000001";
    let owner = "0191e4b8-0001-7000-8000-000000000001";
    sqlx_seed(&app, workspace).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        axum::serve(
            listener,
            nalarvo_local_api::router_with_app("test", Some(app.clone())),
        )
        .into_future(),
    );
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,current_version,created_at,updated_at) VALUES('workspace-policy',?,NULL,'Workspace',1,'t','t')")
        .bind(workspace).execute(&app.pool).await.unwrap();
    let policies = nalarvo_client::list_policies(&base, "test", "c")
        .await
        .unwrap();
    assert_eq!(policies.len(), 2);
    assert!(
        policies
            .iter()
            .find(|p| p.id == "workspace-policy")
            .unwrap()
            .company_id
            .is_none()
    );
    sqlx::query("DELETE FROM policies WHERE id='workspace-policy'")
        .execute(&app.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO budgets(id,workspace_id,company_id,name,total_limit,created_at,updated_at) VALUES('budget',?,'c','Company',1,'t','t')")
        .bind(workspace).execute(&app.pool).await.unwrap();
    let budgets = nalarvo_client::list_budgets(&base, "test", "c")
        .await
        .unwrap();
    assert_eq!(budgets.len(), 1);
    assert!(budgets[0].project_id.is_none());
    for (index, approved) in [true, false].into_iter().enumerate() {
        let proposed = app
            .propose_action(ProposeActionCommand {
                company_id: "c".into(),
                project_id: None,
                run_id: None,
                step_id: None,
                actor: PrincipalRef::agent("agent"),
                action_type: "filesystem.write".into(),
                target: "file.txt".into(),
                parameters: Some(json!({"content":"original"})),
                idempotency_key: Some(format!("decision-{index}")),
                meta: CommandMeta::default(),
            })
            .await
            .unwrap();
        let id = proposed.approval_request_id.unwrap();
        let inbox = nalarvo_client::list_approvals(&base, "test", "c")
            .await
            .unwrap();
        let item = inbox.iter().find(|item| item.id == id).unwrap();
        let value = serde_json::to_value(item).unwrap();
        assert_eq!(value["fingerprint"], proposed.fingerprint);
        let mut request = DecideApprovalRequest {
            decided_by_type: "USER".into(),
            decided_by_id: owner.into(),
            fingerprint: "mismatch".into(),
            approved,
            reason: None,
        };
        assert!(
            nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
                .await
                .is_err()
        );
        request.fingerprint = value["fingerprint"].as_str().unwrap().into();
        assert!(
            nalarvo_client::decide_approval(&base, "test", "foreign", &id, &request)
                .await
                .is_err()
        );
        request.decided_by_id = "impostor".into();
        assert!(
            nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
                .await
                .is_err()
        );
        request.decided_by_id = owner.into();
        // The original proposal fingerprint cannot authorize a mutated Action.
        update(
            &app,
            "UPDATE actions SET target='changed' WHERE id=?",
            &proposed.action_id,
        )
        .await;
        assert!(
            nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
                .await
                .is_err()
        );
        update(
            &app,
            "UPDATE actions SET target='file.txt' WHERE id=?",
            &proposed.action_id,
        )
        .await;
        update(
            &app,
            "UPDATE actions SET input='{}' WHERE id=?",
            &proposed.action_id,
        )
        .await;
        assert!(
            nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
                .await
                .is_err()
        );
        update(
            &app,
            "UPDATE actions SET input='invalid' WHERE id=?",
            &proposed.action_id,
        )
        .await;
        assert!(
            nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
                .await
                .is_err()
        );
        update(
            &app,
            "UPDATE actions SET input='{\"content\":\"original\"}' WHERE id=?",
            &proposed.action_id,
        )
        .await;
        update(
            &app,
            "UPDATE actions SET status='STARTED' WHERE id=?",
            &proposed.action_id,
        )
        .await;
        assert!(
            nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
                .await
                .is_err()
        );
        update(
            &app,
            "UPDATE actions SET status='PENDING' WHERE id=?",
            &proposed.action_id,
        )
        .await;
        update(
            &app,
            "UPDATE approval_requests SET expires_at='invalid' WHERE id=?",
            &id,
        )
        .await;
        assert!(
            nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
                .await
                .is_err()
        );
        update(
            &app,
            "UPDATE approval_requests SET expires_at='2000-01-01T00:00:00Z' WHERE id=?",
            &id,
        )
        .await;
        assert!(
            nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
                .await
                .is_err()
        );
        update(
            &app,
            "UPDATE approval_requests SET expires_at=NULL WHERE id=?",
            &id,
        )
        .await;
        update(
            &app,
            "UPDATE permission_grants SET revoked_at='revoked' WHERE id=?",
            "grant",
        )
        .await;
        assert!(
            nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
                .await
                .is_err()
        );
        update(
            &app,
            "UPDATE permission_grants SET revoked_at=NULL WHERE id=?",
            "grant",
        )
        .await;
        let decision = nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
            .await
            .unwrap();
        assert_eq!(
            decision.decision,
            if approved { "APPROVED" } else { "REJECTED" }
        );
        assert!(
            nalarvo_client::decide_approval(&base, "test", "c", &id, &request)
                .await
                .is_err()
        );
        assert_eq!(
            app.get_approval("c", &id).await.unwrap().status,
            decision.decision
        );
    }
    server.abort();
}

use std::future::IntoFuture;

async fn update(app: &ApplicationContext, sql: &str, id: &str) {
    sqlx::query(sql).bind(id).execute(&app.pool).await.unwrap();
}

async fn sqlx_seed(app: &ApplicationContext, workspace: &str) {
    sqlx::query("INSERT INTO companies(id,workspace_id,name,status,row_version,created_at,updated_at) VALUES('c',?,'Company','ACTIVE',1,'t','t')")
        .bind(workspace).execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO permission_definitions(id,workspace_id,name,resource,operation,created_at) VALUES('permission',?,'filesystem.write','filesystem','write','t')")
        .bind(workspace).execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO permission_grants(id,workspace_id,company_id,permission_definition_id,principal_type,principal_id,scope_type,scope_id,effect,created_at) VALUES('grant',?,'c','permission','AGENT','agent','COMPANY','c','ALLOW','t')")
        .bind(workspace).execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO policies(id,workspace_id,company_id,name,status,current_version,created_at,updated_at) VALUES('policy',?,'c','Approval','ACTIVE',1,'t','t')")
        .bind(workspace).execute(&app.pool).await.unwrap();
    sqlx::query("INSERT INTO policy_versions(id,policy_id,version,rules,created_at) VALUES('version','policy',1,?,'t')")
        .bind(json!({"permission":"filesystem.write","effect":"REQUIRE_APPROVAL"}).to_string()).execute(&app.pool).await.unwrap();
}
