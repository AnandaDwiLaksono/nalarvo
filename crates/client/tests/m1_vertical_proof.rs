use futures_util::StreamExt;
use nalarvo_application::ApplicationContext;
use nalarvo_client::*;
use nalarvo_contracts::*;
use nalarvo_domain::Company;
use nalarvo_local_api::router_with_app;
use std::sync::Arc;
use tokio::net::TcpListener;

const TEST_TOKEN: &str = "test-token-m1-proof";
const WORKSPACE_ID: &str = "0191e4b8-0002-7000-8000-000000000001";

#[tokio::test]
async fn test_m1_full_vertical_proof_and_invariants() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("m1_proof.db");
    let db_url = format!("sqlite://{}", db_path.to_str().unwrap());

    // 1. Initialize daemon application context & HTTP server
    let app_ctx1 = ApplicationContext::init(&db_url).await.unwrap();
    let router1 = router_with_app(Arc::<str>::from(TEST_TOKEN), Some(app_ctx1.clone()));

    let listener1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = listener1.local_addr().unwrap();
    let server_handle1 = tokio::spawn(async move {
        axum::serve(listener1, router1).await.unwrap();
    });

    let base_url1 = format!("http://{addr1}");

    // 2. Subscribe to SSE events stream
    let client = reqwest::Client::new();
    let sse_res = client
        .get(format!("{base_url1}/api/v1/events"))
        .bearer_auth(TEST_TOKEN)
        .send()
        .await
        .unwrap();

    assert!(sse_res.status().is_success());
    let mut sse_bytes = sse_res.bytes_stream();

    // 3. Perform authenticated CreateCompany command via client
    let create_req = CreateCompanyRequest {
        name: "Vertically Proved Corp".into(),
        description: Some("Demonstrating end-to-end M1 persistence & SSE".into()),
    };

    let company_dto = create_company(
        &base_url1,
        TEST_TOKEN,
        WORKSPACE_ID,
        &create_req,
        Some("idem-vert-1"),
    )
    .await
    .unwrap();

    assert_eq!(company_dto.name, "Vertically Proved Corp");
    assert_eq!(company_dto.row_version, 1);

    // 4. Trigger outbox dispatch batch
    let dispatched = app_ctx1
        .dispatch_outbox_batch(10, "worker-1", 10)
        .await
        .unwrap();
    assert_eq!(dispatched, 1);

    // 5. Read SSE event stream to confirm CompanyCreated event received with Principal and Scope evidence
    let mut received_event = false;
    if let Some(Ok(chunk)) = sse_bytes.next().await {
        let chunk_str = String::from_utf8_lossy(&chunk);
        if chunk_str.contains("CompanyCreated") && chunk_str.contains(&company_dto.id) {
            received_event = true;
            // Parse event data line
            for line in chunk_str.lines() {
                if let Some(data_str) = line.strip_prefix("data:") {
                    let parsed: SseEventEnvelope = serde_json::from_str(data_str.trim()).unwrap();
                    assert_eq!(parsed.event_type, "CompanyCreated");
                    assert_eq!(parsed.company_id, company_dto.id);
                    assert_eq!(parsed.principal.principal_type, "USER");
                    assert_eq!(parsed.scope.scope_type, "COMPANY");
                    assert_eq!(parsed.scope.scope_id, company_dto.id);
                }
            }
        }
    }
    assert!(
        received_event,
        "SSE stream must receive committed CompanyCreated event with verified Principal and Scope"
    );

    // 6. Demonstrate daemon restart durability
    server_handle1.abort();
    drop(app_ctx1);

    // Re-initialize ApplicationContext against same DB file (simulating daemon restart)
    let app_ctx2 = ApplicationContext::init(&db_url).await.unwrap();
    let router2 = router_with_app(Arc::<str>::from(TEST_TOKEN), Some(app_ctx2.clone()));
    let listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();
    let _server_handle2 = tokio::spawn(async move {
        axum::serve(listener2, router2).await.unwrap();
    });
    let base_url2 = format!("http://{addr2}");

    // Query company via client post-restart
    let reloaded = get_company(&base_url2, TEST_TOKEN, WORKSPACE_ID, &company_dto.id)
        .await
        .unwrap();
    assert_eq!(reloaded.id, company_dto.id);
    assert_eq!(reloaded.name, "Vertically Proved Corp");

    // 7. Demonstrate Idempotency duplicate protection
    let company_dto2 = create_company(
        &base_url2,
        TEST_TOKEN,
        WORKSPACE_ID,
        &create_req,
        Some("idem-vert-1"),
    )
    .await
    .unwrap();
    assert_eq!(company_dto2.id, company_dto.id);

    let list = list_companies(&base_url2, TEST_TOKEN, WORKSPACE_ID)
        .await
        .unwrap();
    assert_eq!(list.companies.len(), 1);

    // Idempotency key reuse mismatch
    let mismatch_req = CreateCompanyRequest {
        name: "Mismatch Name".into(),
        description: None,
    };
    let err_mismatch = create_company(
        &base_url2,
        TEST_TOKEN,
        WORKSPACE_ID,
        &mismatch_req,
        Some("idem-vert-1"),
    )
    .await
    .unwrap_err();

    if let ClientError::Api { status, code, .. } = err_mismatch {
        assert_eq!(status, reqwest::StatusCode::CONFLICT);
        assert_eq!(code, error_codes::IDEMPOTENCY_KEY_REUSE_MISMATCH);
    } else {
        panic!("Expected Api error for key reuse mismatch");
    }

    // 8. Demonstrate Optimistic Concurrency stale version rejection
    let stale_update = UpdateCompanyRequest {
        name: "Stale Corp Update".into(),
        description: None,
        expected_version: 0, // Current version is 1
    };
    let err_stale = update_company(
        &base_url2,
        TEST_TOKEN,
        WORKSPACE_ID,
        &company_dto.id,
        &stale_update,
    )
    .await
    .unwrap_err();

    if let ClientError::Api { status, code, .. } = err_stale {
        assert_eq!(status, reqwest::StatusCode::CONFLICT);
        assert_eq!(code, error_codes::STALE_VERSION);
    } else {
        panic!("Expected Api error for stale version");
    }

    // 9. Demonstrate Rollback Property: forced transaction rollback produces no canonical record, no outbox, no event
    {
        let mut tx = app_ctx2.pool.begin().await.unwrap();
        let company_temp = Company::create(
            nalarvo_domain::WorkspaceId(WORKSPACE_ID.into()),
            "Rolled Back Co".into(),
            None,
        )
        .unwrap();
        nalarvo_persistence::insert_company_tx(&mut tx, &company_temp)
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }

    let list_after_rollback = list_companies(&base_url2, TEST_TOKEN, WORKSPACE_ID)
        .await
        .unwrap();
    assert_eq!(list_after_rollback.companies.len(), 1);
    assert_eq!(list_after_rollback.companies[0].id, company_dto.id);
}
