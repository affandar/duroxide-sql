use duroxide::providers::{ExecutionMetadata, Provider, WorkItem};
use duroxide::{Event, EventKind, INITIAL_EVENT_ID, INITIAL_EXECUTION_ID};
use duroxide_sql::MssqlProvider;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

// Initialize tracing subscriber for tests with DEBUG level
static INIT: std::sync::Once = std::sync::Once::new();

fn init_test_logging() {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| EnvFilter::new("debug")),
            )
            .with_test_writer()
            .try_init();
    });
}

/// Helper to get a unique test schema name using GUID suffix
fn get_test_schema() -> String {
    format!("test_{}", uuid::Uuid::new_v4().simple())
}

/// Helper to load database URL from environment
fn get_database_url() -> String {
    dotenvy::dotenv().ok();
    std::env::var("DATABASE_URL").expect(
        "DATABASE_URL must be set. Example: \
         server=tcp:myserver.database.windows.net,1433;database=mydb;user=myuser;password=mypass",
    )
}

#[tokio::test]
async fn test_provider_creation() {
    init_test_logging();
    
    let db_url = get_database_url();
    let schema = get_test_schema();
    
    let provider = MssqlProvider::new_with_schema(&db_url, Some(&schema))
        .await
        .expect("Failed to create provider");
    
    assert_eq!(provider.name(), "duroxide-sql");
    assert!(!provider.version().is_empty());
    
    // Cleanup
    provider.cleanup_schema().await.ok();
}

#[tokio::test]
async fn test_read_empty_instance() {
    init_test_logging();
    
    let db_url = get_database_url();
    let schema = get_test_schema();
    
    let provider = MssqlProvider::new_with_schema(&db_url, Some(&schema))
        .await
        .expect("Failed to create provider");
    
    // Reading non-existent instance should return empty history
    let history = provider.read("non-existent-instance").await.unwrap();
    assert!(history.is_empty());
    
    // Cleanup
    provider.cleanup_schema().await.ok();
}

#[tokio::test]
async fn test_enqueue_for_orchestrator() {
    init_test_logging();
    
    let db_url = get_database_url();
    let schema = get_test_schema();
    
    let provider = MssqlProvider::new_with_schema(&db_url, Some(&schema))
        .await
        .expect("Failed to create provider");
    
    let work_item = WorkItem::StartOrchestration {
        instance: "test-instance".to_string(),
        orchestration: "TestOrch".to_string(),
        version: Some("1.0.0".to_string()),
        input: "{}".to_string(),
        parent_instance: None,
        parent_id: None,
        execution_id: INITIAL_EXECUTION_ID,
    };
    
    provider
        .enqueue_for_orchestrator(work_item, None)
        .await
        .expect("Failed to enqueue");
    
    // Cleanup
    provider.cleanup_schema().await.ok();
}

#[tokio::test]
async fn test_fetch_orchestration_item() {
    init_test_logging();
    
    let db_url = get_database_url();
    let schema = get_test_schema();
    
    let provider = MssqlProvider::new_with_schema(&db_url, Some(&schema))
        .await
        .expect("Failed to create provider");
    
    // Enqueue a start orchestration item
    let work_item = WorkItem::StartOrchestration {
        instance: "test-fetch-instance".to_string(),
        orchestration: "FetchTestOrch".to_string(),
        version: Some("1.0.0".to_string()),
        input: r#"{"key": "value"}"#.to_string(),
        parent_instance: None,
        parent_id: None,
        execution_id: INITIAL_EXECUTION_ID,
    };
    
    provider
        .enqueue_for_orchestrator(work_item, None)
        .await
        .expect("Failed to enqueue");
    
    // Fetch the item
    let result = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO)
        .await
        .expect("Fetch should succeed");
    
    assert!(result.is_some(), "Should have fetched an item");
    
    let (item, lock_token, attempt_count) = result.unwrap();
    assert_eq!(item.instance, "test-fetch-instance");
    assert_eq!(item.messages.len(), 1);
    assert!(!lock_token.is_empty());
    assert_eq!(attempt_count, 1);
    
    // Cleanup
    provider.cleanup_schema().await.ok();
}

#[tokio::test]
async fn test_ack_orchestration_item() {
    init_test_logging();
    
    let db_url = get_database_url();
    let schema = get_test_schema();
    
    let provider = MssqlProvider::new_with_schema(&db_url, Some(&schema))
        .await
        .expect("Failed to create provider");
    
    // Enqueue a start orchestration item
    let work_item = WorkItem::StartOrchestration {
        instance: "test-ack-instance".to_string(),
        orchestration: "AckTestOrch".to_string(),
        version: Some("1.0.0".to_string()),
        input: "{}".to_string(),
        parent_instance: None,
        parent_id: None,
        execution_id: INITIAL_EXECUTION_ID,
    };
    
    provider
        .enqueue_for_orchestrator(work_item, None)
        .await
        .expect("Failed to enqueue");
    
    // Fetch the item
    let (item, lock_token, _) = provider
        .fetch_orchestration_item(Duration::from_secs(30), Duration::ZERO)
        .await
        .expect("Fetch should succeed")
        .expect("Should have an item");
    
    // Create history events
    let history_delta = vec![Event::with_event_id(
        INITIAL_EVENT_ID,
        "test-ack-instance".to_string(),
        INITIAL_EXECUTION_ID,
        None,
        EventKind::OrchestrationStarted {
            name: "AckTestOrch".to_string(),
            version: "1.0.0".to_string(),
            input: "{}".to_string(),
            parent_instance: None,
            parent_id: None,
        },
    )];
    
    let metadata = ExecutionMetadata {
        orchestration_name: Some("AckTestOrch".to_string()),
        orchestration_version: Some("1.0.0".to_string()),
        status: Some("Running".to_string()),
        output: None,
        parent_instance_id: None,
    };
    
    // Ack the item
    provider
        .ack_orchestration_item(
            &lock_token,
            item.execution_id,
            history_delta,
            vec![],
            vec![],
            metadata,
            vec![],
        )
        .await
        .expect("Ack should succeed");
    
    // Verify history was persisted
    let history = provider.read("test-ack-instance").await.unwrap();
    assert_eq!(history.len(), 1);
    
    // Cleanup
    provider.cleanup_schema().await.ok();
}

#[tokio::test]
async fn test_worker_queue_basic() {
    init_test_logging();
    
    let db_url = get_database_url();
    let schema = get_test_schema();
    
    let provider = MssqlProvider::new_with_schema(&db_url, Some(&schema))
        .await
        .expect("Failed to create provider");
    
    // Enqueue a work item
    let work_item = WorkItem::ActivityExecute {
        instance: "test-worker-instance".to_string(),
        execution_id: INITIAL_EXECUTION_ID,
        id: 1,
        name: "TestActivity".to_string(),
        input: r#"{"test": true}"#.to_string(),
    };
    
    provider
        .enqueue_for_worker(work_item)
        .await
        .expect("Failed to enqueue worker item");
    
    // Fetch the item
    let result = provider
        .fetch_work_item(Duration::from_secs(30), Duration::ZERO)
        .await
        .expect("Fetch should succeed");
    
    assert!(result.is_some(), "Should have fetched a work item");
    
    let (item, lock_token, attempt_count) = result.unwrap();
    
    match item {
        WorkItem::ActivityExecute { name, input, .. } => {
            assert_eq!(name, "TestActivity");
            assert!(input.contains("test"));
        }
        _ => panic!("Expected ActivityExecute"),
    }
    
    assert!(!lock_token.is_empty());
    assert_eq!(attempt_count, 1);
    
    // Ack the work item
    let completion = WorkItem::ActivityCompleted {
        instance: "test-worker-instance".to_string(),
        execution_id: INITIAL_EXECUTION_ID,
        id: 1,
        result: "done".to_string(),
    };
    
    provider
        .ack_work_item(&lock_token, Some(completion))
        .await
        .expect("Ack should succeed");
    
    // Cleanup
    provider.cleanup_schema().await.ok();
}
