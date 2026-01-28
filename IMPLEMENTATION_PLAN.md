# duroxide-sql Implementation Plan

**MS-SQL/Azure SQL Provider for Duroxide**

> A production-grade MS-SQL provider for [Duroxide](https://github.com/affandar/duroxide), implementing the `Provider` and `ProviderAdmin` traits using T-SQL stored procedures for atomicity guarantees.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DUROXIDE RUNTIME                                   │
│  ┌──────────────────────┐                    ┌──────────────────────┐          │
│  │   Orchestration      │                    │   Worker             │          │
│  │   Dispatcher         │                    │   Dispatcher         │          │
│  │                      │                    │                      │          │
│  │  • Fetches turns     │                    │  • Fetches activities│          │
│  │  • Runs replay       │                    │  • Executes work     │          │
│  │  • Commits via ack   │                    │  • Reports results   │          │
│  └──────────┬───────────┘                    └──────────┬───────────┘          │
│             │                                           │                       │
│             │  fetch_orchestration_item()               │  fetch_work_item()   │
│             │  ack_orchestration_item()                 │  ack_work_item()     │
│             │  abandon_orchestration_item()             │  abandon_work_item() │
│             │  renew_orchestration_item_lock()          │  renew_work_item_lock()│
│             │                                           │                       │
└─────────────┼───────────────────────────────────────────┼───────────────────────┘
              │                                           │
              ▼                                           ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           duroxide-sql PROVIDER                                 │
│                                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                        MssqlProvider (Rust)                               │  │
│  │                                                                           │  │
│  │  • Connection pool (tiberius + bb8)                                      │  │
│  │  • Schema isolation support                                               │  │
│  │  • Stored procedure calls only (no inline SQL for critical ops)          │  │
│  │  • JSON serialization for events/work items                              │  │
│  │  • Error mapping: SQL → ProviderError (retryable vs permanent)           │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                        │                                        │
│                                        │ EXEC schema.procedure_name @params    │
│                                        ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                    T-SQL Stored Procedures                                │  │
│  │                                                                           │  │
│  │  Orchestrator Queue:              Worker Queue:                          │  │
│  │  • sp_fetch_orchestration_item    • sp_fetch_work_item                   │  │
│  │  • sp_ack_orchestration_item      • sp_ack_worker                        │  │
│  │  • sp_abandon_orchestration_item  • sp_abandon_work_item                 │  │
│  │  • sp_renew_orchestration_lock    • sp_renew_work_item_lock              │  │
│  │  • sp_enqueue_orchestrator_work                                          │  │
│  │                                                                           │  │
│  │  History:                         Management:                            │  │
│  │  • sp_fetch_history               • sp_list_instances                    │  │
│  │  • sp_fetch_history_with_exec     • sp_get_instance_info                 │  │
│  │  • sp_append_history              • sp_get_system_metrics                │  │
│  │                                   • sp_delete_instances_atomic           │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                        │                                        │
└────────────────────────────────────────┼────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         AZURE SQL / MS-SQL DATABASE                             │
│                                                                                 │
│  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐ ┌────────────────┐   │
│  │   instances    │ │   executions   │ │    history     │ │ instance_locks │   │
│  │                │ │                │ │                │ │                │   │
│  │ • instance_id  │ │ • instance_id  │ │ • instance_id  │ │ • instance_id  │   │
│  │ • orch_name    │ │ • execution_id │ │ • execution_id │ │ • lock_token   │   │
│  │ • orch_version │ │ • status       │ │ • event_id     │ │ • locked_until │   │
│  │ • current_exec │ │ • output       │ │ • event_data   │ │                │   │
│  │ • parent_id    │ │ • started_at   │ │                │ │                │   │
│  └────────────────┘ └────────────────┘ └────────────────┘ └────────────────┘   │
│                                                                                 │
│  ┌────────────────────────────────┐   ┌────────────────────────────────┐       │
│  │      orchestrator_queue        │   │         worker_queue           │       │
│  │                                │   │                                │       │
│  │ • id (IDENTITY)                │   │ • id (IDENTITY)                │       │
│  │ • instance_id                  │   │ • work_item (JSON)             │       │
│  │ • work_item (JSON)             │   │ • visible_at                   │       │
│  │ • visible_at                   │   │ • lock_token                   │       │
│  │ • lock_token                   │   │ • locked_until                 │       │
│  │ • locked_until                 │   │ • attempt_count                │       │
│  │ • attempt_count                │   │ • instance_id (for cancel)     │       │
│  │                                │   │ • execution_id                 │       │
│  └────────────────────────────────┘   │ • activity_id                  │       │
│                                       └────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Design Decisions

### 1. Stored Procedures Only (No Client-Side Transactions)

All atomic operations are implemented as T-SQL stored procedures:

| Approach | Pros | Cons |
|----------|------|------|
| **Stored Procedures** ✅ | Single round-trip, atomicity guaranteed by SQL Server, better for cloud latency | More complex T-SQL, harder to debug |
| Client-side transactions | Simpler Rust code | 8-9 round-trips per ack, latency amplification in cloud |

**Decision**: Stored procedures only. This matches the PostgreSQL provider pattern and is essential for Azure SQL performance.

### 2. Short-Polling Only (No Long-Polling)

| Approach | Pros | Cons |
|----------|------|------|
| **Short-polling** ✅ | Simple, matches SQLite/PostgreSQL, runtime handles backoff | Slightly higher CPU when idle |
| `WAITFOR DELAY` loop | Lower latency | Wastes SQL Server resources |
| Service Broker | True blocking | Massive complexity, not worth it |

**Decision**: Short-polling. The runtime's `dispatcher_min_poll_interval` (configurable, default 10ms) handles backoff. For remote databases, configure 100-500ms.

```rust
// Configure for Azure SQL
RuntimeOptions {
    dispatcher_min_poll_interval: Duration::from_millis(100),
    ..Default::default()
}
```

### 3. T-SQL Patterns for PostgreSQL Equivalents

| PostgreSQL | T-SQL Equivalent |
|------------|------------------|
| `BEGIN ... EXCEPTION ... END` | `BEGIN TRY ... END CATCH` |
| `GET DIAGNOSTICS ROW_COUNT` | `@@ROWCOUNT` |
| `FOR UPDATE SKIP LOCKED` | `WITH (ROWLOCK, UPDLOCK, READPAST)` |
| `ON CONFLICT DO UPDATE` | `MERGE ... WHEN MATCHED ... WHEN NOT MATCHED` |
| `pg_advisory_xact_lock(id)` | `sp_getapplock @Resource, @LockMode` |
| `jsonb_array_elements()` | `OPENJSON()` |
| `RAISE EXCEPTION` | `THROW 50001, 'message', 1` |
| `COALESCE(jsonb_agg(...), '[]')` | `ISNULL((SELECT ... FOR JSON PATH), '[]')` |

---

## Atomicity Requirements

### Critical Atomic Operations

These operations MUST be fully atomic (all-or-nothing):

#### 1. `ack_orchestration_item()` — 9 Steps

```
BEGIN TRANSACTION + SET XACT_ABORT ON

Step 1: Validate lock token (SELECT with UPDLOCK)
Step 2: Create/update instance (MERGE)
Step 3: Create execution record (INSERT with ignore duplicate)
Step 4: Append history events (INSERT from OPENJSON)
Step 5: Enqueue worker items (INSERT from OPENJSON)
Step 6: Enqueue orchestrator items (INSERT from OPENJSON, with delay for timers)
Step 7: Delete cancelled activities (DELETE matching worker queue entries)
Step 8: Delete processed messages (DELETE by lock_token)
Step 9: Release instance lock (DELETE from instance_locks)

COMMIT TRANSACTION
```

**Critical ordering**: Step 5 (enqueue) BEFORE Step 7 (cancel) for same-turn schedule+cancel.

#### 2. `ack_work_item()` — 2 Steps

```
BEGIN TRANSACTION
Step 1: Delete worker queue entry (must succeed, else THROW)
Step 2: Enqueue completion to orchestrator queue (if provided)
COMMIT
```

#### 3. `fetch_orchestration_item()` — 3 Steps

```
BEGIN TRANSACTION
Step 1: Acquire instance lock (INSERT or UPDATE instance_locks)
Step 2: Tag messages (UPDATE orchestrator_queue SET lock_token)
Step 3: Load history + messages
COMMIT (or ROLLBACK if no work found)
```

---

## Database Schema

### Tables

```sql
-- 1. Orchestration instance metadata
CREATE TABLE instances (
    instance_id NVARCHAR(255) PRIMARY KEY,
    orchestration_name NVARCHAR(255) NOT NULL,
    orchestration_version NVARCHAR(255) NULL,
    current_execution_id BIGINT NOT NULL DEFAULT 1,
    parent_instance_id NVARCHAR(255) NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

-- 2. Execution records (one per execution_id per instance)
CREATE TABLE executions (
    instance_id NVARCHAR(255) NOT NULL,
    execution_id BIGINT NOT NULL,
    status NVARCHAR(50) NOT NULL DEFAULT 'Running',
    output NVARCHAR(MAX) NULL,
    started_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    completed_at DATETIME2 NULL,
    PRIMARY KEY (instance_id, execution_id)
);

-- 3. Event history (append-only log)
CREATE TABLE history (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    instance_id NVARCHAR(255) NOT NULL,
    execution_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL,
    event_type NVARCHAR(100) NULL,
    event_data NVARCHAR(MAX) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_history_event UNIQUE (instance_id, execution_id, event_id)
);

-- 4. Orchestrator work queue
CREATE TABLE orchestrator_queue (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    instance_id NVARCHAR(255) NOT NULL,
    work_item NVARCHAR(MAX) NOT NULL,
    visible_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    lock_token NVARCHAR(255) NULL,
    locked_until BIGINT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

-- 5. Worker (activity) queue
CREATE TABLE worker_queue (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    work_item NVARCHAR(MAX) NOT NULL,
    visible_at BIGINT NOT NULL,
    lock_token NVARCHAR(255) NULL,
    locked_until BIGINT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    instance_id NVARCHAR(255) NULL,
    execution_id BIGINT NULL,
    activity_id BIGINT NULL
);

-- 6. Instance-level locks
CREATE TABLE instance_locks (
    instance_id NVARCHAR(255) PRIMARY KEY,
    lock_token NVARCHAR(255) NOT NULL,
    locked_until BIGINT NOT NULL
);

-- 7. Migration tracking
CREATE TABLE _duroxide_migrations (
    version INT PRIMARY KEY,
    name NVARCHAR(255) NOT NULL,
    applied_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
```

### Indexes

```sql
-- Orchestrator queue fetch optimization
CREATE INDEX idx_orch_queue_fetch ON orchestrator_queue (visible_at, instance_id) 
    WHERE lock_token IS NULL;

-- Worker queue fetch optimization  
CREATE INDEX idx_worker_queue_fetch ON worker_queue (visible_at) 
    WHERE lock_token IS NULL;

-- Activity cancellation (lock stealing)
CREATE INDEX idx_worker_activity ON worker_queue (instance_id, execution_id, activity_id);

-- History lookup
CREATE INDEX idx_history_lookup ON history (instance_id, execution_id, event_id);

-- Parent-child relationships
CREATE INDEX idx_instances_parent ON instances (parent_instance_id);
```

---

## Stored Procedures

### Core Provider Operations

| Procedure | Purpose | Atomicity |
|-----------|---------|-----------|
| `sp_fetch_orchestration_item` | Fetch + lock orchestration turn | ✅ Required |
| `sp_ack_orchestration_item` | Commit turn (9 steps) | ✅ Required |
| `sp_abandon_orchestration_item` | Release lock without commit | ✅ Required |
| `sp_renew_orchestration_lock` | Extend lock timeout | ✅ Required |
| `sp_fetch_work_item` | Fetch + lock activity | ✅ Required |
| `sp_ack_worker` | Delete + enqueue completion | ✅ Required |
| `sp_abandon_work_item` | Release activity lock | ✅ Required |
| `sp_renew_work_item_lock` | Extend activity lock | ✅ Required |
| `sp_enqueue_orchestrator_work` | Add to orchestrator queue | Simple INSERT |
| `sp_enqueue_worker_work` | Add to worker queue | Simple INSERT |

### History Operations

| Procedure | Purpose |
|-----------|---------|
| `sp_fetch_history` | Load current execution history |
| `sp_fetch_history_with_execution` | Load specific execution history |
| `sp_append_history` | Append events (with duplicate check) |

### Management (ProviderAdmin)

| Procedure | Purpose |
|-----------|---------|
| `sp_list_instances` | List all instance IDs |
| `sp_list_instances_by_status` | Filter instances by status |
| `sp_list_executions` | List executions for instance |
| `sp_get_instance_info` | Get instance metadata |
| `sp_get_execution_info` | Get execution metadata |
| `sp_get_system_metrics` | System-wide statistics |
| `sp_get_queue_depths` | Queue depths |
| `sp_list_children` | List child instances |
| `sp_get_parent_id` | Get parent instance ID |
| `sp_delete_instances_atomic` | Atomic batch deletion |
| `sp_prune_executions` | Prune old executions |

---

## Provider Trait Implementation

### Required Methods (Provider trait)

```rust
#[async_trait]
impl Provider for MssqlProvider {
    fn name(&self) -> &str { "duroxide-sql" }
    fn version(&self) -> &str { env!("CARGO_PKG_VERSION") }

    // Orchestrator Queue
    async fn fetch_orchestration_item(&self, lock_timeout: Duration, _poll_timeout: Duration) 
        -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError>;
    async fn ack_orchestration_item(&self, lock_token: &str, execution_id: u64, 
        history_delta: Vec<Event>, worker_items: Vec<WorkItem>, 
        orchestrator_items: Vec<WorkItem>, metadata: ExecutionMetadata,
        cancelled_activities: Vec<ScheduledActivityIdentifier>) -> Result<(), ProviderError>;
    async fn abandon_orchestration_item(&self, lock_token: &str, delay: Option<Duration>, 
        ignore_attempt: bool) -> Result<(), ProviderError>;
    async fn renew_orchestration_item_lock(&self, token: &str, extend_for: Duration) 
        -> Result<(), ProviderError>;

    // Worker Queue
    async fn fetch_work_item(&self, lock_timeout: Duration, _poll_timeout: Duration) 
        -> Result<Option<(WorkItem, String, u32)>, ProviderError>;
    async fn ack_work_item(&self, token: &str, completion: Option<WorkItem>) 
        -> Result<(), ProviderError>;
    async fn abandon_work_item(&self, token: &str, delay: Option<Duration>, 
        ignore_attempt: bool) -> Result<(), ProviderError>;
    async fn renew_work_item_lock(&self, token: &str, extend_for: Duration) 
        -> Result<(), ProviderError>;

    // Enqueue
    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError>;
    async fn enqueue_for_orchestrator(&self, item: WorkItem, delay: Option<Duration>) 
        -> Result<(), ProviderError>;

    // History
    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError>;
    async fn append_with_execution(&self, instance: &str, execution_id: u64, 
        new_events: Vec<Event>) -> Result<(), ProviderError>;
    async fn read_with_execution(&self, instance: &str, execution_id: u64) 
        -> Result<Vec<Event>, ProviderError>;

    // Enable ProviderAdmin
    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> { Some(self) }
}
```

### Optional Methods (ProviderAdmin trait)

```rust
#[async_trait]
impl ProviderAdmin for MssqlProvider {
    async fn list_instances(&self) -> Result<Vec<String>, ProviderError>;
    async fn list_instances_by_status(&self, status: &str) -> Result<Vec<String>, ProviderError>;
    async fn list_executions(&self, instance: &str) -> Result<Vec<u64>, ProviderError>;
    async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, ProviderError>;
    async fn get_execution_info(&self, instance: &str, execution_id: u64) 
        -> Result<ExecutionInfo, ProviderError>;
    async fn get_system_metrics(&self) -> Result<SystemMetrics, ProviderError>;
    async fn get_queue_depths(&self) -> Result<QueueDepths, ProviderError>;
    async fn list_children(&self, instance_id: &str) -> Result<Vec<String>, ProviderError>;
    async fn get_parent_id(&self, instance_id: &str) -> Result<Option<String>, ProviderError>;
    async fn delete_instances_atomic(&self, ids: &[String], force: bool) 
        -> Result<DeleteInstanceResult, ProviderError>;
    async fn prune_executions(&self, instance_id: &str, options: PruneOptions) 
        -> Result<PruneResult, ProviderError>;
    // ... additional methods
}
```

---

## Testing Strategy

### Validation Tests (135+ tests)

Using `duroxide::provider_validations::ProviderFactory`:

| Category | Test Count | Description |
|----------|------------|-------------|
| Atomicity | 4 | All-or-nothing commit, rollback, concurrent ack prevention |
| Error Handling | 5 | Invalid tokens, duplicates, corrupted data |
| Instance Locking | 11 | Exclusive access, token uniqueness, cross-instance isolation |
| Lock Expiration | 11 | Timeout, renewal, abandon |
| Multi-Execution | 5 | Execution isolation, continue-as-new |
| Queue Semantics | 7 | FIFO, peek-lock, delayed visibility |
| Instance Creation | 4 | Via metadata, not on enqueue |
| Management | 7 | List/get operations |
| Long Polling | 3 | Short-poll returns immediately |
| Poison Message | 9 | Attempt count tracking |
| Cancellation | 15 | Lock stealing, activity cancellation |
| Deletion | 13 | Cascade delete, atomic batch |
| Pruning | 3 | Keep_last, safety |
| Bulk Deletion | 4 | Filters, cascades |

### Implementation

```rust
use duroxide::provider_validations::{ProviderFactory, atomicity, instance_locking, ...};

struct MssqlProviderFactory {
    connection_string: String,
    schema_name: Mutex<Option<String>>,
}

#[async_trait]
impl ProviderFactory for MssqlProviderFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        let schema = format!("test_{}", Uuid::new_v4().simple());
        Arc::new(MssqlProvider::new_with_schema(&self.connection_string, &schema).await.unwrap())
    }

    fn lock_timeout(&self) -> Duration {
        Duration::from_secs(30)
    }
}

// Generate tests via macro
macro_rules! provider_validation_test {
    ($module:ident :: $test_fn:ident) => {
        #[tokio::test]
        async fn $test_fn() {
            let factory = MssqlProviderFactory::new();
            $module::$test_fn(&factory).await;
            factory.cleanup_schema().await;
        }
    };
}

mod atomicity_tests {
    provider_validation_test!(atomicity::test_atomicity_failure_rollback);
    provider_validation_test!(atomicity::test_multi_operation_atomic_ack);
    provider_validation_test!(atomicity::test_lock_released_only_on_successful_ack);
    provider_validation_test!(atomicity::test_concurrent_ack_prevention);
}
// ... 130+ more tests
```

### Stress Tests

Using `duroxide::provider_stress_tests::ProviderStressFactory`:

```rust
struct MssqlStressFactory { ... }

#[async_trait]
impl ProviderStressFactory for MssqlStressFactory {
    async fn create_provider(&self) -> Arc<dyn Provider> {
        Arc::new(MssqlProvider::new_with_schema(&self.url, &unique_schema()).await.unwrap())
    }
}

#[tokio::test]
async fn stress_test_parallel_orchestrations() {
    let factory = MssqlStressFactory::new();
    let result = run_parallel_orchestrations_test(&factory).await.unwrap();
    assert!(result.success_rate() > 99.0);
}
```

---

## Project Structure

```
duroxide-sql/
├── Cargo.toml
├── README.md
├── IMPLEMENTATION_PLAN.md        # This file
├── .env.sample
├── src/
│   ├── lib.rs                    # Public exports
│   ├── provider.rs               # Provider + ProviderAdmin impl
│   └── migrations.rs             # Migration runner
├── migrations/
│   ├── 0001_create_tables.sql
│   ├── 0002_create_stored_procedures.sql
│   ├── 0003_add_attempt_count.sql
│   ├── 0004_add_cancellation_support.sql
│   └── 0005_add_deletion_pruning.sql
├── tests/
│   ├── mssql_provider_test.rs    # Validation test harness
│   ├── basic_tests.rs            # Unit tests
│   ├── stress_tests.rs           # Stress tests
│   └── common/
│       └── mod.rs                # Test utilities
└── sql-stress/                   # Optional: stress test binary
    ├── Cargo.toml
    └── src/
        ├── lib.rs
        └── bin/
            └── sql-stress.rs
```

---

## Implementation Phases

### Phase 1: Project Setup ✅
- [x] Create Cargo.toml with dependencies
- [x] Set up project structure
- [x] Connection string configuration

### Phase 2: Database Schema
- [ ] Create migration runner
- [ ] `0001_create_tables.sql` — All 7 tables
- [ ] Add indexes for performance

### Phase 3: Core Stored Procedures
- [ ] `sp_fetch_orchestration_item` — With instance locking
- [ ] `sp_ack_orchestration_item` — 9-step atomic commit
- [ ] `sp_fetch_work_item` — With attempt count
- [ ] `sp_ack_worker` — Delete + enqueue atomic

### Phase 4: Provider Implementation
- [ ] `MssqlProvider` struct with connection pool
- [ ] Implement all 15 `Provider` trait methods
- [ ] JSON serialization for events/work items

### Phase 5: Remaining Procedures
- [ ] Abandon/renew operations
- [ ] History operations
- [ ] Enqueue operations

### Phase 6: ProviderAdmin
- [ ] Management procedures
- [ ] Deletion/pruning procedures
- [ ] Implement `ProviderAdmin` trait

### Phase 7: Testing
- [ ] Set up `MssqlProviderFactory`
- [ ] Run all 135+ validation tests
- [ ] Run stress tests
- [ ] Verify 100% success rate

### Phase 8: Documentation & Release
- [ ] README.md with usage examples
- [ ] CHANGELOG.md
- [ ] Publish to crates.io

---

## Dependencies

```toml
[dependencies]
duroxide = { version = "0.1.14", features = ["provider-test"] }
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
tiberius = { version = "0.12", features = ["rustls", "chrono"] }
bb8 = "0.8"
bb8-tiberius = "0.15"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
uuid = { version = "1.0", features = ["v4"] }
chrono = "0.4"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
anyhow = "1.0"
include_dir = "0.7"

[dev-dependencies]
tempfile = "3"
```

---

## Connection Configuration

```bash
# .env
DATABASE_URL=sqlserver://user:password@server.database.windows.net:1433/database?encrypt=true&trustServerCertificate=false

# Environment variable (pool size)
DUROXIDE_SQL_POOL_MAX=10
```

---

## Success Criteria

1. **All 135+ validation tests pass** — 100% success rate
2. **Stress tests pass** — >99% success rate, stable throughput
3. **Atomicity verified** — No partial commits, proper rollback
4. **Lock semantics correct** — No double-processing, proper expiration
5. **Cloud-ready** — Works with Azure SQL, handles latency gracefully
