use anyhow::Result;
use async_trait::async_trait;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use duroxide::providers::{
    DeleteInstanceResult, ExecutionInfo, ExecutionMetadata, InstanceFilter, InstanceInfo,
    OrchestrationItem, Provider, ProviderAdmin, ProviderError, PruneOptions, PruneResult,
    QueueDepths, ScheduledActivityIdentifier, SystemMetrics, WorkItem,
};
use duroxide::Event;
use std::sync::Arc;
use std::time::Duration;
use tiberius::{AuthMethod, Config, EncryptionLevel};
use tracing::{debug, instrument};

use crate::migrations::MigrationRunner;

/// MS-SQL/Azure SQL provider for Duroxide durable orchestrations.
///
/// Implements the [`Provider`] and [`ProviderAdmin`] traits from Duroxide,
/// storing orchestration state, history, and work queues in MS-SQL/Azure SQL.
///
/// # Example
///
/// ```rust,no_run
/// use duroxide_sql::MssqlProvider;
///
/// # async fn example() -> anyhow::Result<()> {
/// let provider = MssqlProvider::new(
///     "server=tcp:myserver.database.windows.net,1433;database=mydb;user=myuser;password=mypass"
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub struct MssqlProvider {
    pool: Arc<Pool<ConnectionManager>>,
    schema_name: String,
}

impl MssqlProvider {
    /// Create a new MS-SQL provider with the default schema (dbo).
    pub async fn new(connection_string: &str) -> Result<Self> {
        Self::new_with_schema(connection_string, None).await
    }

    /// Create a new MS-SQL provider with a custom schema.
    pub async fn new_with_schema(connection_string: &str, schema_name: Option<&str>) -> Result<Self> {
        let schema_name = schema_name.unwrap_or("dbo").to_string();

        // Parse connection string and build config
        let config = Self::parse_connection_string(connection_string)?;

        // Get pool size from environment or use default
        let pool_max: u32 = std::env::var("DUROXIDE_SQL_POOL_MAX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);

        // Create connection manager
        let mgr = ConnectionManager::new(config);

        // Build connection pool
        let pool = Pool::builder()
            .max_size(pool_max)
            .build(mgr)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create connection pool: {}", e))?;

        let pool = Arc::new(pool);

        // Create the provider
        let provider = Self { pool, schema_name };

        // Run migrations
        provider.run_migrations().await?;

        Ok(provider)
    }

    /// Parse an ADO.NET style connection string into tiberius Config
    fn parse_connection_string(conn_str: &str) -> Result<Config> {
        let mut config = Config::new();

        // Parse key=value pairs
        for part in conn_str.split(';') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if let Some((key, value)) = part.split_once('=') {
                let key = key.trim().to_lowercase();
                let value = value.trim();

                match key.as_str() {
                    "server" => {
                        let server = value.strip_prefix("tcp:").unwrap_or(value);
                        if let Some((host, port)) = server.split_once(',') {
                            config.host(host);
                            if let Ok(p) = port.parse::<u16>() {
                                config.port(p);
                            }
                        } else {
                            config.host(server);
                        }
                    }
                    "database" | "initial catalog" => {
                        config.database(value);
                    }
                    "user" | "user id" | "uid" => {
                        // Will be set with password
                    }
                    "password" | "pwd" => {
                        let user = conn_str
                            .split(';')
                            .find_map(|p| {
                                let p = p.trim();
                                if let Some((k, v)) = p.split_once('=') {
                                    let k = k.trim().to_lowercase();
                                    if k == "user" || k == "user id" || k == "uid" {
                                        return Some(v.trim().to_string());
                                    }
                                }
                                None
                            })
                            .unwrap_or_default();
                        config.authentication(AuthMethod::sql_server(user, value));
                    }
                    "encrypt" => {
                        if value.to_lowercase() == "true" || value == "yes" {
                            config.encryption(EncryptionLevel::Required);
                        }
                    }
                    "trustservercertificate" => {
                        if value.to_lowercase() == "true" || value == "yes" {
                            config.trust_cert();
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(config)
    }

    /// Run database migrations
    async fn run_migrations(&self) -> Result<()> {
        let conn = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("Failed to get connection for migrations: {}", e))?;
        
        let runner = MigrationRunner::new(conn, &self.schema_name);
        runner.run().await?;
        
        Ok(())
    }

    /// Get the schema name (for testing)
    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    /// Clean up the schema (drop all tables and procedures) - for testing only
    pub async fn cleanup_schema(&self) -> Result<()> {
        let mut conn = self.pool.get().await
            .map_err(|e| anyhow::anyhow!("Failed to get connection for cleanup: {}", e))?;

        let sql = format!(
            "IF OBJECT_ID('{}.sp_cleanup_schema', 'P') IS NOT NULL EXEC {}.sp_cleanup_schema",
            self.schema_name, self.schema_name
        );
        
        conn.execute(&sql[..], &[]).await.ok();
        Ok(())
    }

    /// Get current time in milliseconds since Unix epoch
    fn now_millis() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// Get an integer value from a row that may be i32 or i64
    fn get_int_as_i64(row: &tiberius::Row, col: &str) -> i64 {
        // Try i32 first (INT - what COUNT returns), then fall back to i64 (BIGINT)
        // Use try_get to avoid panics on type mismatch
        if let Ok(Some(v)) = row.try_get::<i32, _>(col) {
            return v as i64;
        }
        if let Ok(Some(v)) = row.try_get::<i64, _>(col) {
            return v;
        }
        0
    }

    /// Convert SQL error to ProviderError
    fn sql_to_provider_error(operation: &str, err: tiberius::error::Error) -> ProviderError {
        let msg = err.to_string();
        
        if msg.contains("Invalid lock token") {
            ProviderError::permanent(operation, "Invalid lock token")
        } else if msg.contains("deadlock") || msg.contains("timeout") || msg.contains("connection") {
            ProviderError::retryable(operation, msg)
        } else {
            ProviderError::permanent(operation, msg)
        }
    }

    /// Extract instance ID from a WorkItem for orchestrator queue
    fn get_instance_id_from_work_item(item: &WorkItem) -> Option<String> {
        match item {
            WorkItem::StartOrchestration { instance, .. } => Some(instance.clone()),
            WorkItem::ActivityCompleted { instance, .. } => Some(instance.clone()),
            WorkItem::ActivityFailed { instance, .. } => Some(instance.clone()),
            WorkItem::TimerFired { instance, .. } => Some(instance.clone()),
            WorkItem::ExternalRaised { instance, .. } => Some(instance.clone()),
            WorkItem::SubOrchCompleted { parent_instance, .. } => Some(parent_instance.clone()),
            WorkItem::SubOrchFailed { parent_instance, .. } => Some(parent_instance.clone()),
            WorkItem::ContinueAsNew { instance, .. } => Some(instance.clone()),
            WorkItem::CancelInstance { instance, .. } => Some(instance.clone()),
            _ => None,
        }
    }

    /// Build cancelled activities JSON manually (since ScheduledActivityIdentifier doesn't implement Serialize)
    fn cancelled_activities_to_json(activities: &[ScheduledActivityIdentifier]) -> String {
        let items: Vec<String> = activities
            .iter()
            .map(|a| {
                format!(
                    r#"{{"instance":"{}","execution_id":{},"activity_id":{}}}"#,
                    a.instance, a.execution_id, a.activity_id
                )
            })
            .collect();
        format!("[{}]", items.join(","))
    }

    /// Build metadata JSON manually (since ExecutionMetadata doesn't implement Serialize)
    fn metadata_to_json(metadata: &ExecutionMetadata) -> String {
        let mut parts = Vec::new();
        if let Some(ref name) = metadata.orchestration_name {
            parts.push(format!(r#""orchestration_name":"{}""#, name));
        }
        if let Some(ref version) = metadata.orchestration_version {
            parts.push(format!(r#""orchestration_version":"{}""#, version));
        }
        if let Some(ref status) = metadata.status {
            parts.push(format!(r#""status":"{}""#, status));
        }
        if let Some(ref output) = metadata.output {
            // Escape the output string for JSON
            let escaped = output.replace('\\', "\\\\").replace('"', "\\\"");
            parts.push(format!(r#""output":"{}""#, escaped));
        }
        if let Some(ref parent) = metadata.parent_instance_id {
            parts.push(format!(r#""parent_instance_id":"{}""#, parent));
        }
        format!("{{{}}}", parts.join(","))
    }
}

#[async_trait]
impl Provider for MssqlProvider {
    fn name(&self) -> &str {
        "duroxide-sql"
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    #[instrument(skip(self), target = "duroxide::providers::mssql")]
    async fn fetch_orchestration_item(
        &self,
        lock_timeout: Duration,
        _poll_timeout: Duration,
    ) -> Result<Option<(OrchestrationItem, String, u32)>, ProviderError> {
        let now_ms = Self::now_millis();
        let lock_timeout_ms = lock_timeout.as_millis() as i64;

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("fetch_orchestration_item", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_fetch_orchestration_item @now_ms = @P1, @lock_timeout_ms = @P2",
            self.schema_name
        );

        let result = conn
            .query(&sql[..], &[&now_ms, &lock_timeout_ms])
            .await
            .map_err(|e| Self::sql_to_provider_error("fetch_orchestration_item", e))?;

        let row = result.into_row().await.map_err(|e| {
            Self::sql_to_provider_error("fetch_orchestration_item", e)
        })?;

        match row {
            None => Ok(None),
            Some(row) => {
                let instance_id: &str = row.get("instance_id").unwrap_or("");
                let orchestration_name: &str = row.get("orchestration_name").unwrap_or("Unknown");
                let orchestration_version: &str = row.get("orchestration_version").unwrap_or("unknown");
                let execution_id: i64 = row.get("execution_id").unwrap_or(1);
                let history_json: &str = row.get("history").unwrap_or("[]");
                let messages_json: &str = row.get("messages").unwrap_or("[]");
                let lock_token: &str = row.get("lock_token").unwrap_or("");
                let attempt_count: i32 = row.get("attempt_count").unwrap_or(1);

                let history: Vec<Event> = serde_json::from_str(history_json).map_err(|e| {
                    ProviderError::permanent("fetch_orchestration_item", format!("Failed to deserialize history: {}", e))
                })?;

                let messages: Vec<WorkItem> = serde_json::from_str(messages_json).map_err(|e| {
                    ProviderError::permanent("fetch_orchestration_item", format!("Failed to deserialize messages: {}", e))
                })?;

                debug!(
                    target = "duroxide::providers::mssql",
                    instance_id = %instance_id,
                    message_count = messages.len(),
                    history_count = history.len(),
                    "Fetched orchestration item"
                );

                Ok(Some((
                    OrchestrationItem {
                        instance: instance_id.to_string(),
                        orchestration_name: orchestration_name.to_string(),
                        execution_id: execution_id as u64,
                        version: orchestration_version.to_string(),
                        history,
                        messages,
                    },
                    lock_token.to_string(),
                    attempt_count as u32,
                )))
            }
        }
    }

    #[instrument(skip(self, history_delta, worker_items, orchestrator_items, metadata, cancelled_activities), 
                 fields(lock_token = %lock_token, execution_id = execution_id), 
                 target = "duroxide::providers::mssql")]
    async fn ack_orchestration_item(
        &self,
        lock_token: &str,
        execution_id: u64,
        history_delta: Vec<Event>,
        worker_items: Vec<WorkItem>,
        orchestrator_items: Vec<WorkItem>,
        metadata: ExecutionMetadata,
        cancelled_activities: Vec<ScheduledActivityIdentifier>,
    ) -> Result<(), ProviderError> {
        let now_ms = Self::now_millis();

        // Serialize data to JSON
        let history_json = serde_json::to_string(&history_delta).map_err(|e| {
            ProviderError::permanent("ack_orchestration_item", format!("Failed to serialize history: {}", e))
        })?;

        let worker_items_json = serde_json::to_string(&worker_items).map_err(|e| {
            ProviderError::permanent("ack_orchestration_item", format!("Failed to serialize worker items: {}", e))
        })?;

        let orchestrator_items_json = serde_json::to_string(&orchestrator_items).map_err(|e| {
            ProviderError::permanent("ack_orchestration_item", format!("Failed to serialize orchestrator items: {}", e))
        })?;

        // Build JSON manually for types without Serialize
        let metadata_json = Self::metadata_to_json(&metadata);
        let cancelled_json = Self::cancelled_activities_to_json(&cancelled_activities);

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("ack_orchestration_item", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_ack_orchestration_item \
             @lock_token = @P1, \
             @execution_id = @P2, \
             @history_delta = @P3, \
             @worker_items = @P4, \
             @orchestrator_items = @P5, \
             @metadata = @P6, \
             @cancelled_activities = @P7, \
             @now_ms = @P8",
            self.schema_name
        );

        let exec_id = execution_id as i64;

        conn.execute(
            &sql[..],
            &[
                &lock_token,
                &exec_id,
                &history_json.as_str(),
                &worker_items_json.as_str(),
                &orchestrator_items_json.as_str(),
                &metadata_json.as_str(),
                &cancelled_json.as_str(),
                &now_ms,
            ],
        )
        .await
        .map_err(|e| Self::sql_to_provider_error("ack_orchestration_item", e))?;

        debug!(
            target = "duroxide::providers::mssql",
            execution_id = execution_id,
            history_count = history_delta.len(),
            worker_items_count = worker_items.len(),
            "Acknowledged orchestration item"
        );

        Ok(())
    }

    #[instrument(skip(self), fields(lock_token = %lock_token), target = "duroxide::providers::mssql")]
    async fn abandon_orchestration_item(
        &self,
        lock_token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let now_ms = Self::now_millis();
        let delay_ms: Option<i64> = delay.map(|d| d.as_millis() as i64);

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("abandon_orchestration_item", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_abandon_orchestration_item \
             @lock_token = @P1, @now_ms = @P2, @delay_ms = @P3, @ignore_attempt = @P4",
            self.schema_name
        );

        conn.execute(&sql[..], &[&lock_token, &now_ms, &delay_ms, &ignore_attempt])
            .await
            .map_err(|e| Self::sql_to_provider_error("abandon_orchestration_item", e))?;

        Ok(())
    }

    #[instrument(skip(self), fields(token = %token), target = "duroxide::providers::mssql")]
    async fn renew_orchestration_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        let now_ms = Self::now_millis();
        let extend_ms = extend_for.as_millis() as i64;

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("renew_orchestration_item_lock", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_renew_orchestration_lock @lock_token = @P1, @now_ms = @P2, @extend_ms = @P3",
            self.schema_name
        );

        conn.execute(&sql[..], &[&token, &now_ms, &extend_ms])
            .await
            .map_err(|e| Self::sql_to_provider_error("renew_orchestration_item_lock", e))?;

        Ok(())
    }

    #[instrument(skip(self), target = "duroxide::providers::mssql")]
    async fn fetch_work_item(
        &self,
        lock_timeout: Duration,
        _poll_timeout: Duration,
    ) -> Result<Option<(WorkItem, String, u32)>, ProviderError> {
        let now_ms = Self::now_millis();
        let lock_timeout_ms = lock_timeout.as_millis() as i64;

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("fetch_work_item", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_fetch_work_item @now_ms = @P1, @lock_timeout_ms = @P2",
            self.schema_name
        );

        let result = conn
            .query(&sql[..], &[&now_ms, &lock_timeout_ms])
            .await
            .map_err(|e| Self::sql_to_provider_error("fetch_work_item", e))?;

        let row = result.into_row().await.map_err(|e| {
            Self::sql_to_provider_error("fetch_work_item", e)
        })?;

        match row {
            None => Ok(None),
            Some(row) => {
                let work_item_json: &str = row.get("work_item").unwrap_or("{}");
                let lock_token: &str = row.get("lock_token").unwrap_or("");
                let attempt_count: i32 = row.get("attempt_count").unwrap_or(1);

                let work_item: WorkItem = serde_json::from_str(work_item_json).map_err(|e| {
                    ProviderError::permanent("fetch_work_item", format!("Failed to deserialize work item: {}", e))
                })?;

                Ok(Some((work_item, lock_token.to_string(), attempt_count as u32)))
            }
        }
    }

    #[instrument(skip(self, completion), fields(token = %token), target = "duroxide::providers::mssql")]
    async fn ack_work_item(
        &self,
        token: &str,
        completion: Option<WorkItem>,
    ) -> Result<(), ProviderError> {
        let now_ms = Self::now_millis();

        let completion_json: Option<String> = completion
            .as_ref()
            .map(|c| serde_json::to_string(c))
            .transpose()
            .map_err(|e| {
                ProviderError::permanent("ack_work_item", format!("Failed to serialize completion: {}", e))
            })?;

        let instance_id: Option<String> = completion.as_ref().and_then(Self::get_instance_id_from_work_item);

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("ack_work_item", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_ack_worker @lock_token = @P1, @instance_id = @P2, @completion_json = @P3, @now_ms = @P4",
            self.schema_name
        );

        conn.execute(
            &sql[..],
            &[&token, &instance_id.as_deref(), &completion_json.as_deref(), &now_ms],
        )
        .await
        .map_err(|e| Self::sql_to_provider_error("ack_work_item", e))?;

        Ok(())
    }

    #[instrument(skip(self), fields(token = %token), target = "duroxide::providers::mssql")]
    async fn abandon_work_item(
        &self,
        token: &str,
        delay: Option<Duration>,
        ignore_attempt: bool,
    ) -> Result<(), ProviderError> {
        let now_ms = Self::now_millis();
        let delay_ms: Option<i64> = delay.map(|d| d.as_millis() as i64);

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("abandon_work_item", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_abandon_work_item @lock_token = @P1, @now_ms = @P2, @delay_ms = @P3, @ignore_attempt = @P4",
            self.schema_name
        );

        conn.execute(&sql[..], &[&token, &now_ms, &delay_ms, &ignore_attempt])
            .await
            .map_err(|e| Self::sql_to_provider_error("abandon_work_item", e))?;

        Ok(())
    }

    #[instrument(skip(self), fields(token = %token), target = "duroxide::providers::mssql")]
    async fn renew_work_item_lock(
        &self,
        token: &str,
        extend_for: Duration,
    ) -> Result<(), ProviderError> {
        let now_ms = Self::now_millis();
        let extend_ms = extend_for.as_millis() as i64;

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("renew_work_item_lock", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_renew_work_item_lock @lock_token = @P1, @now_ms = @P2, @extend_ms = @P3",
            self.schema_name
        );

        conn.execute(&sql[..], &[&token, &now_ms, &extend_ms])
            .await
            .map_err(|e| Self::sql_to_provider_error("renew_work_item_lock", e))?;

        Ok(())
    }

    #[instrument(skip(self, item), target = "duroxide::providers::mssql")]
    async fn enqueue_for_worker(&self, item: WorkItem) -> Result<(), ProviderError> {
        let now_ms = Self::now_millis();

        let item_json = serde_json::to_string(&item).map_err(|e| {
            ProviderError::permanent("enqueue_for_worker", format!("Failed to serialize work item: {}", e))
        })?;

        // Extract activity identity for cancellation support
        let (instance_id, execution_id, activity_id) = match &item {
            WorkItem::ActivityExecute { instance, execution_id, id, .. } => {
                (Some(instance.clone()), Some(*execution_id as i64), Some(*id as i64))
            }
            _ => (None, None, None),
        };

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("enqueue_for_worker", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_enqueue_worker_work \
             @work_item = @P1, @visible_at = @P2, @instance_id = @P3, @execution_id = @P4, @activity_id = @P5",
            self.schema_name
        );

        conn.execute(
            &sql[..],
            &[&item_json.as_str(), &now_ms, &instance_id.as_deref(), &execution_id, &activity_id],
        )
        .await
        .map_err(|e| Self::sql_to_provider_error("enqueue_for_worker", e))?;

        Ok(())
    }

    #[instrument(skip(self, item), target = "duroxide::providers::mssql")]
    async fn enqueue_for_orchestrator(
        &self,
        item: WorkItem,
        delay: Option<Duration>,
    ) -> Result<(), ProviderError> {
        let now_ms = Self::now_millis();
        let visible_at = delay.map(|d| now_ms + d.as_millis() as i64).unwrap_or(now_ms);

        let item_json = serde_json::to_string(&item).map_err(|e| {
            ProviderError::permanent("enqueue_for_orchestrator", format!("Failed to serialize work item: {}", e))
        })?;

        let instance_id = Self::get_instance_id_from_work_item(&item).ok_or_else(|| {
            ProviderError::permanent("enqueue_for_orchestrator", "Unexpected work item type for orchestrator queue")
        })?;

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("enqueue_for_orchestrator", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_enqueue_orchestrator_work @instance_id = @P1, @work_item = @P2, @visible_at = @P3",
            self.schema_name
        );

        conn.execute(&sql[..], &[&instance_id.as_str(), &item_json.as_str(), &visible_at])
            .await
            .map_err(|e| Self::sql_to_provider_error("enqueue_for_orchestrator", e))?;

        Ok(())
    }

    #[instrument(skip(self), fields(instance = %instance), target = "duroxide::providers::mssql")]
    async fn read(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("read", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_fetch_history @instance_id = @P1", self.schema_name);

        let result = conn
            .query(&sql[..], &[&instance])
            .await
            .map_err(|e| Self::sql_to_provider_error("read", e))?;

        let mut events = Vec::new();
        let rows = result.into_first_result().await.map_err(|e| {
            Self::sql_to_provider_error("read", e)
        })?;

        for row in rows {
            let event_data: &str = row.get("event_data").unwrap_or("{}");
            let event: Event = serde_json::from_str(event_data).map_err(|e| {
                ProviderError::permanent("read", format!("Failed to deserialize event: {}", e))
            })?;
            events.push(event);
        }

        Ok(events)
    }

    #[instrument(skip(self, new_events), fields(instance = %instance, execution_id = execution_id), target = "duroxide::providers::mssql")]
    async fn append_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
        new_events: Vec<Event>,
    ) -> Result<(), ProviderError> {
        if new_events.is_empty() {
            return Ok(());
        }

        let events_json = serde_json::to_string(&new_events).map_err(|e| {
            ProviderError::permanent("append_with_execution", format!("Failed to serialize events: {}", e))
        })?;

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("append_with_execution", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_append_history @instance_id = @P1, @execution_id = @P2, @events = @P3",
            self.schema_name
        );

        let exec_id = execution_id as i64;

        conn.execute(&sql[..], &[&instance, &exec_id, &events_json.as_str()])
            .await
            .map_err(|e| Self::sql_to_provider_error("append_with_execution", e))?;

        Ok(())
    }

    #[instrument(skip(self), fields(instance = %instance, execution_id = execution_id), target = "duroxide::providers::mssql")]
    async fn read_with_execution(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("read_with_execution", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_fetch_history_with_execution @instance_id = @P1, @execution_id = @P2",
            self.schema_name
        );

        let exec_id = execution_id as i64;

        let result = conn
            .query(&sql[..], &[&instance, &exec_id])
            .await
            .map_err(|e| Self::sql_to_provider_error("read_with_execution", e))?;

        let mut events = Vec::new();
        let rows = result.into_first_result().await.map_err(|e| {
            Self::sql_to_provider_error("read_with_execution", e)
        })?;

        for row in rows {
            let event_data: &str = row.get("event_data").unwrap_or("{}");
            let event: Event = serde_json::from_str(event_data).map_err(|e| {
                ProviderError::permanent("read_with_execution", format!("Failed to deserialize event: {}", e))
            })?;
            events.push(event);
        }

        Ok(events)
    }

    fn as_management_capability(&self) -> Option<&dyn ProviderAdmin> {
        Some(self)
    }
}

// ============================================================================
// ProviderAdmin implementation
// ============================================================================

#[async_trait]
impl ProviderAdmin for MssqlProvider {
    async fn list_instances(&self) -> Result<Vec<String>, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("list_instances", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_list_instances", self.schema_name);

        let result = conn.query(&sql[..], &[]).await
            .map_err(|e| Self::sql_to_provider_error("list_instances", e))?;

        let rows = result.into_first_result().await
            .map_err(|e| Self::sql_to_provider_error("list_instances", e))?;

        let instances: Vec<String> = rows
            .iter()
            .filter_map(|row| row.get::<&str, _>("instance_id").map(|s| s.to_string()))
            .collect();

        Ok(instances)
    }

    async fn list_instances_by_status(&self, status: &str) -> Result<Vec<String>, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("list_instances_by_status", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_list_instances_by_status @status = @P1", self.schema_name);

        let result = conn.query(&sql[..], &[&status]).await
            .map_err(|e| Self::sql_to_provider_error("list_instances_by_status", e))?;

        let rows = result.into_first_result().await
            .map_err(|e| Self::sql_to_provider_error("list_instances_by_status", e))?;

        let instances: Vec<String> = rows
            .iter()
            .filter_map(|row| row.get::<&str, _>("instance_id").map(|s| s.to_string()))
            .collect();

        Ok(instances)
    }

    async fn list_executions(&self, instance: &str) -> Result<Vec<u64>, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("list_executions", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_list_executions @instance_id = @P1", self.schema_name);

        let result = conn.query(&sql[..], &[&instance]).await
            .map_err(|e| Self::sql_to_provider_error("list_executions", e))?;

        let rows = result.into_first_result().await
            .map_err(|e| Self::sql_to_provider_error("list_executions", e))?;

        let executions: Vec<u64> = rows
            .iter()
            .filter_map(|row| row.get::<i64, _>("execution_id").map(|id| id as u64))
            .collect();

        Ok(executions)
    }

    async fn read_history_with_execution_id(
        &self,
        instance: &str,
        execution_id: u64,
    ) -> Result<Vec<Event>, ProviderError> {
        self.read_with_execution(instance, execution_id).await
    }

    async fn read_history(&self, instance: &str) -> Result<Vec<Event>, ProviderError> {
        self.read(instance).await
    }

    async fn latest_execution_id(&self, instance: &str) -> Result<u64, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("latest_execution_id", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "SELECT COALESCE(MAX(execution_id), 1) as max_exec FROM [{schema}].executions WHERE instance_id = @P1",
            schema = self.schema_name
        );

        let result = conn.query(&sql[..], &[&instance]).await
            .map_err(|e| Self::sql_to_provider_error("latest_execution_id", e))?;

        let row = result.into_row().await
            .map_err(|e| Self::sql_to_provider_error("latest_execution_id", e))?;

        match row {
            Some(row) => {
                let max_id: i64 = row.get("max_exec").unwrap_or(1);
                Ok(max_id as u64)
            }
            None => Ok(1),
        }
    }

    async fn get_instance_info(&self, instance: &str) -> Result<InstanceInfo, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("get_instance_info", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_get_instance_info @instance_id = @P1", self.schema_name);

        let result = conn.query(&sql[..], &[&instance]).await
            .map_err(|e| Self::sql_to_provider_error("get_instance_info", e))?;

        let row = result.into_row().await
            .map_err(|e| Self::sql_to_provider_error("get_instance_info", e))?;

        match row {
            None => Err(ProviderError::permanent("get_instance_info", "Instance not found")),
            Some(row) => {
                let now_ms = Self::now_millis() as u64;
                Ok(InstanceInfo {
                    instance_id: row.get::<&str, _>("instance_id").unwrap_or("").to_string(),
                    orchestration_name: row.get::<&str, _>("orchestration_name").unwrap_or("").to_string(),
                    orchestration_version: row.get::<&str, _>("orchestration_version").unwrap_or("unknown").to_string(),
                    current_execution_id: row.get::<i64, _>("current_execution_id").unwrap_or(1) as u64,
                    status: row.get::<&str, _>("status").unwrap_or("Running").to_string(),
                    output: row.get::<&str, _>("output").map(|s| s.to_string()),
                    created_at: now_ms,  // TODO: parse from row
                    updated_at: now_ms,  // TODO: parse from row
                    parent_instance_id: row.get::<&str, _>("parent_instance_id").map(|s| s.to_string()),
                })
            }
        }
    }

    async fn get_execution_info(&self, instance: &str, execution_id: u64) -> Result<ExecutionInfo, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("get_execution_info", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_get_execution_info @instance_id = @P1, @execution_id = @P2", self.schema_name);
        let exec_id = execution_id as i64;

        let result = conn.query(&sql[..], &[&instance, &exec_id]).await
            .map_err(|e| Self::sql_to_provider_error("get_execution_info", e))?;

        let row = result.into_row().await
            .map_err(|e| Self::sql_to_provider_error("get_execution_info", e))?;

        match row {
            None => Err(ProviderError::permanent("get_execution_info", "Execution not found")),
            Some(row) => {
                let now_ms = Self::now_millis() as u64;
                Ok(ExecutionInfo {
                    execution_id: Self::get_int_as_i64(&row, "execution_id") as u64,
                    status: row.get::<&str, _>("status").unwrap_or("Running").to_string(),
                    output: row.get::<&str, _>("output").map(|s| s.to_string()),
                    started_at: now_ms,  // TODO: parse from row
                    completed_at: None,  // TODO: parse from row
                    event_count: Self::get_int_as_i64(&row, "event_count") as usize,
                })
            }
        }
    }

    async fn get_system_metrics(&self) -> Result<SystemMetrics, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("get_system_metrics", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_get_system_metrics", self.schema_name);

        let result = conn.query(&sql[..], &[]).await
            .map_err(|e| Self::sql_to_provider_error("get_system_metrics", e))?;

        let row = result.into_row().await
            .map_err(|e| Self::sql_to_provider_error("get_system_metrics", e))?;

        match row {
            None => Ok(SystemMetrics::default()),
            Some(row) => {
                Ok(SystemMetrics {
                    total_instances: Self::get_int_as_i64(&row, "total_instances") as u64,
                    total_executions: Self::get_int_as_i64(&row, "total_executions") as u64,
                    running_instances: Self::get_int_as_i64(&row, "running_instances") as u64,
                    completed_instances: Self::get_int_as_i64(&row, "completed_instances") as u64,
                    failed_instances: Self::get_int_as_i64(&row, "failed_instances") as u64,
                    total_events: Self::get_int_as_i64(&row, "total_events") as u64,
                })
            }
        }
    }

    async fn get_queue_depths(&self) -> Result<QueueDepths, ProviderError> {
        let now_ms = Self::now_millis();

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("get_queue_depths", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_get_queue_depths @now_ms = @P1", self.schema_name);

        let result = conn.query(&sql[..], &[&now_ms]).await
            .map_err(|e| Self::sql_to_provider_error("get_queue_depths", e))?;

        let row = result.into_row().await
            .map_err(|e| Self::sql_to_provider_error("get_queue_depths", e))?;

        match row {
            None => Ok(QueueDepths::default()),
            Some(row) => {
                Ok(QueueDepths {
                    orchestrator_queue: Self::get_int_as_i64(&row, "orchestrator_queue") as usize,
                    worker_queue: Self::get_int_as_i64(&row, "worker_queue") as usize,
                    timer_queue: 0, // Timers handled via orchestrator queue with delayed visibility
                })
            }
        }
    }

    async fn list_children(&self, instance_id: &str) -> Result<Vec<String>, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("list_children", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_list_children @instance_id = @P1", self.schema_name);

        let result = conn.query(&sql[..], &[&instance_id]).await
            .map_err(|e| Self::sql_to_provider_error("list_children", e))?;

        let rows = result.into_first_result().await
            .map_err(|e| Self::sql_to_provider_error("list_children", e))?;

        let children: Vec<String> = rows
            .iter()
            .filter_map(|row| row.get::<&str, _>("child_instance_id").map(|s| s.to_string()))
            .collect();

        Ok(children)
    }

    async fn get_parent_id(&self, instance_id: &str) -> Result<Option<String>, ProviderError> {
        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("get_parent_id", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_get_parent_id @instance_id = @P1", self.schema_name);

        let result = conn.query(&sql[..], &[&instance_id]).await
            .map_err(|e| Self::sql_to_provider_error("get_parent_id", e))?;

        let row = result.into_row().await
            .map_err(|e| Self::sql_to_provider_error("get_parent_id", e))?;

        match row {
            None => Err(ProviderError::permanent("get_parent_id", "Instance not found")),
            Some(row) => {
                let parent_id = row.get::<&str, _>("parent_instance_id").map(|s| s.to_string());
                Ok(parent_id)
            }
        }
    }

    async fn delete_instances_atomic(
        &self,
        ids: &[String],
        force: bool,
    ) -> Result<DeleteInstanceResult, ProviderError> {
        if ids.is_empty() {
            return Ok(DeleteInstanceResult::default());
        }

        // Build JSON array manually
        let ids_json = format!(
            "[{}]",
            ids.iter()
                .map(|id| format!(r#""{}""#, id))
                .collect::<Vec<_>>()
                .join(",")
        );

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("delete_instances_atomic", format!("Connection error: {}", e))
        })?;

        let sql = format!(
            "EXEC {}.sp_delete_instances_atomic @instance_ids = @P1, @force = @P2",
            self.schema_name
        );

        let result = conn.query(&sql[..], &[&ids_json.as_str(), &force]).await
            .map_err(|e| Self::sql_to_provider_error("delete_instances_atomic", e))?;

        let row = result.into_row().await
            .map_err(|e| Self::sql_to_provider_error("delete_instances_atomic", e))?;

        match row {
            None => Ok(DeleteInstanceResult::default()),
            Some(row) => {
                Ok(DeleteInstanceResult {
                    instances_deleted: Self::get_int_as_i64(&row, "instances_deleted") as u64,
                    executions_deleted: Self::get_int_as_i64(&row, "executions_deleted") as u64,
                    events_deleted: Self::get_int_as_i64(&row, "events_deleted") as u64,
                    queue_messages_deleted: Self::get_int_as_i64(&row, "queue_messages_deleted") as u64,
                })
            }
        }
    }

    async fn delete_instance_bulk(
        &self,
        filter: InstanceFilter,
    ) -> Result<DeleteInstanceResult, ProviderError> {
        // Build filter JSON manually
        let mut parts = Vec::new();
        if let Some(ref ids) = filter.instance_ids {
            let ids_str = ids.iter().map(|id| format!(r#""{}""#, id)).collect::<Vec<_>>().join(",");
            parts.push(format!(r#""instance_ids":[{}]"#, ids_str));
        }
        if let Some(limit) = filter.limit {
            parts.push(format!(r#""limit":{}"#, limit));
        }
        if let Some(ref before) = filter.completed_before {
            parts.push(format!(r#""completed_before":"{}""#, before));
        }
        let filter_json = format!("{{{}}}", parts.join(","));

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("delete_instance_bulk", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_delete_instance_bulk @filter = @P1", self.schema_name);

        let result = conn.query(&sql[..], &[&filter_json.as_str()]).await
            .map_err(|e| Self::sql_to_provider_error("delete_instance_bulk", e))?;

        let row = result.into_row().await
            .map_err(|e| Self::sql_to_provider_error("delete_instance_bulk", e))?;

        match row {
            None => Ok(DeleteInstanceResult::default()),
            Some(row) => {
                Ok(DeleteInstanceResult {
                    instances_deleted: Self::get_int_as_i64(&row, "instances_deleted") as u64,
                    executions_deleted: Self::get_int_as_i64(&row, "executions_deleted") as u64,
                    events_deleted: Self::get_int_as_i64(&row, "events_deleted") as u64,
                    queue_messages_deleted: Self::get_int_as_i64(&row, "queue_messages_deleted") as u64,
                })
            }
        }
    }

    async fn prune_executions(
        &self,
        instance_id: &str,
        options: PruneOptions,
    ) -> Result<PruneResult, ProviderError> {
        // Build options JSON manually
        let mut parts = Vec::new();
        if let Some(keep_last) = options.keep_last {
            parts.push(format!(r#""keep_last":{}"#, keep_last));
        }
        if let Some(ref before) = options.completed_before {
            parts.push(format!(r#""completed_before":"{}""#, before));
        }
        let options_json = format!("{{{}}}", parts.join(","));

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("prune_executions", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_prune_executions @instance_id = @P1, @options = @P2", self.schema_name);

        let result = conn.query(&sql[..], &[&instance_id, &options_json.as_str()]).await
            .map_err(|e| Self::sql_to_provider_error("prune_executions", e))?;

        let row = result.into_row().await
            .map_err(|e| Self::sql_to_provider_error("prune_executions", e))?;

        match row {
            None => Ok(PruneResult::default()),
            Some(row) => {
                Ok(PruneResult {
                    instances_processed: Self::get_int_as_i64(&row, "instances_processed") as u64,
                    executions_deleted: Self::get_int_as_i64(&row, "executions_deleted") as u64,
                    events_deleted: Self::get_int_as_i64(&row, "events_deleted") as u64,
                })
            }
        }
    }

    async fn prune_executions_bulk(
        &self,
        filter: InstanceFilter,
        options: PruneOptions,
    ) -> Result<PruneResult, ProviderError> {
        // Build filter JSON
        let mut filter_parts = Vec::new();
        if let Some(ref ids) = filter.instance_ids {
            let ids_str = ids.iter().map(|id| format!(r#""{}""#, id)).collect::<Vec<_>>().join(",");
            filter_parts.push(format!(r#""instance_ids":[{}]"#, ids_str));
        }
        if let Some(limit) = filter.limit {
            filter_parts.push(format!(r#""limit":{}"#, limit));
        }
        let filter_json = format!("{{{}}}", filter_parts.join(","));

        // Build options JSON
        let mut options_parts = Vec::new();
        if let Some(keep_last) = options.keep_last {
            options_parts.push(format!(r#""keep_last":{}"#, keep_last));
        }
        let options_json = format!("{{{}}}", options_parts.join(","));

        let mut conn = self.pool.get().await.map_err(|e| {
            ProviderError::retryable("prune_executions_bulk", format!("Connection error: {}", e))
        })?;

        let sql = format!("EXEC {}.sp_prune_executions_bulk @filter = @P1, @options = @P2", self.schema_name);

        let result = conn.query(&sql[..], &[&filter_json.as_str(), &options_json.as_str()]).await
            .map_err(|e| Self::sql_to_provider_error("prune_executions_bulk", e))?;

        let row = result.into_row().await
            .map_err(|e| Self::sql_to_provider_error("prune_executions_bulk", e))?;

        match row {
            None => Ok(PruneResult::default()),
            Some(row) => {
                Ok(PruneResult {
                    instances_processed: Self::get_int_as_i64(&row, "instances_processed") as u64,
                    executions_deleted: Self::get_int_as_i64(&row, "executions_deleted") as u64,
                    events_deleted: Self::get_int_as_i64(&row, "events_deleted") as u64,
                })
            }
        }
    }
}
