use anyhow::Result;
use bb8::PooledConnection;
use bb8_tiberius::ConnectionManager;
use include_dir::{include_dir, Dir};
use tracing::{debug, info};

static MIGRATIONS: Dir = include_dir!("$CARGO_MANIFEST_DIR/migrations");

/// Migration metadata
#[derive(Debug)]
struct Migration {
    version: i32,
    name: String,
    sql: String,
}

/// Migration runner that handles schema-qualified migrations
pub struct MigrationRunner<'a> {
    conn: PooledConnection<'a, ConnectionManager>,
    schema_name: String,
}

impl<'a> MigrationRunner<'a> {
    pub fn new(conn: PooledConnection<'a, ConnectionManager>, schema_name: &str) -> Self {
        Self {
            conn,
            schema_name: schema_name.to_string(),
        }
    }

    /// Run all pending migrations
    pub async fn run(mut self) -> Result<()> {
        // Ensure schema exists
        self.ensure_schema().await?;

        // Ensure migrations table exists
        self.ensure_migrations_table().await?;

        // Get applied migrations
        let applied = self.get_applied_migrations().await?;
        debug!("Applied migrations: {:?}", applied);

        // Load and sort migrations
        let migrations = self.load_migrations()?;
        info!("Found {} migrations", migrations.len());

        // Apply pending migrations
        for migration in migrations {
            if !applied.contains(&migration.version) {
                info!("Applying migration {}: {}", migration.version, migration.name);
                self.apply_migration(&migration).await?;
            }
        }

        Ok(())
    }

    /// Ensure the target schema exists
    async fn ensure_schema(&mut self) -> Result<()> {
        if self.schema_name != "dbo" {
            let sql = format!(
                "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{}') \
                 EXEC('CREATE SCHEMA [{}]')",
                self.schema_name, self.schema_name
            );
            self.conn.execute(&sql[..], &[]).await?;
        }
        Ok(())
    }

    /// Ensure the migrations tracking table exists
    async fn ensure_migrations_table(&mut self) -> Result<()> {
        let sql = format!(
            "IF NOT EXISTS (SELECT * FROM sys.tables t \
             JOIN sys.schemas s ON t.schema_id = s.schema_id \
             WHERE s.name = '{}' AND t.name = '_duroxide_migrations') \
             CREATE TABLE [{schema}]._duroxide_migrations ( \
                 version INT PRIMARY KEY, \
                 name NVARCHAR(255) NOT NULL, \
                 applied_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME() \
             )",
            self.schema_name,
            schema = self.schema_name
        );
        self.conn.execute(&sql[..], &[]).await?;
        Ok(())
    }

    /// Get list of already applied migration versions
    async fn get_applied_migrations(&mut self) -> Result<Vec<i32>> {
        let sql = format!(
            "SELECT version FROM [{}]._duroxide_migrations ORDER BY version",
            self.schema_name
        );
        
        let result = self.conn.query(&sql[..], &[]).await?;
        let rows = result.into_first_result().await?;
        
        let versions: Vec<i32> = rows
            .iter()
            .filter_map(|row| row.get::<i32, _>("version"))
            .collect();
        
        Ok(versions)
    }

    /// Load migrations from embedded files
    fn load_migrations(&self) -> Result<Vec<Migration>> {
        let mut migrations = Vec::new();

        for entry in MIGRATIONS.files() {
            let filename = entry.path().file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");
            
            // Parse filename: NNNN_name.sql
            if let Some(sql_file) = filename.strip_suffix(".sql") {
                if let Some((version_str, name)) = sql_file.split_once('_') {
                    if let Ok(version) = version_str.parse::<i32>() {
                        let sql = entry.contents_utf8().unwrap_or("").to_string();
                        migrations.push(Migration {
                            version,
                            name: name.to_string(),
                            sql,
                        });
                    }
                }
            }
        }

        // Sort by version
        migrations.sort_by_key(|m| m.version);
        Ok(migrations)
    }

    /// Apply a single migration
    async fn apply_migration(&mut self, migration: &Migration) -> Result<()> {
        // Replace schema placeholder if present
        let sql = migration.sql.replace("{SCHEMA}", &self.schema_name);

        // Split on GO batch separator (case-insensitive, standalone on line)
        // GO is an SSMS batch separator, not valid T-SQL, so we need to split and execute separately
        let lines: Vec<&str> = sql.lines().collect();
        let batches: Vec<String> = lines
            .split(|line| line.trim().eq_ignore_ascii_case("GO"))
            .map(|batch_lines| batch_lines.join("\n"))
            .filter(|s| !s.trim().is_empty())
            .collect();

        // Execute each batch
        for (i, stmt) in batches.iter().enumerate() {
            let stmt = stmt.trim();
            if !stmt.is_empty() {
                debug!("Executing batch {} (len={})", i, stmt.len());
                // Use simple_query for DDL statements (CREATE PROCEDURE, etc.)
                // This properly handles batches without expecting parameters
                let result = self.conn.simple_query(stmt).await?;
                // Consume all results to ensure connection is clean for next batch
                result.into_results().await?;
            }
        }

        // Record migration
        let record_sql = format!(
            "INSERT INTO [{}]._duroxide_migrations (version, name) VALUES (@P1, @P2)",
            self.schema_name
        );
        self.conn.execute(&record_sql[..], &[&migration.version, &migration.name.as_str()]).await?;

        info!("Migration {} applied successfully", migration.version);
        Ok(())
    }
}
