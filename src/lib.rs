//! # Duroxide MS-SQL Provider
//!
//! A MS-SQL/Azure SQL provider implementation for [Duroxide](https://crates.io/crates/duroxide),
//! a durable task orchestration framework for Rust.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use duroxide_sql::MssqlProvider;
//! use duroxide::runtime::Runtime;
//! use std::sync::Arc;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // Create a provider with the connection string
//! let provider = MssqlProvider::new(
//!     "server=tcp:myserver.database.windows.net,1433;database=mydb;user=myuser;password=mypass"
//! ).await?;
//!
//! // Use with the Duroxide runtime
//! // let runtime = Runtime::start_with_store(Arc::new(provider), activity_registry, orchestration_registry).await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Custom Schema
//!
//! To isolate data in a specific schema (useful for multi-tenant deployments or testing):
//!
//! ```rust,no_run
//! use duroxide_sql::MssqlProvider;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let provider = MssqlProvider::new_with_schema(
//!     "server=tcp:myserver.database.windows.net,1433;database=mydb;user=myuser;password=mypass",
//!     Some("my_schema"),
//! ).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Configuration
//!
//! | Environment Variable | Description | Default |
//! |---------------------|-------------|---------|
//! | `DUROXIDE_SQL_POOL_MAX` | Maximum connection pool size | `10` |
//!
//! ## Features
//!
//! - Automatic schema migration on startup
//! - Connection pooling via bb8 + tiberius
//! - Custom schema support for multi-tenant isolation
//! - Full implementation of the Duroxide `Provider` and `ProviderAdmin` traits
//! - T-SQL stored procedures for atomic operations
//! - Azure SQL and on-premise MS-SQL support

pub mod migrations;
pub mod provider;

pub use provider::MssqlProvider;
