# duroxide-sql

> ⚠️ **EXPERIMENTAL** ⚠️
> 
> This project is experimental and under active development. It is **not recommended for production use**. APIs may change without notice, and there may be bugs or missing features.

A MS-SQL/Azure SQL provider implementation for [Duroxide](https://github.com/affandar/duroxide), a durable task orchestration framework for Rust.

## Features

- ✅ Full implementation of `Provider` and `ProviderAdmin` traits
- ✅ T-SQL stored procedures for atomic operations
- ✅ Azure SQL and on-premise MS-SQL support
- ✅ Connection pooling via bb8 + tiberius
- ✅ Custom schema support for multi-tenant isolation
- ✅ Automatic schema migration on startup
- ✅ Poison message detection with attempt count tracking
- ✅ Lock renewal for long-running orchestrations and activities

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
duroxide-sql = { git = "https://github.com/affandar/duroxide-sql" }
duroxide = { git = "https://github.com/affandar/duroxide" }
```

## Usage

```rust
use duroxide_sql::MssqlProvider;
use duroxide::runtime::Runtime;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create provider with connection string
    let provider = MssqlProvider::new(
        "server=tcp:myserver.database.windows.net,1433;database=mydb;user=myuser;password=mypass;encrypt=true"
    ).await?;

    // Use with Duroxide runtime
    let runtime = Runtime::start_with_store(
        Arc::new(provider),
        activity_registry,
        orchestration_registry,
    ).await;

    Ok(())
}
```

### Custom Schema

For multi-tenant isolation or testing:

```rust
let provider = MssqlProvider::new_with_schema(
    "server=tcp:myserver.database.windows.net,1433;database=mydb;user=myuser;password=mypass",
    Some("my_schema"),
).await?;
```

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `DUROXIDE_SQL_POOL_MAX` | Maximum connection pool size | `10` |

## Connection String Format

Supports ADO.NET style connection strings:

```
server=tcp:hostname,port;database=dbname;user=username;password=secret;encrypt=true;trustservercertificate=false
```

## Architecture

This provider uses **T-SQL stored procedures** for all critical operations to ensure atomicity:

- `sp_fetch_orchestration_item` - Fetch + lock orchestration turn
- `sp_ack_orchestration_item` - 9-step atomic commit
- `sp_fetch_work_item` - Fetch + lock activity
- `sp_ack_worker` - Delete + enqueue completion

See [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md) for detailed architecture.

## Testing

Run against Azure SQL:

```bash
export DATABASE_URL="server=tcp:myserver.database.windows.net,1433;database=mydb;user=myuser;password=mypass;encrypt=true"
cargo test
```

## License

MIT License - see [LICENSE](LICENSE) for details.
