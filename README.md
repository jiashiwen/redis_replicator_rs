# redis_replicator_rs

[![Crates.io](https://img.shields.io/crates/v/redis_replicator_rs.svg)](https://crates.io/crates/redis_replicator_rs)
[![Documentation](https://docs.rs/redis_replicator_rs/badge.svg)](https://docs.rs/redis_replicator_rs)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Build Status](https://github.com/yourusername/redis_replicator_rs/workflows/CI/badge.svg)](https://github.com/yourusername/redis_replicator_rs/actions)

A high-performance, async Redis replication library for Rust that can parse RDB files, AOF files, and handle real-time Redis replication.

## Features

- 🚀 **High Performance**: Async/await support with efficient memory usage
- 📦 **RDB Parsing**: Complete support for Redis RDB file format parsing
- 📝 **AOF Parsing**: Full AOF (Append Only File) parsing capabilities
- 🔄 **Real-time Replication**: Connect to Redis master and receive real-time updates
- 🛡️ **Error Handling**: Comprehensive error handling with recovery options
- 🔧 **Configurable**: Extensive configuration options for different use cases
- 📊 **Monitoring**: Built-in metrics and progress reporting
- 🔐 **Security**: Support for Redis AUTH and TLS connections
- 🗜️ **Compression**: Support for various compression algorithms
- 🧪 **Well Tested**: Comprehensive test suite with benchmarks

## Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
redis_replicator_rs = "0.1.0"
```

### Parse an RDB file

```rust
use redis_replicator_rs::prelude::*;

#[derive(Default)]
struct MyRdbHandler;

#[async_trait::async_trait]
impl RdbEventHandler for MyRdbHandler {
    async fn handle_event(&mut self, event: RdbEvent) -> Result<()> {
        match event {
            RdbEvent::StartRdb { version } => {
                println!("Starting RDB parsing, version: {}", version);
            }
            RdbEvent::Entry { entry, .. } => {
                println!("Key: {}, Value: {:?}", entry.key, entry.value);
            }
            RdbEvent::EndRdb => {
                println!("RDB parsing completed");
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut handler = MyRdbHandler::default();
    rdb::parse_file("dump.rdb", &mut handler).await?;
    Ok(())
}
```

### Parse an AOF file

```rust
use redis_replicator_rs::prelude::*;

#[derive(Default)]
struct MyAofHandler;

#[async_trait::async_trait]
impl AofEventHandler for MyAofHandler {
    async fn handle_event(&mut self, event: AofEvent) -> Result<()> {
        match event {
            AofEvent::Command { command, args, .. } => {
                println!("Command: {} {}", command, args.join(" "));
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut handler = MyAofHandler::default();
    aof::parse_file("appendonly.aof", &mut handler).await?;
    Ok(())
}
```

### Redis Replication

```rust
use redis_replicator_rs::prelude::*;
use redis_replicator_rs::replication::{ReplicationClient, ReplicationClientConfig};
use std::time::Duration;

#[derive(Default)]
struct MyReplicationHandler;

#[async_trait::async_trait]
impl ReplicationEventHandler for MyReplicationHandler {
    async fn handle_event(&mut self, event: ReplicationEvent) -> Result<()> {
        match event {
            ReplicationEvent::Connected { .. } => {
                println!("Connected to Redis master");
            }
            ReplicationEvent::RdbEntry { entry, .. } => {
                println!("RDB Entry: {} = {:?}", entry.key, entry.value);
            }
            ReplicationEvent::AofCommand { command, args, .. } => {
                println!("AOF Command: {} {}", command, args.join(" "));
            }
            _ => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ReplicationClientConfig {
        master_host: "localhost".to_string(),
        master_port: 6379,
        password: Some("your_password".to_string()),
        connect_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    
    let mut client = ReplicationClient::new(config)?;
    let handler = MyReplicationHandler::default();
    
    client.start_replication(handler).await?;
    Ok(())
}
```

## Examples

The repository includes comprehensive examples:

- **RDB Parsing**: `cargo run --example parse_rdb -- dump.rdb`
- **AOF Parsing**: `cargo run --example parse_aof -- appendonly.aof`
- **Redis Replication**: `cargo run --example redis_replication -- localhost:6379`

Each example supports various command-line options for filtering, analysis, and monitoring.

## Configuration

### RDB Parsing Options

```rust
use redis_replicator_rs::rdb::RdbParseOptions;

let options = RdbParseOptions {
    validate_checksums: true,
    strict_mode: false,
    max_entries: Some(10000),
    skip_entries: 0,
    buffer_size: 8192,
    decompress_strings: true,
    parse_metadata: true,
};
```

### AOF Parsing Options

```rust
use redis_replicator_rs::aof::AofParseOptions;

let options = AofParseOptions {
    validate_only: false,
    max_entries: None,
    skip_entries: 0,
    buffer_size: 8192,
    strict_mode: true,
};
```

### Replication Client Configuration

```rust
use redis_replicator_rs::replication::ReplicationClientConfig;
use std::time::Duration;

let config = ReplicationClientConfig {
    master_host: "redis.example.com".to_string(),
    master_port: 6379,
    password: Some("password".to_string()),
    connect_timeout: Duration::from_secs(30),
    read_timeout: Duration::from_secs(60),
    heartbeat_interval: Duration::from_secs(10),
    buffer_size: 16384,
    parse_rdb: true,
    parse_aof: true,
    auto_reconnect: true,
    max_reconnect_attempts: 10,
    reconnect_delay: Duration::from_secs(5),
    replication_offset: None,
    replica_id: None,
};
```

## Features

The library supports several optional features:

```toml
[dependencies]
redis_replicator_rs = { version = "0.1.0", features = ["full"] }
```

Available features:
- `default`: Includes `metrics` feature
- `metrics`: Prometheus metrics support
- `tracing`: Structured logging with tracing
- `compression`: Support for LZ4 and other compression algorithms
- `tls`: TLS/SSL connection support
- `full`: All features enabled

## Performance

The library is designed for high performance:

- **Memory Efficient**: Streaming parsing with configurable buffer sizes
- **Async/Await**: Non-blocking I/O operations
- **Zero-Copy**: Minimal data copying where possible
- **Concurrent**: Support for concurrent parsing operations

Run benchmarks with:

```bash
cargo bench
```

## Error Handling

The library provides comprehensive error handling:

```rust
use redis_replicator_rs::error::{RedisReplicatorError, ErrorCategory};

match error {
    RedisReplicatorError::Io { source, context } => {
        eprintln!("I/O error: {} (context: {})", source, context);
    }
    RedisReplicatorError::Protocol { message, .. } => {
        eprintln!("Protocol error: {}", message);
    }
    RedisReplicatorError::Parse { message, position, .. } => {
        eprintln!("Parse error at position {}: {}", position, message);
    }
    _ => {
        eprintln!("Other error: {}", error);
    }
}

// Check if error is recoverable
if error.is_recoverable() {
    println!("This error can be recovered from");
}

// Get error category
match error.category() {
    ErrorCategory::Network => println!("Network-related error"),
    ErrorCategory::Protocol => println!("Protocol-related error"),
    ErrorCategory::Data => println!("Data-related error"),
    ErrorCategory::Configuration => println!("Configuration error"),
}
```

## Monitoring and Metrics

With the `metrics` feature enabled, the library provides Prometheus metrics:

```rust
use redis_replicator_rs::metrics;

// Initialize metrics
metrics::init_metrics();

// Metrics are automatically collected during parsing and replication
```

Available metrics:
- `redis_replicator_rdb_entries_total`: Total RDB entries processed
- `redis_replicator_aof_commands_total`: Total AOF commands processed
- `redis_replicator_bytes_processed_total`: Total bytes processed
- `redis_replicator_errors_total`: Total errors encountered
- `redis_replicator_connection_duration_seconds`: Connection duration

## Logging

With the `tracing` feature enabled:

```rust
use redis_replicator_rs;

// Initialize logging
redis_replicator_rs::init();

// Or with custom subscriber
use tracing_subscriber;
let subscriber = tracing_subscriber::fmt::Subscriber::builder()
    .with_max_level(tracing::Level::INFO)
    .finish();
redis_replicator_rs::init_with_subscriber(subscriber);
```

## Testing

Run the test suite:

```bash
# Run all tests
cargo test

# Run tests with all features
cargo test --all-features

# Run integration tests
cargo test --test integration

# Run benchmarks
cargo bench
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/redis_replicator_rs.git
   cd redis_replicator_rs
   ```

2. Install dependencies:
   ```bash
   cargo build
   ```

3. Run tests:
   ```bash
   cargo test --all-features
   ```

4. Run examples:
   ```bash
   cargo run --example parse_rdb -- --help
   ```

### Code Style

This project uses `rustfmt` and `clippy` for code formatting and linting:

```bash
cargo fmt
cargo clippy --all-features
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by the Java [redis-replicator](https://github.com/leonchen83/redis-replicator) library
- Redis protocol specification and documentation
- The Rust async ecosystem

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a detailed changelog.

## Support

If you have any questions or need help, please:

1. Check the [documentation](https://docs.rs/redis_replicator_rs)
2. Look at the [examples](examples/)
3. Search existing [issues](https://github.com/yourusername/redis_replicator_rs/issues)
4. Create a new issue if needed

## Roadmap

- [ ] Support for Redis Streams
- [ ] Redis Cluster support
- [ ] More compression algorithms
- [ ] WebAssembly support
- [ ] Redis modules support
- [ ] Performance optimizations
- [ ] More comprehensive benchmarks

---

**Note**: This library is not affiliated with Redis Ltd. Redis is a trademark of Redis Ltd.