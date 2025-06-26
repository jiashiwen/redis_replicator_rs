//! Redis Replicator - A Rust library for parsing Redis RDB files, AOF files, and replicating from Redis servers
//!
//! This library provides comprehensive functionality for working with Redis data formats:
//! - **RDB parsing**: Parse Redis database dump files
//! - **AOF parsing**: Parse Redis append-only files
//! - **Replication**: Connect to Redis servers and receive replication data
//! - **Data types**: Support for all Redis data types including strings, lists, sets, hashes, sorted sets, streams, and modules
//!
//! # Features
//!
//! - **Async/await support**: All operations are asynchronous using Tokio
//! - **Memory efficient**: Streaming parsers with configurable memory limits
//! - **Error handling**: Comprehensive error types with detailed context
//! - **Extensible**: Event-driven architecture for custom processing
//! - **Performance**: Optimized for high throughput and low latency
//!
//! # Quick Start
//!
//! ## Parsing an RDB file
//!
//! ```rust,no_run
//! use redis_replicator_rs::rdb;
//! use redis_replicator_rs::rdb::RdbEventHandler;
//! use redis_replicator_rs::types::Entry;
//! use redis_replicator_rs::error::Result;
//!
//! #[derive(Debug)]
//! struct MyHandler;
//!
//! #[async_trait::async_trait]
//! impl RdbEventHandler for MyHandler {
//!     async fn handle_event(&mut self, event: rdb::RdbEvent) -> Result<()> {
//!         match event {
//!             rdb::RdbEvent::KeyValue { key, value, .. } => {
//!                 println!("Key: {}, Value: {:?}", key, value);
//!             }
//!             _ => {}
//!         }
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut handler = MyHandler;
//!     rdb::parse_file("dump.rdb", &mut handler).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Parsing an AOF file
//!
//! ```rust,no_run
//! use redis_replicator_rs::aof;
//! use redis_replicator_rs::aof::AofEventHandler;
//! use redis_replicator_rs::error::Result;
//!
//! #[derive(Debug)]
//! struct MyAofHandler;
//!
//! #[async_trait::async_trait]
//! impl AofEventHandler for MyAofHandler {
//!     async fn handle_event(&mut self, event: aof::AofEvent) -> Result<()> {
//!         match event {
//!             aof::AofEvent::Command { command, args, .. } => {
//!                 println!("Command: {} {:?}", command, args);
//!             }
//!             _ => {}
//!         }
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut handler = MyAofHandler;
//!     aof::parse_file("appendonly.aof", &mut handler).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Replicating from a Redis server
//!
//! ```rust,no_run
//! use redis_replicator_rs::replication;
//! use redis_replicator_rs::replication::ReplicationEventHandler;
//! use redis_replicator_rs::error::Result;
//!
//! #[derive(Debug)]
//! struct MyReplicationHandler;
//!
//! #[async_trait::async_trait]
//! impl ReplicationEventHandler for MyReplicationHandler {
//!     async fn handle_event(&mut self, event: replication::ReplicationEvent) -> Result<()> {
//!         match event {
//!             replication::ReplicationEvent::Entry { entry } => {
//!                 println!("Received entry: {:?}", entry);
//!             }
//!             _ => {}
//!         }
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut handler = MyReplicationHandler;
//!     replication::replicate_from_master("127.0.0.1:6379", None, &mut handler).await?;
//!     Ok(())
//! }
//! ```
//!
//! # Architecture
//!
//! The library is organized into several modules:
//!
//! - [`types`]: Core data types and structures
//! - [`error`]: Error handling and result types
//! - [`utils`]: Utility functions and helpers
//! - [`rdb`]: RDB file parsing functionality
//! - [`aof`]: AOF file parsing functionality
//! - [`replication`]: Redis replication client
//!
//! Each module provides both high-level convenience functions and low-level APIs
//! for advanced use cases.

#![deny(missing_docs)]
#![warn(clippy::all)]
#![warn(rust_2018_idioms)]

// Re-export commonly used types and traits
pub use error::{RedisReplicatorError, Result};
pub use types::{Entry, RedisValue};

// Public modules
pub mod error;
pub mod types;
pub mod utils;
pub mod rdb;
pub mod aof;
pub mod replication;

/// Library version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Library name
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// Library description
pub const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

/// Get library version information
pub fn version() -> &'static str {
    VERSION
}

/// Get library name
pub fn name() -> &'static str {
    NAME
}

/// Get library description
pub fn description() -> &'static str {
    DESCRIPTION
}

/// Library configuration and feature flags
pub mod config {
    /// Default buffer size for I/O operations
    pub const DEFAULT_BUFFER_SIZE: usize = 64 * 1024; // 64KB
    
    /// Default maximum memory usage for parsing operations
    pub const DEFAULT_MAX_MEMORY: usize = 256 * 1024 * 1024; // 256MB
    
    /// Default connection timeout
    pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;
    
    /// Default read timeout
    pub const DEFAULT_READ_TIMEOUT_SECS: u64 = 30;
    
    /// Default write timeout
    pub const DEFAULT_WRITE_TIMEOUT_SECS: u64 = 10;
    
    /// Default heartbeat interval for replication
    pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 10;
    
    /// Maximum supported RDB version
    pub const MAX_RDB_VERSION: u32 = 11;
    
    /// Minimum supported RDB version
    pub const MIN_RDB_VERSION: u32 = 1;
    
    /// Default CRC check enabled
    pub const DEFAULT_CRC_CHECK: bool = true;
    
    /// Default compression support
    pub const DEFAULT_COMPRESSION_SUPPORT: bool = true;
    
    /// Feature flags
    pub mod features {
        /// Enable RDB parsing support
        pub const RDB_PARSING: bool = true;
        
        /// Enable AOF parsing support
        pub const AOF_PARSING: bool = true;
        
        /// Enable replication support
        pub const REPLICATION: bool = true;
        
        /// Enable compression support
        pub const COMPRESSION: bool = true;
        
        /// Enable async I/O
        pub const ASYNC_IO: bool = true;
        
        /// Enable tracing/logging
        pub const TRACING: bool = true;
    }
}

/// Prelude module for convenient imports
pub mod prelude {
    //! Convenient re-exports of commonly used types and traits
    
    pub use crate::error::{RedisReplicatorError, Result};
    pub use crate::types::{
        Entry, RedisValue, RdbEntry, AofEntry, ReplicationEntry,
        ZSetEntry, StreamValue, StreamEntry, StreamId,
    };
    pub use crate::rdb::{RdbEvent, RdbEventHandler, RdbInfo, RdbStats};
    pub use crate::aof::{AofEvent, AofEventHandler, AofInfo, AofStats};
    pub use crate::replication::{
        ReplicationEvent, ReplicationEventHandler, ReplicationInfo, 
        ReplicationState, ReplicationStats,
    };
    pub use crate::utils::{
        current_timestamp_ms, parse_redis_int, parse_redis_float,
    };
}

/// Initialize the library with default logging
pub fn init() {
    tracing_subscriber::fmt::init();
}

/// Initialize the library with custom tracing subscriber
pub fn init_with_subscriber<S>(subscriber: S)
where
    S: tracing::Subscriber + Send + Sync + 'static,
{
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_version_info() {
        assert!(!version().is_empty());
        assert!(!name().is_empty());
        assert!(!description().is_empty());
    }
    
    #[test]
    fn test_config_constants() {
        assert!(config::DEFAULT_BUFFER_SIZE > 0);
        assert!(config::DEFAULT_MAX_MEMORY > 0);
        assert!(config::MAX_RDB_VERSION >= config::MIN_RDB_VERSION);
    }
    
    #[test]
    fn test_feature_flags() {
        // All features should be enabled by default
        assert!(config::features::RDB_PARSING);
        assert!(config::features::AOF_PARSING);
        assert!(config::features::REPLICATION);
        assert!(config::features::COMPRESSION);
        assert!(config::features::ASYNC_IO);
        assert!(config::features::TRACING);
    }
    
    #[test]
    fn test_init() {
        // Test that init doesn't panic
        init();
    }
}