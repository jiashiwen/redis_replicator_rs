//! RDB (Redis Database) file parsing module
//!
//! This module provides functionality to parse Redis RDB files, which are
//! point-in-time snapshots of Redis datasets.

pub mod parser;
pub mod structure;
pub mod types;

pub use parser::{RdbParser, RdbParseOptions};
pub use structure::*;
pub use types::*;

use crate::error::{RedisReplicatorError, Result};
use crate::types::{Entry, RdbEntry, RedisValue};
use std::path::Path;
use tokio::fs::File;
use tokio::io::BufReader;

/// Parse an RDB file from a file path
pub async fn parse_file<P: AsRef<Path>>(path: P) -> Result<Vec<Entry>> {
    let file = File::open(path).await
        .map_err(|e| RedisReplicatorError::Io(e))?;
    
    let reader = BufReader::new(file);
    let mut parser = RdbParser::new();
    parser.parse_stream(reader).await
}

/// Parse an RDB file with custom options
pub async fn parse_file_with_options<P: AsRef<Path>>(
    path: P,
    options: RdbParseOptions,
) -> Result<Vec<Entry>> {
    let file = File::open(path).await
        .map_err(|e| RedisReplicatorError::Io(e))?;
    
    let reader = BufReader::new(file);
    let mut parser = RdbParser::with_options(options);
    parser.parse_stream(reader).await
}

/// Parse RDB data from a byte slice
pub fn parse_bytes(data: &[u8]) -> Result<Vec<Entry>> {
    let mut parser = RdbParser::new();
    parser.parse_buffer(data)
}

/// Parse RDB data from a byte slice with custom options
pub fn parse_bytes_with_options(data: &[u8], options: RdbParseOptions) -> Result<Vec<Entry>> {
    let mut parser = RdbParser::with_options(options);
    parser.parse_buffer(data)
}

/// Validate RDB file format without full parsing
pub async fn validate_file<P: AsRef<Path>>(path: P) -> Result<RdbInfo> {
    let file = File::open(path).await
        .map_err(|e| RedisReplicatorError::Io(e))?;
    
    let reader = BufReader::new(file);
    let parser = RdbParser::new();
    parser.validate_stream(reader).await
}

/// RDB file information
#[derive(Debug, Clone)]
pub struct RdbInfo {
    /// RDB version
    pub version: u32,
    /// Redis version that created the file
    pub redis_version: Option<String>,
    /// Creation timestamp
    pub creation_time: Option<u64>,
    /// Used memory
    pub used_memory: Option<u64>,
    /// Number of databases
    pub database_count: usize,
    /// Total number of keys
    pub key_count: u64,
    /// Total number of keys with expiry
    pub expires_count: u64,
    /// File size in bytes
    pub file_size: u64,
    /// Checksum (if present)
    pub checksum: Option<u64>,
}

/// RDB parsing statistics
#[derive(Debug, Clone, Default)]
pub struct RdbStats {
    /// Number of databases processed
    pub databases_processed: usize,
    /// Number of keys processed
    pub keys_processed: u64,
    /// Number of expired keys skipped
    pub expired_keys_skipped: u64,
    /// Number of bytes processed
    pub bytes_processed: u64,
    /// Processing time in milliseconds
    pub processing_time_ms: u64,
    /// Memory usage during parsing
    pub peak_memory_usage: usize,
    /// Number of errors encountered
    pub errors_encountered: u32,
}

impl RdbStats {
    /// Create new empty statistics
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Calculate keys per second
    pub fn keys_per_second(&self) -> f64 {
        if self.processing_time_ms == 0 {
            0.0
        } else {
            (self.keys_processed as f64) / (self.processing_time_ms as f64 / 1000.0)
        }
    }
    
    /// Calculate bytes per second
    pub fn bytes_per_second(&self) -> f64 {
        if self.processing_time_ms == 0 {
            0.0
        } else {
            (self.bytes_processed as f64) / (self.processing_time_ms as f64 / 1000.0)
        }
    }
    
    /// Get processing efficiency (keys processed / total keys)
    pub fn efficiency(&self) -> f64 {
        let total_keys = self.keys_processed + self.expired_keys_skipped;
        if total_keys == 0 {
            1.0
        } else {
            (self.keys_processed as f64) / (total_keys as f64)
        }
    }
}

/// RDB parsing event for streaming processing
#[derive(Debug, Clone)]
pub enum RdbEvent {
    /// Start of RDB file
    StartOfFile {
        /// RDB file version
        version: u32,
        /// RDB file information
        info: RdbInfo,
    },
    /// Start of database
    StartOfDatabase {
        /// Database ID
        database_id: u32,
    },
    /// Key-value pair
    KeyValue {
        /// Database ID
        database_id: u32,
        /// Key bytes
        key: Vec<u8>,
        /// Value data
        value: RedisValue,
        /// Expiry timestamp
        expiry: Option<u64>,
        /// RDB entry
        entry: RdbEntry,
    },
    /// End of database
    EndOfDatabase {
        /// Database ID
        database_id: u32,
        /// Number of keys processed
        key_count: u64,
    },
    /// Auxiliary field
    AuxField {
        /// Field key
        key: String,
        /// Field value
        value: String,
    },
    /// Resize database hint
    ResizeDatabase {
        /// Database ID
        database_id: u32,
        /// Hash table size
        hash_table_size: u32,
        /// Expire hash table size
        expire_hash_table_size: u32,
    },
    /// End of RDB file
    EndOfFile {
        /// File checksum
        checksum: Option<u64>,
        /// Parsing statistics
        stats: RdbStats,
    },
    /// Error occurred
    Error {
        /// The error that occurred
        error: RedisReplicatorError,
        /// Position where error occurred
        position: u64,
    },
}

/// Trait for handling RDB parsing events
pub trait RdbEventHandler {
    /// Handle an RDB event
    fn handle_event(&mut self, event: RdbEvent) -> Result<()>;
    
    /// Called when parsing starts
    fn on_start(&mut self) -> Result<()> {
        Ok(())
    }
    
    /// Called when parsing completes
    fn on_complete(&mut self, _stats: &RdbStats) -> Result<()> {
        Ok(())
    }
    
    /// Called when an error occurs
    fn on_error(&mut self, error: &RedisReplicatorError, _position: u64) -> Result<()> {
        Err(error.clone())
    }
}

/// Simple event handler that collects all entries
#[derive(Debug, Default)]
pub struct CollectingEventHandler {
    /// Collected RDB entries
    pub entries: Vec<Entry>,
    /// RDB information
    pub info: Option<RdbInfo>,
    /// RDB statistics
    pub stats: Option<RdbStats>,
}

impl RdbEventHandler for CollectingEventHandler {
    fn handle_event(&mut self, event: RdbEvent) -> Result<()> {
        match event {
            RdbEvent::StartOfFile { info, .. } => {
                self.info = Some(info);
            },
            RdbEvent::KeyValue { entry, .. } => {
                self.entries.push(Entry::Rdb(entry));
            },
            RdbEvent::EndOfFile { stats, .. } => {
                self.stats = Some(stats);
            },
            _ => {}
        }
        Ok(())
    }
}

/// Event handler that filters entries based on criteria
pub struct FilteringEventHandler<F> {
    /// Filtered RDB entries
    pub entries: Vec<Entry>,
    /// Filter function
    pub filter: F,
}

impl<F> FilteringEventHandler<F>
where
    F: Fn(&RdbEntry) -> bool,
{
    /// Create a new filtering event handler with the given filter function
    pub fn new(filter: F) -> Self {
        Self {
            entries: Vec::new(),
            filter,
        }
    }
}

impl<F> RdbEventHandler for FilteringEventHandler<F>
where
    F: Fn(&RdbEntry) -> bool,
{
    fn handle_event(&mut self, event: RdbEvent) -> Result<()> {
        if let RdbEvent::KeyValue { entry, .. } = event {
            if (self.filter)(&entry) {
                self.entries.push(Entry::Rdb(entry));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdb_stats() {
        let mut stats = RdbStats::new();
        stats.keys_processed = 1000;
        stats.processing_time_ms = 2000; // 2 seconds
        stats.bytes_processed = 1024 * 1024; // 1MB
        
        assert_eq!(stats.keys_per_second(), 500.0);
        assert_eq!(stats.bytes_per_second(), 524288.0);
        assert_eq!(stats.efficiency(), 1.0);
        
        stats.expired_keys_skipped = 100;
        assert!((stats.efficiency() - 0.909).abs() < 0.01);
    }

    #[test]
    fn test_collecting_event_handler() {
        let mut handler = CollectingEventHandler::default();
        
        let info = RdbInfo {
            version: 9,
            redis_version: Some("6.2.0".to_string()),
            creation_time: Some(1234567890),
            used_memory: Some(1024),
            database_count: 1,
            key_count: 10,
            expires_count: 2,
            file_size: 2048,
            checksum: Some(0x1234567890abcdef),
        };
        
        let event = RdbEvent::StartOfFile {
            version: 9,
            info: info.clone(),
        };
        
        handler.handle_event(event).unwrap();
        assert!(handler.info.is_some());
        assert_eq!(handler.info.as_ref().unwrap().version, 9);
    }

    #[test]
    fn test_filtering_event_handler() {
        let filter = |entry: &RdbEntry| {
            if let RdbEntry::KeyValue { key, .. } = entry {
                key.starts_with("user:")
            } else {
                false
            }
        };
        
        let handler = FilteringEventHandler::new(filter);
        
        // This would normally come from actual parsing
        // Just testing the handler logic here
        assert_eq!(handler.entries.len(), 0);
    }
}