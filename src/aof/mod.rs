//! AOF (Append Only File) parser module
//!
//! This module provides functionality to parse Redis AOF files, which contain
//! a log of all write operations performed on a Redis database.

use crate::error::{RedisReplicatorError, Result};
use crate::types::AofEntry;
use bytes::Bytes;
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

pub mod parser;
pub mod protocol;

pub use parser::{AofParser, AofParseOptions};
pub use protocol::{RespValue, RespParser};

/// Parse an AOF file and return all entries
pub async fn parse_file<P: AsRef<Path>>(path: P) -> Result<Vec<AofEntry>> {
    parse_file_with_options(path, AofParseOptions::default()).await
}

/// Parse an AOF file with custom options
pub async fn parse_file_with_options<P: AsRef<Path>>(
    path: P,
    options: AofParseOptions,
) -> Result<Vec<AofEntry>> {
    let file = File::open(path.as_ref()).await.map_err(|e| {
        RedisReplicatorError::io_error(format!(
            "Failed to open AOF file '{}': {}",
            path.as_ref().display(),
            e
        ))
    })?;

    let reader = BufReader::new(file);
    let mut parser = AofParser::new(options);
    parser.parse_reader(reader).await
}

/// Parse AOF data from bytes
pub async fn parse_bytes(data: Bytes) -> Result<Vec<AofEntry>> {
    parse_bytes_with_options(data, AofParseOptions::default()).await
}

/// Parse AOF data from bytes with custom options
pub async fn parse_bytes_with_options(
    data: Bytes,
    options: AofParseOptions,
) -> Result<Vec<AofEntry>> {
    let cursor = std::io::Cursor::new(data);
    let reader = BufReader::new(cursor);
    let mut parser = AofParser::new(options);
    parser.parse_reader(reader).await
}

/// Validate an AOF file structure
pub async fn validate_file<P: AsRef<Path>>(path: P) -> Result<AofInfo> {
    let file = File::open(path.as_ref()).await.map_err(|e| {
        RedisReplicatorError::io_error(format!(
            "Failed to open AOF file '{}': {}",
            path.as_ref().display(),
            e
        ))
    })?;

    let reader = BufReader::new(file);
    let options = AofParseOptions {
        validate_only: true,
        ..Default::default()
    };
    let mut parser = AofParser::new(options);
    parser.validate_reader(reader).await
}

/// Information about an AOF file
#[derive(Debug, Clone)]
pub struct AofInfo {
    /// Total number of commands
    pub command_count: u64,
    /// Number of different command types
    pub command_types: HashMap<String, u64>,
    /// File size in bytes
    pub file_size: u64,
    /// Estimated memory usage
    pub estimated_memory: u64,
    /// Parse duration
    pub parse_duration: Duration,
    /// Whether the file is valid
    pub is_valid: bool,
    /// Any warnings encountered
    pub warnings: Vec<String>,
    /// First command timestamp (if available)
    pub first_timestamp: Option<SystemTime>,
    /// Last command timestamp (if available)
    pub last_timestamp: Option<SystemTime>,
}

impl Default for AofInfo {
    fn default() -> Self {
        Self {
            command_count: 0,
            command_types: HashMap::new(),
            file_size: 0,
            estimated_memory: 0,
            parse_duration: Duration::from_secs(0),
            is_valid: true,
            warnings: Vec::new(),
            first_timestamp: None,
            last_timestamp: None,
        }
    }
}

impl AofInfo {
    /// Add a command to the statistics
    pub fn add_command(&mut self, command: &str, timestamp: Option<SystemTime>) {
        self.command_count += 1;
        *self.command_types.entry(command.to_uppercase()).or_insert(0) += 1;
        
        if let Some(ts) = timestamp {
            if self.first_timestamp.is_none() || Some(ts) < self.first_timestamp {
                self.first_timestamp = Some(ts);
            }
            if self.last_timestamp.is_none() || Some(ts) > self.last_timestamp {
                self.last_timestamp = Some(ts);
            }
        }
    }
    
    /// Add a warning
    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }
    
    /// Mark as invalid
    pub fn mark_invalid(&mut self) {
        self.is_valid = false;
    }
    
    /// Get the most common command types
    pub fn top_commands(&self, limit: usize) -> Vec<(String, u64)> {
        let mut commands: Vec<_> = self.command_types.iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        commands.sort_by(|a, b| b.1.cmp(&a.1));
        commands.truncate(limit);
        commands
    }
    
    /// Get command diversity (number of unique commands / total commands)
    pub fn command_diversity(&self) -> f64 {
        if self.command_count == 0 {
            0.0
        } else {
            self.command_types.len() as f64 / self.command_count as f64
        }
    }
    
    /// Get average commands per second (if timestamps available)
    pub fn commands_per_second(&self) -> Option<f64> {
        if let (Some(first), Some(last)) = (self.first_timestamp, self.last_timestamp) {
            let duration = last.duration_since(first).ok()?;
            if duration.as_secs_f64() > 0.0 {
                Some(self.command_count as f64 / duration.as_secs_f64())
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// AOF parsing statistics
#[derive(Debug, Clone, Default)]
pub struct AofStats {
    /// Number of commands parsed
    pub commands_parsed: u64,
    /// Number of bytes processed
    pub bytes_processed: u64,
    /// Number of parse errors encountered
    pub parse_errors: u64,
    /// Number of protocol errors
    pub protocol_errors: u64,
    /// Parse start time
    pub start_time: Option<SystemTime>,
    /// Parse end time
    pub end_time: Option<SystemTime>,
    /// Memory usage during parsing
    pub peak_memory_usage: usize,
}

impl AofStats {
    /// Create new statistics
    pub fn new() -> Self {
        Self {
            start_time: Some(SystemTime::now()),
            ..Default::default()
        }
    }
    
    /// Mark parsing as complete
    pub fn complete(&mut self) {
        self.end_time = Some(SystemTime::now());
    }
    
    /// Get parsing duration
    pub fn duration(&self) -> Option<Duration> {
        if let (Some(start), Some(end)) = (self.start_time, self.end_time) {
            end.duration_since(start).ok()
        } else {
            None
        }
    }
    
    /// Get parsing rate in commands per second
    pub fn commands_per_second(&self) -> Option<f64> {
        if let Some(duration) = self.duration() {
            let secs = duration.as_secs_f64();
            if secs > 0.0 {
                Some(self.commands_parsed as f64 / secs)
            } else {
                None
            }
        } else {
            None
        }
    }
    
    /// Get parsing rate in bytes per second
    pub fn bytes_per_second(&self) -> Option<f64> {
        if let Some(duration) = self.duration() {
            let secs = duration.as_secs_f64();
            if secs > 0.0 {
                Some(self.bytes_processed as f64 / secs)
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// AOF event types
#[derive(Debug, Clone)]
pub enum AofEvent {
    /// Command parsed
    Command {
        /// The parsed command
        entry: AofEntry,
        /// Position in file
        position: u64,
    },
    /// Parse error encountered
    Error {
        /// The error
        error: RedisReplicatorError,
        /// Position where error occurred
        position: u64,
    },
    /// Parsing started
    Started {
        /// File size (if known)
        file_size: Option<u64>,
    },
    /// Parsing completed
    Completed {
        /// Final statistics
        stats: AofStats,
    },
    /// Progress update
    Progress {
        /// Bytes processed so far
        bytes_processed: u64,
        /// Total bytes (if known)
        total_bytes: Option<u64>,
        /// Commands processed
        commands_processed: u64,
    },
}

/// Trait for handling AOF parsing events
pub trait AofEventHandler: Send + Sync {
    /// Handle an AOF event
    fn handle_event(&mut self, event: AofEvent) -> Result<()>;
    
    /// Called when parsing starts
    fn on_start(&mut self, file_size: Option<u64>) -> Result<()> {
        self.handle_event(AofEvent::Started { file_size })
    }
    
    /// Called when a command is parsed
    fn on_command(&mut self, entry: AofEntry, position: u64) -> Result<()> {
        self.handle_event(AofEvent::Command { entry, position })
    }
    
    /// Called when an error occurs
    fn on_error(&mut self, error: RedisReplicatorError, position: u64) -> Result<()> {
        self.handle_event(AofEvent::Error { error, position })
    }
    
    /// Called when parsing completes
    fn on_complete(&mut self, stats: AofStats) -> Result<()> {
        self.handle_event(AofEvent::Completed { stats })
    }
    
    /// Called for progress updates
    fn on_progress(
        &mut self,
        bytes_processed: u64,
        total_bytes: Option<u64>,
        commands_processed: u64,
    ) -> Result<()> {
        self.handle_event(AofEvent::Progress {
            bytes_processed,
            total_bytes,
            commands_processed,
        })
    }
}

/// A simple event handler that collects all entries
#[derive(Debug, Default)]
pub struct CollectingEventHandler {
    /// Collected AOF entries
    pub entries: Vec<AofEntry>,
    /// Parsing errors with their positions
    pub errors: Vec<(RedisReplicatorError, u64)>,
    /// AOF statistics
    pub stats: Option<AofStats>,
}

impl AofEventHandler for CollectingEventHandler {
    fn handle_event(&mut self, event: AofEvent) -> Result<()> {
        match event {
            AofEvent::Command { entry, .. } => {
                self.entries.push(entry);
            }
            AofEvent::Error { error, position } => {
                self.errors.push((error, position));
            }
            AofEvent::Completed { stats } => {
                self.stats = Some(stats);
            }
            _ => {}
        }
        Ok(())
    }
}

/// An event handler that filters entries based on criteria
#[derive(Debug)]
pub struct FilteringEventHandler<F> {
    /// Filtered AOF entries
    pub entries: Vec<AofEntry>,
    /// Filter function
    pub filter: F,
    /// AOF statistics
    pub stats: Option<AofStats>,
}

impl<F> FilteringEventHandler<F>
where
    F: Fn(&AofEntry) -> bool,
{
    /// Create a new filtering event handler with the given filter function
    pub fn new(filter: F) -> Self {
        Self {
            entries: Vec::new(),
            filter,
            stats: None,
        }
    }
}

impl<F> AofEventHandler for FilteringEventHandler<F>
where
    F: Fn(&AofEntry) -> bool + Send + Sync,
{
    fn handle_event(&mut self, event: AofEvent) -> Result<()> {
        match event {
            AofEvent::Command { entry, .. } => {
                if (self.filter)(&entry) {
                    self.entries.push(entry);
                }
            }
            AofEvent::Completed { stats } => {
                self.stats = Some(stats);
            }
            _ => {}
        }
        Ok(())
    }
}

/// Utility functions for AOF parsing
pub mod utils {
    use super::*;
    
    /// Check if a file appears to be an AOF file
    pub async fn is_aof_file<P: AsRef<Path>>(path: P) -> Result<bool> {
        let mut file = File::open(path).await.map_err(|e| {
            RedisReplicatorError::io_error(format!("Failed to open file: {}", e))
        })?;
        
        let mut buffer = [0u8; 1024];
        let bytes_read = file.read(&mut buffer).await.map_err(|e| {
            RedisReplicatorError::io_error(format!("Failed to read file: {}", e))
        })?;
        
        if bytes_read == 0 {
            return Ok(false);
        }
        
        // Check for RESP protocol markers
        let content = &buffer[..bytes_read];
        Ok(content.iter().any(|&b| matches!(b, b'*' | b'$' | b'+' | b'-' | b':')))
    }
    
    /// Get file size
    pub async fn get_file_size<P: AsRef<Path>>(path: P) -> Result<u64> {
        let metadata = tokio::fs::metadata(path).await.map_err(|e| {
            RedisReplicatorError::io_error(format!("Failed to get file metadata: {}", e))
        })?;
        Ok(metadata.len())
    }
    
    /// Convert timestamp to SystemTime
    pub fn timestamp_to_system_time(timestamp: i64) -> SystemTime {
        if timestamp > 0 {
            UNIX_EPOCH + Duration::from_secs(timestamp as u64)
        } else {
            SystemTime::now()
        }
    }
    
    /// Convert SystemTime to timestamp
    pub fn system_time_to_timestamp(time: SystemTime) -> i64 {
        time.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }
    
    /// Estimate memory usage of an AOF entry
    pub fn estimate_entry_size(entry: &AofEntry) -> usize {
        let base_size = std::mem::size_of::<AofEntry>();
        let command_size = match entry {
            AofEntry::Command { args, .. } => args.iter().map(|arg| arg.len()).sum::<usize>(),
            _ => 0,
        };
        base_size + command_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_parse_simple_aof() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let aof_content = b"*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        temp_file.write_all(aof_content).unwrap();
        
        let entries = parse_file(temp_file.path()).await.unwrap();
        assert_eq!(entries.len(), 1);
        if let AofEntry::Command { args, .. } = &entries[0] {
            assert_eq!(args.len(), 3);
            assert_eq!(args[0], "SET");
            assert_eq!(args[1], "key");
            assert_eq!(args[2], "value");
        } else {
            panic!("Expected Command entry");
        }
    }

    #[tokio::test]
    async fn test_parse_bytes() {
        let aof_content = Bytes::from_static(b"*2\r\n$4\r\nPING\r\n");
        let entries = parse_bytes(aof_content).await.unwrap();
        assert_eq!(entries.len(), 1);
        if let AofEntry::Command { args, .. } = &entries[0] {
            assert_eq!(args[0], "PING");
        } else {
            panic!("Expected Command entry");
        }
    }

    #[tokio::test]
    async fn test_aof_info() {
        let mut info = AofInfo::default();
        info.add_command("SET", None);
        info.add_command("GET", None);
        info.add_command("SET", None);
        
        assert_eq!(info.command_count, 3);
        assert_eq!(info.command_types.get("SET"), Some(&2));
        assert_eq!(info.command_types.get("GET"), Some(&1));
        
        let top = info.top_commands(2);
        assert_eq!(top[0].0, "SET");
        assert_eq!(top[0].1, 2);
    }

    #[tokio::test]
    async fn test_collecting_event_handler() {
        let mut handler = CollectingEventHandler::default();
        
        let entry = AofEntry::Command {
            args: vec!["SET".to_string(), "key".to_string(), "value".to_string()],
            timestamp: None,
        };
        
        handler.on_command(entry.clone(), 0).unwrap();
        assert_eq!(handler.entries.len(), 1);
        match (&handler.entries[0], &entry) {
            (AofEntry::Command { args: args1, .. }, AofEntry::Command { args: args2, .. }) => {
                assert_eq!(args1, args2);
            }
            _ => panic!("Expected Command entries"),
        }
    }

    #[tokio::test]
    async fn test_filtering_event_handler() {
        let mut handler = FilteringEventHandler::new(|entry: &AofEntry| {
            match entry {
                AofEntry::Command { args, .. } => args.get(0).map_or(false, |cmd| cmd == "SET"),
                _ => false,
            }
        });
        
        let set_entry = AofEntry::Command {
            args: vec!["SET".to_string(), "key".to_string(), "value".to_string()],
            timestamp: None,
        };
        
        let get_entry = AofEntry::Command {
            args: vec!["GET".to_string(), "key".to_string()],
            timestamp: None,
        };
        
        handler.on_command(set_entry.clone(), 0).unwrap();
        handler.on_command(get_entry, 0).unwrap();
        
        assert_eq!(handler.entries.len(), 1);
        match (&handler.entries[0], &set_entry) {
            (AofEntry::Command { args: args1, .. }, AofEntry::Command { args: args2, .. }) => {
                assert_eq!(args1, args2);
            }
            _ => panic!("Expected Command entries"),
        }
    }

    #[test]
    fn test_aof_stats() {
        let mut stats = AofStats::new();
        assert!(stats.start_time.is_some());
        
        stats.commands_parsed = 100;
        stats.complete();
        
        assert!(stats.end_time.is_some());
        assert!(stats.duration().is_some());
    }

    #[test]
    fn test_utils() {
        let entry = AofEntry::Command {
            args: vec!["SET".to_string(), "key".to_string(), "value".to_string()],
            timestamp: None,
        };
        
        let size = utils::estimate_entry_size(&entry);
        assert!(size > 0);
        
        let now = SystemTime::now();
        let timestamp = utils::system_time_to_timestamp(now);
        let converted = utils::timestamp_to_system_time(timestamp);
        
        // Should be close (within a second)
        assert!(now.duration_since(converted).unwrap_or_default().as_secs() <= 1);
    }
}