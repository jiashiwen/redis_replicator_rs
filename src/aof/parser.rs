//! AOF parser implementation
//!
//! This module provides the main AOF parser that can read and parse
//! Redis AOF (Append Only File) files.

use crate::aof::protocol::{RespParser, RespValue};
use crate::aof::{AofEventHandler, AofInfo, AofStats};
use crate::error::{RedisReplicatorError, Result};
use crate::types::AofEntry;
use chrono::DateTime;
use std::time::SystemTime;
use tokio::io::AsyncBufRead;

/// AOF parsing options
#[derive(Debug, Clone)]
pub struct AofParseOptions {
    /// Whether to validate only (don't collect entries)
    pub validate_only: bool,
    /// Maximum number of entries to parse (0 = unlimited)
    pub max_entries: u64,
    /// Skip entries before this position
    pub skip_entries: u64,
    /// Filter commands by name (empty = all commands)
    pub command_filter: Vec<String>,
    /// Filter by database ID (None = all databases)
    pub database_filter: Option<u32>,
    /// Whether to parse timestamps from commands
    pub parse_timestamps: bool,
    /// Progress reporting interval (in entries)
    pub progress_interval: u64,
    /// Maximum memory usage during parsing
    pub max_memory_usage: usize,
    /// Whether to continue on parse errors
    pub continue_on_error: bool,
    /// RESP parser limits
    pub resp_max_depth: usize,
    /// Maximum bulk string size
    pub resp_max_bulk_size: usize,
    /// Maximum array size
    pub resp_max_array_size: usize,
}

impl Default for AofParseOptions {
    fn default() -> Self {
        Self {
            validate_only: false,
            max_entries: 0,
            skip_entries: 0,
            command_filter: Vec::new(),
            database_filter: None,
            parse_timestamps: true,
            progress_interval: 10000,
            max_memory_usage: 1024 * 1024 * 1024, // 1GB
            continue_on_error: false,
            resp_max_depth: 32,
            resp_max_bulk_size: 512 * 1024 * 1024, // 512MB
            resp_max_array_size: 1024 * 1024,      // 1M elements
        }
    }
}

impl AofParseOptions {
    /// Create options for validation only
    pub fn validate_only() -> Self {
        Self {
            validate_only: true,
            ..Default::default()
        }
    }
    
    /// Create options with command filter
    pub fn with_commands(commands: Vec<String>) -> Self {
        Self {
            command_filter: commands.into_iter().map(|s| s.to_uppercase()).collect(),
            ..Default::default()
        }
    }
    
    /// Create options with database filter
    pub fn with_database(db_id: u32) -> Self {
        Self {
            database_filter: Some(db_id),
            ..Default::default()
        }
    }
    
    /// Create options with entry limits
    pub fn with_limits(max_entries: u64, skip_entries: u64) -> Self {
        Self {
            max_entries,
            skip_entries,
            ..Default::default()
        }
    }
}

/// AOF parser
#[derive(Debug)]
pub struct AofParser {
    options: AofParseOptions,
    resp_parser: RespParser,
    stats: AofStats,
    current_database: u32,
    entries_processed: u64,
    bytes_processed: u64,
    memory_usage: usize,
}

impl AofParser {
    /// Create a new AOF parser with the given options
    pub fn new(options: AofParseOptions) -> Self {
        let resp_parser = RespParser::with_limits(
            options.resp_max_depth,
            options.resp_max_bulk_size,
            options.resp_max_array_size,
        );
        
        Self {
            options,
            resp_parser,
            stats: AofStats::new(),
            current_database: 0,
            entries_processed: 0,
            bytes_processed: 0,
            memory_usage: 0,
        }
    }
    
    /// Parse AOF data from a buffered reader
    pub async fn parse_reader<R: AsyncBufRead + Unpin>(
        &mut self,
        mut reader: R,
    ) -> Result<Vec<AofEntry>> {
        let mut entries = Vec::new();
        
        loop {
            // Check memory usage
            if self.memory_usage > self.options.max_memory_usage {
                return Err(RedisReplicatorError::resource_error(
                    "Memory usage limit exceeded during AOF parsing".to_string(),
                ));
            }
            
            // Check entry limit
            if self.options.max_entries > 0 && self.entries_processed >= self.options.max_entries {
                break;
            }
            
            // Try to parse the next RESP value
            let resp_value = match self.resp_parser.parse_value(&mut reader).await {
                Ok(value) => value,
                Err(e) => {
                    if self.options.continue_on_error {
                tracing::warn!("Parse error: {}", e);
                        self.stats.parse_errors += 1;
                        continue;
                    } else {
                        // Check if this is EOF
                        if e.to_string().contains("Unexpected end of stream") {
                            break;
                        }
                        return Err(e);
                    }
                }
            };
            
            // Convert RESP value to AOF entry
            match self.process_resp_value(resp_value) {
                Ok(Some(entry)) => {
                    if !self.options.validate_only {
                        let command_size = match &entry {
                            AofEntry::Command { args, .. } => args.iter().map(|arg| arg.len()).sum::<usize>(),
                            _ => 0,
                        };
                        self.memory_usage += std::mem::size_of::<AofEntry>() + command_size;
                        entries.push(entry);
                    }
                }
                Ok(None) => {
                    // Entry was filtered out or skipped
                }
                Err(e) => {
                    if self.options.continue_on_error {
                tracing::warn!("Processing error: {}", e);
                        self.stats.protocol_errors += 1;
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
            
            // Report progress
            if self.options.progress_interval > 0
                && self.entries_processed % self.options.progress_interval == 0
            {
                tracing::debug!(
                    "Processed {} entries, {} bytes",
                    self.entries_processed, self.bytes_processed
                );
            }
        }
        
        self.stats.complete();
        tracing::info!(
            "AOF parsing completed: {} entries, {} bytes, {:?}",
            self.entries_processed,
            self.bytes_processed,
            self.stats.duration()
        );
        
        Ok(entries)
    }
    
    /// Parse AOF data with an event handler
    pub async fn parse_with_handler<R: AsyncBufRead + Unpin, H: AofEventHandler>(
        &mut self,
        mut reader: R,
        handler: &mut H,
    ) -> Result<()> {
        handler.on_start(None)?;
        
        loop {
            // Check memory usage
            if self.memory_usage > self.options.max_memory_usage {
                let error = RedisReplicatorError::resource_error(
                    "Memory usage limit exceeded during AOF parsing".to_string(),
                );
                handler.on_error(error.clone(), self.bytes_processed)?;
                return Err(error);
            }
            
            // Check entry limit
            if self.options.max_entries > 0 && self.entries_processed >= self.options.max_entries {
                break;
            }
            
            // Try to parse the next RESP value
            let resp_value = match self.resp_parser.parse_value(&mut reader).await {
                Ok(value) => value,
                Err(e) => {
                    if self.options.continue_on_error {
                        handler.on_error(e, self.bytes_processed)?;
                        self.stats.parse_errors += 1;
                        continue;
                    } else {
                        // Check if this is EOF
                        if e.to_string().contains("Unexpected end of stream") {
                            break;
                        }
                        handler.on_error(e.clone(), self.bytes_processed)?;
                        return Err(e);
                    }
                }
            };
            
            // Convert RESP value to AOF entry
            match self.process_resp_value(resp_value) {
                Ok(Some(entry)) => {
                    handler.on_command(entry, self.bytes_processed)?;
                }
                Ok(None) => {
                    // Entry was filtered out or skipped
                }
                Err(e) => {
                    if self.options.continue_on_error {
                        handler.on_error(e, self.bytes_processed)?;
                        self.stats.protocol_errors += 1;
                        continue;
                    } else {
                        handler.on_error(e.clone(), self.bytes_processed)?;
                        return Err(e);
                    }
                }
            }
            
            // Report progress
            if self.options.progress_interval > 0
                && self.entries_processed % self.options.progress_interval == 0
            {
                handler.on_progress(
                    self.bytes_processed,
                    None,
                    self.entries_processed,
                )?;
            }
        }
        
        self.stats.complete();
        handler.on_complete(self.stats.clone())?;
        
        Ok(())
    }
    
    /// Validate AOF data from a buffered reader
    pub async fn validate_reader<R: AsyncBufRead + Unpin>(
        &mut self,
        reader: R,
    ) -> Result<AofInfo> {
        let mut info = AofInfo::default();
        let start_time = SystemTime::now();
        
        // Set validation mode
        self.options.validate_only = true;
        
        match self.parse_reader(reader).await {
            Ok(_) => {
                info.is_valid = true;
            }
            Err(e) => {
                info.is_valid = false;
                info.add_warning(format!("Validation failed: {}", e));
            }
        }
        
        info.command_count = self.stats.commands_parsed;
        info.file_size = self.stats.bytes_processed;
        info.estimated_memory = self.memory_usage as u64;
        info.parse_duration = start_time.elapsed().unwrap_or_default();
        
        if self.stats.parse_errors > 0 {
            info.add_warning(format!("{} parse errors encountered", self.stats.parse_errors));
        }
        
        if self.stats.protocol_errors > 0 {
            info.add_warning(format!(
                "{} protocol errors encountered",
                self.stats.protocol_errors
            ));
        }
        
        Ok(info)
    }
    
    /// Process a RESP value and convert it to an AOF entry
    fn process_resp_value(&mut self, value: RespValue) -> Result<Option<AofEntry>> {
        // Update statistics
        self.bytes_processed += value.memory_usage() as u64;
        
        // Check if this is a command array
        if !crate::aof::protocol::utils::is_command(&value) {
            return Err(RedisReplicatorError::protocol_error("protocol", "Expected command array in AOF".to_string(),
            ));
        }
        
        // Convert to command
        let command = crate::aof::protocol::utils::array_to_command(&value)?;
        
        if command.is_empty() {
            return Err(RedisReplicatorError::protocol_error("data", "Empty command in AOF".to_string(),
            ));
        }
        
        // Get command name
        let command_name = String::from_utf8_lossy(&command[0]).to_uppercase();
        
        // Handle special commands
        match command_name.as_str() {
            "SELECT" => {
                if command.len() >= 2 {
                    if let Ok(db_str) = String::from_utf8(command[1].clone()) {
                        if let Ok(db_id) = db_str.parse::<u32>() {
                            self.current_database = db_id;
                        }
                    }
                }
                // Don't include SELECT commands in output unless specifically requested
                if !self.options.command_filter.is_empty()
                    && !self.options.command_filter.contains(&command_name)
                {
                    return Ok(None);
                }
            }
            _ => {}
        }
        
        // Apply filters
        if !self.options.command_filter.is_empty()
            && !self.options.command_filter.contains(&command_name)
        {
            return Ok(None);
        }
        
        if let Some(db_filter) = self.options.database_filter {
            if self.current_database != db_filter {
                return Ok(None);
            }
        }
        
        // Skip entries if needed
        if self.entries_processed < self.options.skip_entries {
            self.entries_processed += 1;
            return Ok(None);
        }
        
        // Parse timestamp if enabled
        let timestamp = if self.options.parse_timestamps {
            self.extract_timestamp(&command)
        } else {
            None
        };
        
        // Create AOF entry
        let entry = AofEntry::Command {
            args: command.into_iter().map(|arg| String::from_utf8_lossy(&arg).to_string()).collect(),
            timestamp: timestamp.map(|ts| DateTime::from_timestamp(ts.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64, 0).unwrap()),
        };
        
        // Update statistics
        self.entries_processed += 1;
        self.stats.commands_parsed += 1;
        
        Ok(Some(entry))
    }
    
    /// Extract timestamp from command if available
    fn extract_timestamp(&self, command: &[Vec<u8>]) -> Option<SystemTime> {
        if command.is_empty() {
            return None;
        }
        
        let command_name = String::from_utf8_lossy(&command[0]).to_uppercase();
        
        // Look for timestamp in commands that support it
        match command_name.as_str() {
            "SET" | "SETEX" | "PSETEX" => {
                // Check for EX/PX options with timestamps
                for i in 1..command.len() {
                    let arg = String::from_utf8_lossy(&command[i]).to_uppercase();
                    if (arg == "EX" || arg == "PX") && i + 1 < command.len() {
                        if let Ok(timestamp_str) = String::from_utf8(command[i + 1].clone()) {
                            if let Ok(timestamp) = timestamp_str.parse::<i64>() {
                                return Some(crate::aof::utils::timestamp_to_system_time(timestamp));
                            }
                        }
                    }
                }
            }
            "EXPIRE" | "EXPIREAT" | "PEXPIRE" | "PEXPIREAT" => {
                if command.len() >= 3 {
                    if let Ok(timestamp_str) = String::from_utf8(command[2].clone()) {
                        if let Ok(timestamp) = timestamp_str.parse::<i64>() {
                            return Some(crate::aof::utils::timestamp_to_system_time(timestamp));
                        }
                    }
                }
            }
            _ => {}
        }
        
        None
    }
    
    /// Get current parsing statistics
    pub fn stats(&self) -> &AofStats {
        &self.stats
    }
    
    /// Get current memory usage
    pub fn memory_usage(&self) -> usize {
        self.memory_usage
    }
    
    /// Get number of entries processed
    pub fn entries_processed(&self) -> u64 {
        self.entries_processed
    }
    
    /// Get number of bytes processed
    pub fn bytes_processed(&self) -> u64 {
        self.bytes_processed
    }
    
    /// Reset parser state
    pub fn reset(&mut self) {
        self.stats = AofStats::new();
        self.current_database = 0;
        self.entries_processed = 0;
        self.bytes_processed = 0;
        self.memory_usage = 0;
    }
}

/// Utility functions for AOF parsing
pub mod utils {
    use super::*;
    
    /// Parse AOF file and return basic statistics
    pub async fn get_aof_stats<R: AsyncBufRead + Unpin>(reader: R) -> Result<AofStats> {
        let options = AofParseOptions::validate_only();
        let mut parser = AofParser::new(options);
        parser.parse_reader(reader).await?;
        Ok(parser.stats().clone())
    }
    
    /// Count commands in AOF file
    pub async fn count_commands<R: AsyncBufRead + Unpin>(reader: R) -> Result<u64> {
        let stats = get_aof_stats(reader).await?;
        Ok(stats.commands_parsed)
    }
    
    /// Extract unique command names from AOF file
    pub async fn extract_command_names<R: AsyncBufRead + Unpin>(
        reader: R,
    ) -> Result<Vec<String>> {
        let options = AofParseOptions::default();
        let mut parser = AofParser::new(options);
        let entries = parser.parse_reader(reader).await?;
        
        let mut commands = std::collections::HashSet::new();
        for entry in entries {
            match entry {
                AofEntry::Command { args, .. } => {
                    if !args.is_empty() {
                        let command_name = args[0].to_uppercase();
                        commands.insert(command_name);
                    }
                }
                _ => {} // Skip non-command entries
            }
        }
        
        let mut result: Vec<String> = commands.into_iter().collect();
        result.sort();
        Ok(result)
    }
    
    /// Check if AOF data is valid
    pub async fn is_valid_aof<R: AsyncBufRead + Unpin>(reader: R) -> bool {
        let options = AofParseOptions::validate_only();
        let mut parser = AofParser::new(options);
        parser.parse_reader(reader).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_parse_simple_command() {
        let aof_data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let reader = BufReader::new(Cursor::new(aof_data));
        
        let mut parser = AofParser::new(AofParseOptions::default());
        let entries = parser.parse_reader(reader).await.unwrap();
        
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
    async fn test_parse_with_select() {
        let aof_data = b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let reader = BufReader::new(Cursor::new(aof_data));
        
        let mut parser = AofParser::new(AofParseOptions::default());
        let entries = parser.parse_reader(reader).await.unwrap();
        
        assert_eq!(entries.len(), 1); // SELECT is filtered out by default
    }

    #[tokio::test]
    async fn test_command_filter() {
        let aof_data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let reader = BufReader::new(Cursor::new(aof_data));
        
        let options = AofParseOptions::with_commands(vec!["SET".to_string()]);
        let mut parser = AofParser::new(options);
        let entries = parser.parse_reader(reader).await.unwrap();
        
        assert_eq!(entries.len(), 1);
        if let AofEntry::Command { args, .. } = &entries[0] {
            assert_eq!(args[0], "SET");
        } else {
            panic!("Expected Command entry");
        }
    }

    #[tokio::test]
    async fn test_database_filter() {
        let aof_data = b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let reader = BufReader::new(Cursor::new(aof_data));
        
        let options = AofParseOptions::with_database(0);
        let mut parser = AofParser::new(options);
        let entries = parser.parse_reader(reader).await.unwrap();
        
        assert_eq!(entries.len(), 0); // SET is in database 1, filter is for database 0
    }

    #[tokio::test]
    async fn test_entry_limits() {
        let aof_data = b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n*3\r\n$3\r\nSET\r\n$4\r\nkey3\r\n$6\r\nvalue3\r\n";
        let reader = BufReader::new(Cursor::new(aof_data));
        
        let options = AofParseOptions::with_limits(2, 1);
        let mut parser = AofParser::new(options);
        let entries = parser.parse_reader(reader).await.unwrap();
        
        assert_eq!(entries.len(), 2); // Skip 1, take 2
        if let AofEntry::Command { args, .. } = &entries[0] {
            assert_eq!(args[1], "key2");
        } else {
            panic!("Expected Command entry");
        }
        if let AofEntry::Command { args, .. } = &entries[1] {
            assert_eq!(args[1], "key3");
        } else {
            panic!("Expected Command entry");
        }
    }

    #[tokio::test]
    async fn test_validation() {
        let valid_aof = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let reader = BufReader::new(Cursor::new(valid_aof));
        
        let mut parser = AofParser::new(AofParseOptions::default());
        let info = parser.validate_reader(reader).await.unwrap();
        
        assert!(info.is_valid);
        assert_eq!(info.command_count, 1);
    }

    #[tokio::test]
    async fn test_stats() {
        let aof_data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let reader = BufReader::new(Cursor::new(aof_data));
        
        let stats = utils::get_aof_stats(reader).await.unwrap();
        assert_eq!(stats.commands_parsed, 1);
        assert!(stats.bytes_processed > 0);
    }

    #[tokio::test]
    async fn test_command_extraction() {
        let aof_data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let reader = BufReader::new(Cursor::new(aof_data));
        
        let commands = utils::extract_command_names(reader).await.unwrap();
        assert_eq!(commands, vec!["GET", "SET"]);
    }

    #[tokio::test]
    async fn test_continue_on_error() {
        // Invalid RESP data mixed with valid data
        let aof_data = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\ninvalid\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let reader = BufReader::new(Cursor::new(aof_data));
        
        let options = AofParseOptions {
            continue_on_error: true,
            ..Default::default()
        };
        let mut parser = AofParser::new(options);
        let entries = parser.parse_reader(reader).await.unwrap();
        
        // Should get the valid entries despite the error
        assert_eq!(entries.len(), 2);
        assert!(parser.stats().parse_errors > 0);
    }
}