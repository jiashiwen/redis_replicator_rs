//! Redis replication module
//!
//! This module provides functionality to connect to a Redis master server
//! and replicate its data using the Redis replication protocol.

use crate::error::{RedisReplicatorError, Result};
use crate::types::{Entry, ReplicationEntry};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};
use tokio::net::TcpStream;
use tokio::time::timeout;

pub mod client;
pub mod protocol;
// pub mod stream;

pub use client::{ReplicationClient, ReplicationClientConfig as ReplicationConfig};
pub use protocol::{ReplicationCommand, ReplicationResponse};
// pub use stream::{ReplicationStream, ReplicationStreamOptions};

// /// Connect to a Redis master and start replication
// pub async fn replicate_from_master(
//     config: ReplicationConfig,
// ) -> Result<ReplicationStream> {
//     let mut client = ReplicationClient::new(config).await?;
//     client.start_replication().await
// }

// /// Connect to a Redis master with custom options
// pub async fn replicate_with_options(
//     config: ReplicationConfig,
//     options: ReplicationStreamOptions,
// ) -> Result<ReplicationStream> {
//     let mut client = ReplicationClient::with_options(config, options).await?
//     client.start_replication().await
// }

/// Replication information
#[derive(Debug, Clone)]
pub struct ReplicationInfo {
    /// Master server address
    pub master_addr: SocketAddr,
    /// Replication ID
    pub repl_id: String,
    /// Replication offset
    pub repl_offset: i64,
    /// Master run ID
    pub master_run_id: String,
    /// Connection state
    pub state: ReplicationState,
    /// Last sync time
    pub last_sync: Option<SystemTime>,
    /// Bytes received
    pub bytes_received: u64,
    /// Commands received
    pub commands_received: u64,
    /// Connection uptime
    pub uptime: Duration,
    /// Lag behind master (if available)
    pub lag: Option<Duration>,
}

impl Default for ReplicationInfo {
    fn default() -> Self {
        Self {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            repl_id: String::new(),
            repl_offset: -1,
            master_run_id: String::new(),
            state: ReplicationState::Disconnected,
            last_sync: None,
            bytes_received: 0,
            commands_received: 0,
            uptime: Duration::from_secs(0),
            lag: None,
        }
    }
}

/// Replication state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationState {
    /// Not connected
    Disconnected,
    /// Connecting to master
    Connecting,
    /// Performing handshake
    Handshake,
    /// Receiving full sync (RDB)
    FullSync,
    /// Receiving incremental sync (commands)
    IncrementalSync,
    /// Connected and syncing
    Connected,
    /// Error state
    Error,
}

impl std::fmt::Display for ReplicationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicationState::Disconnected => write!(f, "disconnected"),
            ReplicationState::Connecting => write!(f, "connecting"),
            ReplicationState::Handshake => write!(f, "handshake"),
            ReplicationState::FullSync => write!(f, "full_sync"),
            ReplicationState::IncrementalSync => write!(f, "incremental_sync"),
            ReplicationState::Connected => write!(f, "connected"),
            ReplicationState::Error => write!(f, "error"),
        }
    }
}

/// Replication statistics
#[derive(Debug, Clone, Default)]
pub struct ReplicationStats {
    /// Total bytes received
    pub bytes_received: u64,
    /// Total commands received
    pub commands_received: u64,
    /// RDB bytes received
    pub rdb_bytes_received: u64,
    /// AOF commands received
    pub aof_commands_received: u64,
    /// Connection attempts
    pub connection_attempts: u64,
    /// Successful connections
    pub successful_connections: u64,
    /// Connection errors
    pub connection_errors: u64,
    /// Protocol errors
    pub protocol_errors: u64,
    /// Last error
    pub last_error: Option<String>,
    /// Start time
    pub start_time: Option<SystemTime>,
    /// Last activity time
    pub last_activity: Option<SystemTime>,
}

impl ReplicationStats {
    /// Create new statistics
    pub fn new() -> Self {
        Self {
            start_time: Some(SystemTime::now()),
            ..Default::default()
        }
    }
    
    /// Record a connection attempt
    pub fn record_connection_attempt(&mut self) {
        self.connection_attempts += 1;
    }
    
    /// Record a successful connection
    pub fn record_successful_connection(&mut self) {
        self.successful_connections += 1;
        self.last_activity = Some(SystemTime::now());
    }
    
    /// Record a connection error
    pub fn record_connection_error(&mut self, error: &str) {
        self.connection_errors += 1;
        self.last_error = Some(error.to_string());
    }
    
    /// Record a protocol error
    pub fn record_protocol_error(&mut self, error: &str) {
        self.protocol_errors += 1;
        self.last_error = Some(error.to_string());
    }
    
    /// Record received data
    pub fn record_data(&mut self, bytes: u64, is_rdb: bool) {
        self.bytes_received += bytes;
        if is_rdb {
            self.rdb_bytes_received += bytes;
        }
        self.last_activity = Some(SystemTime::now());
    }
    
    /// Record received command
    pub fn record_command(&mut self, is_aof: bool) {
        self.commands_received += 1;
        if is_aof {
            self.aof_commands_received += 1;
        }
        self.last_activity = Some(SystemTime::now());
    }
    
    /// Get uptime
    pub fn uptime(&self) -> Duration {
        if let Some(start) = self.start_time {
            SystemTime::now().duration_since(start).unwrap_or_default()
        } else {
            Duration::from_secs(0)
        }
    }
    
    /// Get time since last activity
    pub fn time_since_last_activity(&self) -> Option<Duration> {
        if let Some(last) = self.last_activity {
            SystemTime::now().duration_since(last).ok()
        } else {
            None
        }
    }
    
    /// Get connection success rate
    pub fn connection_success_rate(&self) -> f64 {
        if self.connection_attempts == 0 {
            0.0
        } else {
            self.successful_connections as f64 / self.connection_attempts as f64
        }
    }
    
    /// Get average bytes per second
    pub fn bytes_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs > 0.0 {
            self.bytes_received as f64 / uptime_secs
        } else {
            0.0
        }
    }
    
    /// Get average commands per second
    pub fn commands_per_second(&self) -> f64 {
        let uptime_secs = self.uptime().as_secs_f64();
        if uptime_secs > 0.0 {
            self.commands_received as f64 / uptime_secs
        } else {
            0.0
        }
    }
}

/// Replication event types
#[derive(Debug, Clone)]
pub enum ReplicationEvent {
    /// Connection established
    Connected {
        /// Master address
        master_addr: SocketAddr,
        /// Replication info
        info: ReplicationInfo,
    },
    /// Connection lost
    Disconnected {
        /// Reason for disconnection
        reason: String,
    },
    /// State changed
    StateChanged {
        /// Previous state
        from: ReplicationState,
        /// New state
        to: ReplicationState,
    },
    /// RDB data received
    RdbData {
        /// RDB data chunk
        data: Vec<u8>,
        /// Current offset
        offset: u64,
        /// Total size (if known)
        total_size: Option<u64>,
    },
    /// Command received
    Command {
        /// The replication entry
        entry: ReplicationEntry,
        /// Current replication offset
        offset: i64,
    },
    /// Error occurred
    Error {
        /// The error
        error: RedisReplicatorError,
        /// Whether the error is recoverable
        recoverable: bool,
    },
    /// Heartbeat/ping received
    Heartbeat {
        /// Timestamp
        timestamp: SystemTime,
    },
    /// Full sync completed
    FullSyncCompleted {
        /// RDB size
        rdb_size: u64,
        /// Duration
        duration: Duration,
    },
}

/// Trait for handling replication events
pub trait ReplicationEventHandler: Send + Sync {
    /// Handle a replication event
    fn handle_event(&mut self, event: ReplicationEvent) -> Result<()>;
    
    /// Called when connected to master
    fn on_connected(&mut self, master_addr: SocketAddr, info: ReplicationInfo) -> Result<()> {
        self.handle_event(ReplicationEvent::Connected { master_addr, info })
    }
    
    /// Called when disconnected from master
    fn on_disconnected(&mut self, reason: String) -> Result<()> {
        self.handle_event(ReplicationEvent::Disconnected { reason })
    }
    
    /// Called when state changes
    fn on_state_changed(&mut self, from: ReplicationState, to: ReplicationState) -> Result<()> {
        self.handle_event(ReplicationEvent::StateChanged { from, to })
    }
    
    /// Called when RDB data is received
    fn on_rdb_data(
        &mut self,
        data: Vec<u8>,
        offset: u64,
        total_size: Option<u64>,
    ) -> Result<()> {
        self.handle_event(ReplicationEvent::RdbData {
            data,
            offset,
            total_size,
        })
    }
    
    /// Called when a command is received
    fn on_command(&mut self, entry: ReplicationEntry, offset: i64) -> Result<()> {
        self.handle_event(ReplicationEvent::Command { entry, offset })
    }
    
    /// Called when an error occurs
    fn on_error(&mut self, error: RedisReplicatorError, recoverable: bool) -> Result<()> {
        self.handle_event(ReplicationEvent::Error { error, recoverable })
    }
    
    /// Called on heartbeat
    fn on_heartbeat(&mut self, timestamp: SystemTime) -> Result<()> {
        self.handle_event(ReplicationEvent::Heartbeat { timestamp })
    }
    
    /// Called when full sync completes
    fn on_full_sync_completed(&mut self, rdb_size: u64, duration: Duration) -> Result<()> {
        self.handle_event(ReplicationEvent::FullSyncCompleted { rdb_size, duration })
    }
}

/// A simple event handler that collects all entries
#[derive(Debug, Default)]
pub struct CollectingEventHandler {
    /// Collected replication entries
    pub entries: Vec<Entry>,
    /// RDB data
    pub rdb_data: Vec<u8>,
    /// Collected replication events
    pub events: Vec<ReplicationEvent>,
    /// Replication information
    pub info: Option<ReplicationInfo>,
}

impl ReplicationEventHandler for CollectingEventHandler {
    fn handle_event(&mut self, event: ReplicationEvent) -> Result<()> {
        match &event {
            ReplicationEvent::Connected { info, .. } => {
                self.info = Some(info.clone());
            }
            ReplicationEvent::RdbData { data, .. } => {
                self.rdb_data.extend_from_slice(data);
            }
            ReplicationEvent::Command { entry, .. } => {
                self.entries.push(Entry::Replication(entry.clone()));
            }
            _ => {}
        }
        self.events.push(event);
        Ok(())
    }
}

/// An event handler that filters entries based on criteria
#[derive(Debug)]
pub struct FilteringEventHandler<F> {
    /// Filtered replication entries
    pub entries: Vec<Entry>,
    /// RDB data
    pub rdb_data: Vec<u8>,
    /// Filter function
    pub filter: F,
    /// Replication information
    pub info: Option<ReplicationInfo>,
}

impl<F> FilteringEventHandler<F>
where
    F: Fn(&ReplicationEntry) -> bool,
{
    /// Create a new filtering event handler with the given filter function
    pub fn new(filter: F) -> Self {
        Self {
            entries: Vec::new(),
            rdb_data: Vec::new(),
            filter,
            info: None,
        }
    }
}

impl<F> ReplicationEventHandler for FilteringEventHandler<F>
where
    F: Fn(&ReplicationEntry) -> bool + Send + Sync,
{
    fn handle_event(&mut self, event: ReplicationEvent) -> Result<()> {
        match event {
            ReplicationEvent::Connected { info, .. } => {
                self.info = Some(info);
            }
            ReplicationEvent::RdbData { data, .. } => {
                self.rdb_data.extend_from_slice(&data);
            }
            ReplicationEvent::Command { entry, .. } => {
                if (self.filter)(&entry) {
                    self.entries.push(Entry::Replication(entry));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Utility functions for replication
pub mod utils {
    use super::*;
    use std::net::ToSocketAddrs;
    
    /// Parse Redis master address
    pub fn parse_master_addr(addr: &str) -> Result<SocketAddr> {
        addr.to_socket_addrs()
            .map_err(|e| {
                RedisReplicatorError::connection_error(format!(
                    "Invalid master address '{}': {}",
                    addr, e
                ))
            })?
            .next()
            .ok_or_else(|| {
                RedisReplicatorError::connection_error(format!(
                    "Could not resolve master address: {}",
                    addr
                ))
            })
    }
    
    /// Create a TCP connection with timeout
    pub async fn connect_with_timeout(
        addr: SocketAddr,
        timeout_duration: Duration,
    ) -> Result<TcpStream> {
        timeout(timeout_duration, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                RedisReplicatorError::connection_error(format!(
                    "Connection timeout to {}",
                    addr
                ))
            })?
            .map_err(|e| {
                RedisReplicatorError::connection_error(format!(
                    "Failed to connect to {}: {}",
                    addr, e
                ))
            })
    }
    
    /// Generate a random replication ID
    pub fn generate_repl_id() -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        SystemTime::now().hash(&mut hasher);
        std::process::id().hash(&mut hasher);
        
        format!("{:016x}", hasher.finish())
    }
    
    /// Check if a replication offset is valid
    pub fn is_valid_offset(offset: i64) -> bool {
        offset >= -1
    }
    
    /// Calculate lag between offsets
    pub fn calculate_lag(master_offset: i64, replica_offset: i64) -> Option<i64> {
        if is_valid_offset(master_offset) && is_valid_offset(replica_offset) {
            Some(master_offset - replica_offset)
        } else {
            None
        }
    }
    
    /// Format replication info as string
    pub fn format_replication_info(info: &ReplicationInfo) -> String {
        format!(
            "master={}:{} id={} offset={} state={} uptime={:?}",
            info.master_addr.ip(),
            info.master_addr.port(),
            info.repl_id,
            info.repl_offset,
            info.state,
            info.uptime
        )
    }
    
    /// Check if replication state is connected
    pub fn is_connected_state(state: ReplicationState) -> bool {
        matches!(
            state,
            ReplicationState::Connected
                | ReplicationState::FullSync
                | ReplicationState::IncrementalSync
        )
    }
    
    /// Check if replication state indicates an error
    pub fn is_error_state(state: ReplicationState) -> bool {
        matches!(state, ReplicationState::Error | ReplicationState::Disconnected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_state_display() {
        assert_eq!(ReplicationState::Disconnected.to_string(), "disconnected");
        assert_eq!(ReplicationState::Connected.to_string(), "connected");
        assert_eq!(ReplicationState::Error.to_string(), "error");
    }

    #[test]
    fn test_replication_stats() {
        let mut stats = ReplicationStats::new();
        assert!(stats.start_time.is_some());
        
        stats.record_connection_attempt();
        assert_eq!(stats.connection_attempts, 1);
        
        stats.record_successful_connection();
        assert_eq!(stats.successful_connections, 1);
        assert_eq!(stats.connection_success_rate(), 1.0);
        
        stats.record_data(100, true);
        assert_eq!(stats.bytes_received, 100);
        assert_eq!(stats.rdb_bytes_received, 100);
        
        stats.record_command(false);
        assert_eq!(stats.commands_received, 1);
        assert_eq!(stats.aof_commands_received, 0);
    }

    #[test]
    fn test_collecting_event_handler() {
        let mut handler = CollectingEventHandler::default();
        
        let info = ReplicationInfo::default();
        handler
            .on_connected("127.0.0.1:6379".parse().unwrap(), info.clone())
            .unwrap();
        
        assert!(handler.info.is_some());
        assert_eq!(handler.events.len(), 1);
        
        handler.on_rdb_data(vec![1, 2, 3], 0, None).unwrap();
        assert_eq!(handler.rdb_data, vec![1, 2, 3]);
    }

    #[test]
    fn test_filtering_event_handler() {
        let mut handler = FilteringEventHandler::new(|entry: &ReplicationEntry| {
            match entry {
                ReplicationEntry::Command { args, .. } => args.get(0).map_or(false, |cmd| cmd == "SET"),
                _ => false,
            }
        });
        
        let set_entry = ReplicationEntry::Command {
            args: vec!["SET".to_string(), "key".to_string(), "value".to_string()],
            timestamp: None,
        };
        
        let get_entry = ReplicationEntry::Command {
            args: vec!["GET".to_string(), "key".to_string()],
            timestamp: None,
        };
        
        handler.on_command(set_entry, 0).unwrap();
        handler.on_command(get_entry, 1).unwrap();
        
        assert_eq!(handler.entries.len(), 1);
        match &handler.entries[0] {
            Entry::Replication(ReplicationEntry::Command { args, .. }) => {
                assert_eq!(args[0], "SET");
            }
            _ => panic!("Expected replication command entry"),
        }
    }

    #[test]
    fn test_utils() {
        assert!(utils::parse_master_addr("127.0.0.1:6379").is_ok());
        assert!(utils::parse_master_addr("invalid").is_err());
        
        let repl_id = utils::generate_repl_id();
        assert_eq!(repl_id.len(), 16);
        
        assert!(utils::is_valid_offset(0));
        assert!(utils::is_valid_offset(-1));
        assert!(!utils::is_valid_offset(-2));
        
        assert_eq!(utils::calculate_lag(100, 90), Some(10));
        assert_eq!(utils::calculate_lag(-1, -1), Some(0));
        
        assert!(utils::is_connected_state(ReplicationState::Connected));
        assert!(!utils::is_connected_state(ReplicationState::Disconnected));
        
        assert!(utils::is_error_state(ReplicationState::Error));
        assert!(!utils::is_error_state(ReplicationState::Connected));
    }

    #[test]
    fn test_replication_info_default() {
        let info = ReplicationInfo::default();
        assert_eq!(info.repl_offset, -1);
        assert_eq!(info.state, ReplicationState::Disconnected);
        assert!(info.repl_id.is_empty());
    }
}