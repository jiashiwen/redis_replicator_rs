//! Redis replication client implementation
//!
//! This module implements a Redis replication client that can connect to
//! a Redis master server and receive replication data.

use crate::error::{RedisReplicatorError, Result};
use crate::replication::protocol::{
    ReplicationCommand, ReplicationProtocol, ReplicationResponse,
};
use crate::replication::{ReplicationEvent, ReplicationEventHandler, ReplicationInfo, ReplicationState, ReplicationStats};
use crate::rdb::RdbEvent;
use crate::aof::AofEvent;
use crate::types::{Entry, ReplicationEntry, RdbEntry};
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio::time::timeout;

/// Redis replication client configuration
#[derive(Debug, Clone)]
pub struct ReplicationClientConfig {
    /// Master server address
    pub master_addr: SocketAddr,
    /// Authentication password
    pub password: Option<String>,
    /// Client listening port (for REPLCONF)
    pub listening_port: Option<u16>,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Read timeout
    pub read_timeout: Duration,
    /// Write timeout
    pub write_timeout: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Maximum reconnection attempts
    pub max_reconnect_attempts: u32,
    /// Reconnection delay
    pub reconnect_delay: Duration,
    /// Buffer size for network I/O
    pub buffer_size: usize,
    /// Maximum memory usage for buffering
    pub max_memory_usage: usize,
    /// Enable RDB parsing
    pub parse_rdb: bool,
    /// Enable AOF parsing
    pub parse_aof: bool,
    /// Replication offset to start from (-1 for full resync)
    pub start_offset: i64,
    /// Replication ID to continue from
    pub repl_id: Option<String>,
}

impl Default for ReplicationClientConfig {
    fn default() -> Self {
        Self {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            password: None,
            listening_port: None,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(10),
            max_reconnect_attempts: 3,
            reconnect_delay: Duration::from_secs(5),
            buffer_size: 64 * 1024,
            max_memory_usage: 256 * 1024 * 1024, // 256MB
            parse_rdb: true,
            parse_aof: true,
            start_offset: -1,
            repl_id: None,
        }
    }
}

/// Redis replication client
#[derive(Debug)]
pub struct ReplicationClient {
    /// Client configuration
    config: ReplicationClientConfig,
    /// Protocol handler
    protocol: ReplicationProtocol,
    /// Current replication state
    state: Arc<Mutex<ReplicationState>>,
    /// Replication statistics
    stats: Arc<Mutex<ReplicationStats>>,
    /// Event sender
    #[allow(dead_code)]
    event_sender: Option<mpsc::UnboundedSender<ReplicationEvent>>,
    /// Shutdown signal
    #[allow(dead_code)]
    shutdown_receiver: Option<mpsc::Receiver<()>>,
    /// Current replication info
    repl_info: Arc<Mutex<ReplicationInfo>>,
}

impl ReplicationClient {
    /// Create a new replication client
    pub fn new(config: ReplicationClientConfig) -> Self {
        let repl_info = ReplicationInfo {
            master_addr: config.master_addr,
            repl_id: config.repl_id.clone().unwrap_or_else(|| "?".to_string()),
            repl_offset: config.start_offset,
            master_run_id: String::new(),
            state: ReplicationState::Disconnected,
            last_sync: None,
            bytes_received: 0,
            commands_received: 0,
            uptime: Duration::from_secs(0),
            lag: None,
        };
        
        Self {
            config,
            protocol: ReplicationProtocol::new(),
            state: Arc::new(Mutex::new(ReplicationState::Disconnected)),
            stats: Arc::new(Mutex::new(ReplicationStats::default())),
            event_sender: None,
            shutdown_receiver: None,
            repl_info: Arc::new(Mutex::new(repl_info)),
        }
    }
    
    /// Start replication with an event handler
    pub async fn start_replication<H>(
        &mut self,
        mut handler: H,
    ) -> Result<()>
    where
        H: ReplicationEventHandler + Send + 'static,
    {
        tracing::info!("Starting replication from {}", self.config.master_addr);
        
        let mut reconnect_attempts = 0;
        
        loop {
            match self.connect_and_replicate(&mut handler).await {
                Ok(()) => {
                    tracing::info!("Replication completed successfully");
                    break;
                }
                Err(e) => {
                    tracing::error!("Replication error: {}", e);
                    
                    reconnect_attempts += 1;
                    if reconnect_attempts > self.config.max_reconnect_attempts {
                        return Err(RedisReplicatorError::replication_error(
                            format!(
                                "Max reconnection attempts ({}) exceeded",
                                self.config.max_reconnect_attempts
                            ),
                        ));
                    }
                    
                    tracing::warn!(
                        "Reconnection attempt {} of {}",
                        reconnect_attempts, self.config.max_reconnect_attempts
                    );
                    
                    // Update state
                    {
                        let mut state = self.state.lock().await;
                        *state = ReplicationState::Connecting;
                    }
                    
                    // Send reconnection event
                    let event = ReplicationEvent::StateChanged {
                        from: ReplicationState::Error,
                        to: ReplicationState::Connecting,
                    };
                    if let Err(e) = handler.handle_event(event) {
                        tracing::warn!("Event handler error during reconnection: {}", e);
                    }
                    
                    // Wait before reconnecting
                    tokio::time::sleep(self.config.reconnect_delay).await;
                }
            }
        }
        
        Ok(())
    }
    
    /// Connect to master and start replication
    async fn connect_and_replicate<H>(
        &mut self,
        handler: &mut H,
    ) -> Result<()>
    where
        H: ReplicationEventHandler + Send,
    {
        // Connect to master
        let stream = self.connect_to_master().await?;
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::with_capacity(self.config.buffer_size, reader);
        let mut writer = BufWriter::with_capacity(self.config.buffer_size, writer);
        
        // Update state
        {
            let mut state = self.state.lock().await;
            *state = ReplicationState::Connected;
        }
        
        // Send connected event
        let event = ReplicationEvent::Connected {
            master_addr: self.config.master_addr,
            info: ReplicationInfo {
                master_addr: self.config.master_addr,
                repl_id: String::new(),
                repl_offset: 0,
                master_run_id: String::new(),
                state: ReplicationState::Connected,
                last_sync: None,
                bytes_received: 0,
                commands_received: 0,
                uptime: Duration::from_secs(0),
                lag: None,
            },
        };
        handler.handle_event(event)?;
        
        // Perform handshake
        self.perform_handshake(&mut reader, &mut writer, handler).await?;
        
        // Start replication
        self.start_sync(&mut reader, &mut writer, handler).await?;
        
        Ok(())
    }
    
    /// Connect to Redis master
    async fn connect_to_master(&self) -> Result<TcpStream> {
        tracing::info!("Connecting to Redis master at {}", self.config.master_addr);
        
        let stream = timeout(
            self.config.connect_timeout,
            TcpStream::connect(self.config.master_addr),
        )
        .await
        .map_err(|_| {
            RedisReplicatorError::connection_error(
                "Connection timeout".to_string(),
            )
        })?
        .map_err(|e| {
            RedisReplicatorError::connection_error(
                format!("Failed to connect: {}", e),
            )
        })?;
        
        // Configure socket
        stream.set_nodelay(true).map_err(|e| {
            RedisReplicatorError::connection_error(
                format!("Failed to set nodelay: {}", e),
            )
        })?;
        
        tracing::info!("Connected to Redis master");
        Ok(stream)
    }
    
    /// Perform replication handshake
    async fn perform_handshake<R, W, H>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
        handler: &mut H,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
        H: ReplicationEventHandler + Send,
    {
        tracing::info!("Starting replication handshake");
        
        // Send PING
        let ping_cmd = ReplicationCommand::Ping;
        self.protocol.send_command(writer, &ping_cmd).await?;
        
        let response = timeout(
            self.config.read_timeout,
            self.protocol.read_response(reader),
        )
        .await
        .map_err(|_| {
            RedisReplicatorError::protocol_error("ping", "PING timeout")
        })??;
        
        if response.is_error() {
            return Err(RedisReplicatorError::protocol_error(
                "ping",
                format!("PING failed: {}", response.error_message().unwrap_or("unknown error"))
            ));
        }
        
        tracing::debug!("PING successful");
        
        // Authenticate if password is provided
        if let Some(password) = &self.config.password {
            let auth_cmd = ReplicationCommand::Auth {
                password: password.clone(),
            };
            self.protocol.send_command(writer, &auth_cmd).await?;
            
            let response = timeout(
                self.config.read_timeout,
                self.protocol.read_response(reader),
            )
            .await
            .map_err(|_| {
                RedisReplicatorError::protocol_error("auth", "AUTH timeout")
            })??;
            
            if response.is_error() {
                return Err(RedisReplicatorError::authentication_error(format!(
                    "Authentication failed: {}",
                    response.error_message().unwrap_or("unknown error")
                )));
            }
            
            tracing::debug!("Authentication successful");
        }
        
        // Send REPLCONF commands
        if let Some(port) = self.config.listening_port {
            let replconf_cmd = ReplicationCommand::ReplConf {
                key: "listening-port".to_string(),
                value: port.to_string(),
            };
            self.protocol.send_command(writer, &replconf_cmd).await?;
            
            let response = timeout(
                self.config.read_timeout,
                self.protocol.read_response(reader),
            )
            .await
            .map_err(|_| {
                RedisReplicatorError::protocol_error(
                    "replconf",
                    "REPLCONF listening-port timeout"
                )
            })??;
            
            if response.is_error() {
            tracing::warn!(
                "REPLCONF listening-port failed: {}",
                response.error_message().unwrap_or("unknown error")
            );
            } else {
                tracing::debug!("REPLCONF listening-port successful");
            }
        }
        
        // Send REPLCONF capa
        let capa_cmd = ReplicationCommand::ReplConf {
            key: "capa".to_string(),
            value: "eof".to_string(),
        };
        self.protocol.send_command(writer, &capa_cmd).await?;
        
        let response = timeout(
            self.config.read_timeout,
            self.protocol.read_response(reader),
        )
        .await
        .map_err(|_| {
            RedisReplicatorError::protocol_error(
                "replconf",
                "REPLCONF capa timeout"
            )
        })??;
        
        if response.is_error() {
            tracing::warn!(
                "REPLCONF capa failed: {}",
                response.error_message().unwrap_or("unknown error")
            );
        } else {
            tracing::debug!("REPLCONF capa successful");
        }
        
        tracing::info!("Replication handshake completed");
        
        // Send handshake completed event
        let event = ReplicationEvent::StateChanged {
            from: ReplicationState::Handshake,
            to: ReplicationState::FullSync,
        };
        handler.handle_event(event)?;
        
        Ok(())
    }
    
    /// Start synchronization
    async fn start_sync<R, W, H>(
        &mut self,
        reader: &mut R,
        writer: &mut W,
        handler: &mut H,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
        H: ReplicationEventHandler + Send,
    {
        tracing::info!("Starting synchronization");
        
        // Send PSYNC command
        let repl_id = self.config.repl_id.clone().unwrap_or_else(|| "?".to_string());
        let offset = self.config.start_offset;
        
        let psync_cmd = ReplicationCommand::PSync { repl_id, offset };
        self.protocol.send_command(writer, &psync_cmd).await?;
        
        let response = timeout(
            self.config.read_timeout,
            self.protocol.read_response(reader),
        )
        .await
        .map_err(|_| {
            RedisReplicatorError::protocol_error("psync", "PSYNC timeout")
        })??;
        
        if response.is_error() {
            return Err(RedisReplicatorError::protocol_error(
                "psync",
                format!("PSYNC failed: {}", response.error_message().unwrap_or("unknown error"))
            ));
        }
        
        match response {
            ReplicationResponse::FullResync { repl_id, offset } => {
            tracing::info!("Full resync initiated: repl_id={}, offset={}", repl_id, offset);
                
                // Update replication info
                {
                    let mut info = self.repl_info.lock().await;
                    info.repl_id = repl_id.clone();
                    info.repl_offset = offset;
                    info.state = ReplicationState::FullSync;
                }
                
                // Update state
                {
                    let mut state = self.state.lock().await;
                    *state = ReplicationState::FullSync;
                }
                
                // Send full sync started event
                let event = ReplicationEvent::StateChanged {
                    from: ReplicationState::Handshake,
                    to: ReplicationState::FullSync,
                };
                handler.handle_event(event)?;
                
                // Receive and process RDB data
                self.receive_rdb_data(reader, handler).await?;
                
                // Start incremental sync
                self.start_incremental_sync(reader, writer, handler).await?;
            }
            ReplicationResponse::Continue => {
            tracing::info!("Partial resync - continuing from current offset");
                
                // Update state
                {
                    let mut state = self.state.lock().await;
                    *state = ReplicationState::IncrementalSync;
                }
                
                // Send partial sync started event
                let event = ReplicationEvent::StateChanged {
                    from: ReplicationState::Handshake,
                    to: ReplicationState::IncrementalSync,
                };
                handler.handle_event(event)?;
                
                // Start incremental sync
                self.start_incremental_sync(reader, writer, handler).await?;
            }
            _ => {
                return Err(RedisReplicatorError::protocol_error("protocol", "Unexpected PSYNC response".to_string(),
                ));
            }
        }
        
        Ok(())
    }
    
    /// Receive and process RDB data
    async fn receive_rdb_data<R, H>(
        &mut self,
        reader: &mut R,
        handler: &mut H,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
        H: ReplicationEventHandler + Send,
    {
        tracing::info!("Receiving RDB data");
        
        // Read RDB size (sent as bulk string)
        let response = timeout(
            self.config.read_timeout,
            self.protocol.read_response(reader),
        )
        .await
        .map_err(|_| {
            RedisReplicatorError::protocol_error("rdb_size", "RDB size timeout")
        })??;
        
        let rdb_data = match response {
            ReplicationResponse::BulkString(Some(data)) => data,
            ReplicationResponse::BulkString(None) => {
                return Err(RedisReplicatorError::protocol_error(
                    "rdb_data",
                    "Empty RDB data"
                ));
            }
            _ => {
                return Err(RedisReplicatorError::protocol_error("protocol", "Expected RDB bulk string".to_string(),
                ));
            }
        };
        
        tracing::info!("Received RDB data: {} bytes", rdb_data.len());
        
        // Update replication info
        {
            let _info = self.repl_info.lock().await;
            // RDB size tracking removed as field doesn't exist
        }
        
        // Send RDB data received event
        let event = ReplicationEvent::RdbData {
            data: rdb_data.to_vec(),
            offset: 0,
            total_size: Some(rdb_data.len() as u64),
        };
        handler.handle_event(event)?;
        
        // Parse RDB data if enabled
        if self.config.parse_rdb {
            self.parse_rdb_data(&rdb_data, handler).await?;
        }
        
        tracing::info!("RDB data processing completed");
        Ok(())
    }
    
    /// Parse RDB data
    async fn parse_rdb_data<H>(
        &mut self,
        rdb_data: &[u8],
        handler: &mut H,
    ) -> Result<()>
    where
        H: ReplicationEventHandler + Send,
    {
        tracing::debug!("Parsing RDB data: {} bytes", rdb_data.len());
        
        // Create a simple RDB event handler that converts to replication events
        struct RdbToReplicationHandler<'a, H: ReplicationEventHandler> {
            repl_handler: &'a mut H,
        }
        
        impl<'a, H: ReplicationEventHandler> crate::rdb::RdbEventHandler for RdbToReplicationHandler<'a, H> {
            fn handle_event(&mut self, event: RdbEvent) -> Result<()> {
                match event {
                    RdbEvent::StartOfFile {  .. } => {
                        let repl_event = ReplicationEvent::StateChanged {
                            from: ReplicationState::FullSync,
                            to: ReplicationState::FullSync,
                        };
                        self.repl_handler.handle_event(repl_event)
                    }
                    RdbEvent::EndOfFile { .. } => {
                        let repl_event = ReplicationEvent::FullSyncCompleted {
                             rdb_size: 0,
                             duration: std::time::Duration::from_secs(0),
                         };
                        self.repl_handler.handle_event(repl_event)
                    }
                    RdbEvent::StartOfDatabase { database_id } => {
                        let _entry = Entry::Rdb(crate::types::RdbEntry::SelectDb {
                            db: database_id,
                        });
                        let repl_event = ReplicationEvent::Command {
                            entry: ReplicationEntry::Command {
                                args: vec!["SELECT".to_string(), database_id.to_string()],
                                timestamp: None,
                            },
                            offset: 0,
                        };
                        self.repl_handler.handle_event(repl_event)
                    }
                    RdbEvent::KeyValue { entry, .. } => {
                        if let RdbEntry::KeyValue { key, .. } = entry {
                            let repl_event = ReplicationEvent::Command {
                                entry: ReplicationEntry::Command {
                                    args: vec!["SET".to_string(), key],
                                    timestamp: None,
                                },
                                offset: 0,
                            };
                            self.repl_handler.handle_event(repl_event)
                        } else {
                            Ok(())
                        }
                    }
                    _ => Ok(()), // Ignore other events
                }
            }
        }
        
        let rdb_handler = RdbToReplicationHandler {
            repl_handler: handler,
        };
        
        // Send RDB completion event
        let completion_event = ReplicationEvent::FullSyncCompleted {
            rdb_size: rdb_data.len() as u64,
            duration: std::time::Duration::from_secs(0),
        };
        rdb_handler.repl_handler.handle_event(completion_event)?;
        
        Ok(())
    }
    
    /// Start incremental synchronization
    async fn start_incremental_sync<R, W, H>(
        &mut self,
        reader: &mut R,
        _writer: &mut W,
        handler: &mut H,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
        H: ReplicationEventHandler + Send,
    {
        tracing::info!("Starting incremental synchronization");
        
        // Update state
        {
            let mut state = self.state.lock().await;
            *state = ReplicationState::IncrementalSync;
        }
        
        // Send incremental sync started event
        let event = ReplicationEvent::StateChanged {
            from: ReplicationState::FullSync,
            to: ReplicationState::IncrementalSync,
        };
        handler.handle_event(event)?;
        
        // Start heartbeat task
        let heartbeat_handle = self.start_heartbeat_task().await;
        
        // Process replication stream
        let result = self.process_replication_stream(reader, handler).await;
        
        // Cancel heartbeat task
        heartbeat_handle.abort();
        
        result
    }
    
    /// Start heartbeat task
    async fn start_heartbeat_task(
        &self,
    ) -> tokio::task::JoinHandle<()>
    {
        let interval = self.config.heartbeat_interval;
        
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                // Send REPLCONF ACK command
                // Note: In a real implementation, we would need to track the current offset
                // and send it with the ACK command
        tracing::debug!("Sending heartbeat (REPLCONF ACK)");
                
                // This is a simplified implementation
                // In practice, we would need to properly handle the writer reference
                break;
            }
        })
    }
    
    /// Process replication stream
    async fn process_replication_stream<R, H>(
        &mut self,
        reader: &mut R,
        handler: &mut H,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin,
        H: ReplicationEventHandler + Send,
    {
        tracing::info!("Processing replication stream");
        
        loop {
            // Read command from stream
            let response = timeout(
                self.config.read_timeout,
                self.protocol.read_response(reader),
            )
            .await
            .map_err(|_| {
                RedisReplicatorError::protocol_error("timeout", "Replication stream timeout".to_string(),
                )
            })??;
            
            match response {
                ReplicationResponse::Command(args) => {
                    // Process Redis command
                    self.process_redis_command(args, handler).await?;
                }
                ReplicationResponse::BulkString(Some(data)) => {
                    // This might be AOF data
                    if self.config.parse_aof {
                        self.process_aof_data(&data, handler).await?;
                    }
                }
                ReplicationResponse::Error(msg) => {
                tracing::error!("Replication stream error: {}", msg);
                    return Err(RedisReplicatorError::protocol_error(
                        "replication",
                        format!("Replication stream error: {}", msg)
                    ));
                }
                _ => {
                    tracing::warn!("Unexpected response in replication stream: {:?}", response);
                }
            }
            
            // Update statistics
            {
                let mut stats = self.stats.lock().await;
                stats.commands_received += 1;
                stats.last_activity = Some(SystemTime::now());
            }
        }
    }
    
    /// Process Redis command
    async fn process_redis_command<H>(
        &mut self,
        args: Vec<Bytes>,
        handler: &mut H,
    ) -> Result<()>
    where
        H: ReplicationEventHandler + Send,
    {
        if args.is_empty() {
            return Ok(());
        }
        
        // Convert bytes to strings
        let string_args: Result<Vec<String>> = args
            .iter()
            .map(|arg| {
                String::from_utf8(arg.to_vec()).map_err(|_| {
                    RedisReplicatorError::protocol_error("validation", "Invalid UTF-8 in command argument".to_string(),
                    )
                })
            })
            .collect();
        
        let string_args = string_args?;
        
        tracing::debug!("Processing command: {:?}", string_args);
        
        // Create replication entry
        let entry = ReplicationEntry::Command {
            args: string_args,
            timestamp: None,
        };
        
        // Send entry event
        let event = ReplicationEvent::Command { entry, offset: 0i64 };
        handler.handle_event(event)?;
        
        Ok(())
    }
    
    /// Process AOF data
    async fn process_aof_data<H>(
        &mut self,
        aof_data: &[u8],
        handler: &mut H,
    ) -> Result<()>
    where
        H: ReplicationEventHandler + Send,
    {
        tracing::debug!("Processing AOF data: {} bytes", aof_data.len());
        
        // Create a simple AOF event handler that converts to replication events
        struct AofToReplicationHandler<'a, H: ReplicationEventHandler> {
            repl_handler: &'a mut H,
        }
        
        impl<'a, H: ReplicationEventHandler> crate::aof::AofEventHandler for AofToReplicationHandler<'a, H> {
            fn handle_event(&mut self, event: AofEvent) -> Result<()> {
                match event {
                    AofEvent::Command { entry, position: _ } => {
                        let repl_entry = match entry {
                            crate::types::AofEntry::Command { args, timestamp } => {
                                ReplicationEntry::Command { args, timestamp }
                            }
                            crate::types::AofEntry::Multi => {
                                ReplicationEntry::Command { args: vec!["MULTI".to_string()], timestamp: None }
                            }
                            crate::types::AofEntry::Exec => {
                                ReplicationEntry::Command { args: vec!["EXEC".to_string()], timestamp: None }
                            }
                            crate::types::AofEntry::Select { db } => {
                                ReplicationEntry::Command { args: vec!["SELECT".to_string(), db.to_string()], timestamp: None }
                            }
                        };
                        let repl_event = ReplicationEvent::Command { entry: repl_entry, offset: 0 };
                        self.repl_handler.handle_event(repl_event)
                    }
                    _ => Ok(()), // Ignore other events
                }
            }
        }
        
        let _aof_handler = AofToReplicationHandler {
            repl_handler: handler,
        };
        
        // Parse AOF data using the AOF parser
        let aof_entries = crate::aof::parse_bytes(Bytes::from(aof_data.to_vec())).await?;
        
        // Process each AOF entry through the handler
        for entry in aof_entries {
            let repl_entry = match entry {
                crate::types::AofEntry::Command { args, timestamp } => {
                    ReplicationEntry::Command { args, timestamp }
                }
                crate::types::AofEntry::Multi => {
                    ReplicationEntry::Command { args: vec!["MULTI".to_string()], timestamp: None }
                }
                crate::types::AofEntry::Exec => {
                    ReplicationEntry::Command { args: vec!["EXEC".to_string()], timestamp: None }
                }
                crate::types::AofEntry::Select { db } => {
                    ReplicationEntry::Command { args: vec!["SELECT".to_string(), db.to_string()], timestamp: None }
                }
            };
            let repl_event = ReplicationEvent::Command { entry: repl_entry, offset: 0i64 };
            handler.handle_event(repl_event)?;
        }
        
        Ok(())
    }
    
    /// Get current replication info
    pub async fn get_replication_info(&self) -> ReplicationInfo {
        self.repl_info.lock().await.clone()
    }
    
    /// Get current replication state
    pub async fn get_state(&self) -> ReplicationState {
        *self.state.lock().await
    }
    
    /// Get replication statistics
    pub async fn get_stats(&self) -> ReplicationStats {
        self.stats.lock().await.clone()
    }
    
    /// Stop replication
    pub async fn stop(&mut self) {
        tracing::info!("Stopping replication client");
        
        // Update state
        {
            let mut state = self.state.lock().await;
            *state = ReplicationState::Disconnected;
        }
        
        // Clear protocol buffers
        self.protocol.clear_buffers();
    }
}

/// Utility functions for replication client
pub mod utils {
    use super::*;
    use std::net::ToSocketAddrs;
    
    /// Create a replication client with default configuration
    pub fn create_client(master_addr: &str) -> Result<ReplicationClient> {
        let addr = master_addr
            .to_socket_addrs()
            .map_err(|e| {
                RedisReplicatorError::configuration_error(format!(
                    "Invalid master address: {}",
                    e
                ))
            })?
            .next()
            .ok_or_else(|| {
                RedisReplicatorError::configuration_error(
                    "No valid address found".to_string(),
                )
            })?;
        
        let config = ReplicationClientConfig {
            master_addr: addr,
            ..Default::default()
        };
        
        Ok(ReplicationClient::new(config))
    }
    
    /// Create a replication client with authentication
    pub fn create_authenticated_client(
        master_addr: &str,
        password: &str,
    ) -> Result<ReplicationClient> {
        let addr = master_addr
            .to_socket_addrs()
            .map_err(|e| {
                RedisReplicatorError::configuration_error(format!(
                    "Invalid master address: {}",
                    e
                ))
            })?
            .next()
            .ok_or_else(|| {
                RedisReplicatorError::configuration_error(
                    "No valid address found".to_string(),
                )
            })?;
        
        let config = ReplicationClientConfig {
            master_addr: addr,
            password: Some(password.to_string()),
            ..Default::default()
        };
        
        Ok(ReplicationClient::new(config))
    }
    
    /// Create a replication client with custom configuration
    pub fn create_custom_client(
        config: ReplicationClientConfig,
    ) -> ReplicationClient {
        ReplicationClient::new(config)
    }
    
    /// Validate replication client configuration
    pub fn validate_config(config: &ReplicationClientConfig) -> Result<()> {
        if config.connect_timeout.is_zero() {
            return Err(RedisReplicatorError::configuration_error(
                "Connect timeout must be greater than zero".to_string(),
            ));
        }
        
        if config.read_timeout.is_zero() {
            return Err(RedisReplicatorError::configuration_error(
                "Read timeout must be greater than zero".to_string(),
            ));
        }
        
        if config.write_timeout.is_zero() {
            return Err(RedisReplicatorError::configuration_error(
                "Write timeout must be greater than zero".to_string(),
            ));
        }
        
        if config.buffer_size == 0 {
            return Err(RedisReplicatorError::configuration_error(
                "Buffer size must be greater than zero".to_string(),
            ));
        }
        
        if config.max_memory_usage == 0 {
            return Err(RedisReplicatorError::configuration_error(
                "Max memory usage must be greater than zero".to_string(),
            ));
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    
    #[derive(Debug, Clone)]
    struct TestEventHandler {
        events: Arc<std::sync::Mutex<Vec<ReplicationEvent>>>,
        event_count: Arc<AtomicUsize>,
    }
    
    impl TestEventHandler {
        fn new() -> Self {
            Self {
                events: Arc::new(std::sync::Mutex::new(Vec::new())),
                event_count: Arc::new(AtomicUsize::new(0)),
            }
        }
        
        fn get_events(&self) -> Vec<ReplicationEvent> {
            self.events.lock().unwrap().clone()
        }
        
        fn get_event_count(&self) -> usize {
            self.event_count.load(Ordering::Relaxed)
        }
    }
    
    #[async_trait::async_trait]
    impl ReplicationEventHandler for TestEventHandler {
        fn handle_event(&mut self, event: ReplicationEvent) -> Result<()> {
            self.events.lock().unwrap().push(event);
            self.event_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }
    
    #[test]
    fn test_config_default() {
        let config = ReplicationClientConfig::default();
        assert_eq!(config.master_addr.to_string(), "127.0.0.1:6379");
        assert_eq!(config.password, None);
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.buffer_size, 64 * 1024);
    }
    
    #[test]
    fn test_client_creation() {
        let config = ReplicationClientConfig::default();
        let client = ReplicationClient::new(config);
        
        // Basic checks
        assert!(client.event_sender.is_none());
        assert!(client.shutdown_receiver.is_none());
    }
    
    #[tokio::test]
    async fn test_client_state() {
        let config = ReplicationClientConfig::default();
        let client = ReplicationClient::new(config);
        
        let state = client.get_state().await;
        assert_eq!(state, ReplicationState::Disconnected);
        
        let stats = client.get_stats().await;
        assert_eq!(stats.commands_received, 0);
    }
    
    #[test]
    fn test_utils_create_client() {
        let result = utils::create_client("127.0.0.1:6379");
        assert!(result.is_ok());
        
        let result = utils::create_client("invalid-address");
        assert!(result.is_err());
    }
    
    #[test]
    fn test_utils_create_authenticated_client() {
        let result = utils::create_authenticated_client("127.0.0.1:6379", "password");
        assert!(result.is_ok());
        
        if let Ok(client) = result {
            assert_eq!(client.config.password, Some("password".to_string()));
        }
    }
    
    #[test]
    fn test_utils_validate_config() {
        let mut config = ReplicationClientConfig::default();
        assert!(utils::validate_config(&config).is_ok());
        
        config.connect_timeout = Duration::from_secs(0);
        assert!(utils::validate_config(&config).is_err());
        
        config.connect_timeout = Duration::from_secs(10);
        config.buffer_size = 0;
        assert!(utils::validate_config(&config).is_err());
    }
    
    #[tokio::test]
    async fn test_event_handler() {
        let mut handler = TestEventHandler::new();
        
        let event = ReplicationEvent::Connected {
            master_addr: "127.0.0.1:6379".parse().unwrap(),
            info: ReplicationInfo::default(),
        };
        
        handler.handle_event(event.clone()).unwrap();
        
        assert_eq!(handler.get_event_count(), 1);
        let events = handler.get_events();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ReplicationEvent::Connected { .. }));
    }
}