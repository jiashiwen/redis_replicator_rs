//! Example: Redis Replication Client
//!
//! This example demonstrates how to use the redis_replicator_rs library to replicate data from a Redis master.
//! It shows different replication scenarios including full sync, incremental sync, and real-time monitoring.

use redis_replicator_rs::prelude::*;
use redis_replicator_rs::replication::{ReplicationClient, ReplicationEventHandler, ReplicationEvent, ReplicationState};
use redis_replicator_rs::types::ReplicationEntry;
use redis_replicator_rs::replication::client::ReplicationClientConfig;
use std::collections::HashMap;
use std::env;
use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::time;

/// A comprehensive replication event handler that tracks all replication events
#[derive(Debug, Default, Clone)]
struct ReplicationMonitor {
    stats: Arc<Mutex<ReplicationStats>>,
    start_time: Option<Instant>,
    last_heartbeat: Option<Instant>,
}

#[derive(Debug, Default)]
struct ReplicationStats {
    total_events: usize,
    rdb_entries: usize,
    aof_commands: usize,
    heartbeats: usize,
    errors: usize,
    bytes_received: u64,
    command_stats: HashMap<String, usize>,
    database_stats: HashMap<u32, usize>,
    last_offset: Option<u64>,
}

#[async_trait::async_trait]
impl ReplicationEventHandler for ReplicationMonitor {
    fn handle_event(&mut self, event: ReplicationEvent) -> Result<()> {
        let mut stats = self.stats.lock().unwrap();
        stats.total_events += 1;
        
        match event {
            ReplicationEvent::Connected { master_addr, info } => {
                println!("🔗 Connected to Redis master");
                println!("   Master address: {}", master_addr);
                println!("   Master info: {:?}", info);
                self.start_time = Some(Instant::now());
            }
            ReplicationEvent::Disconnected { reason } => {
                println!("❌ Disconnected from master: {}", reason);
            }
            ReplicationEvent::StateChanged { from: _, to: ReplicationState::FullSync } => {
                println!("🔄 Starting full synchronization");
            }
            ReplicationEvent::FullSyncCompleted { .. } => {
                println!("✅ Full synchronization completed");
                println!("   Total RDB entries: {}", stats.rdb_entries);
                println!("   Data received: {:.2} MB", stats.bytes_received as f64 / 1024.0 / 1024.0);
            }
            ReplicationEvent::StateChanged { from: _, to: ReplicationState::IncrementalSync } => {
                println!("📈 Starting incremental synchronization");
            }
            ReplicationEvent::RdbData { data, .. } => {
                stats.rdb_entries += 1;
                stats.bytes_received += data.len() as u64;
                
                // Print progress for large RDB files
                if stats.rdb_entries % 10000 == 0 {
                    println!("📦 Processed {} RDB entries ({:.2} MB)", 
                        stats.rdb_entries, 
                        stats.bytes_received as f64 / 1024.0 / 1024.0
                    );
                }
            }
            ReplicationEvent::Command { entry, offset } => {
                if let ReplicationEntry::Command { args, .. } = entry {
                    stats.aof_commands += 1;
                    stats.last_offset = Some(offset as u64);
                    
                    // Update command statistics
                    if let Some(command) = args.first() {
                        *stats.command_stats.entry(command.to_uppercase()).or_insert(0) += 1;
                    }
                
                    // Print command info (limit output for high-traffic scenarios)
                    if stats.aof_commands <= 100 || stats.aof_commands % 1000 == 0 {
                        println!("⚡ Command #{}: {} (offset: {})", 
                            stats.aof_commands, 
                            args.join(" "), 
                            offset as u64
                        );
                    }
                }
            }
            ReplicationEvent::Heartbeat { timestamp: _ } => {
                stats.heartbeats += 1;
                self.last_heartbeat = Some(Instant::now());
                
                if stats.heartbeats % 10 == 0 {
                    println!("💓 Heartbeat #{}", stats.heartbeats);
                }
            }
            ReplicationEvent::Error { error, recoverable } => {
                stats.errors += 1;
                if recoverable {
                    println!("⚠️  Recoverable error: {}", error);
                } else {
                    println!("❌ Fatal error: {}", error);
                }
            }
            ReplicationEvent::StateChanged { from: _, to: ReplicationState::Connecting } => {
                println!("🔄 Reconnecting...");
            }
            _ => {
                // Handle other events silently or with minimal logging
            }
        }
        
        Ok(())
    }
}

impl ReplicationMonitor {
    fn new() -> Self {
        Self {
            stats: Arc::new(Mutex::new(ReplicationStats::default())),
            start_time: None,
            last_heartbeat: None,
        }
    }
    
    fn print_stats(&self) {
        let stats = self.stats.lock().unwrap();
        
        println!("\n📊 Replication Statistics:");
        println!("   Total events: {}", stats.total_events);
        println!("   RDB entries: {}", stats.rdb_entries);
        println!("   AOF commands: {}", stats.aof_commands);
        println!("   Heartbeats: {}", stats.heartbeats);
        println!("   Errors: {}", stats.errors);
        println!("   Bytes received: {} ({:.2} MB)", 
            stats.bytes_received, 
            stats.bytes_received as f64 / 1024.0 / 1024.0
        );
        
        if let Some(offset) = stats.last_offset {
            println!("   Last offset: {}", offset);
        }
        
        if let Some(start) = self.start_time {
            let duration = start.elapsed();
            println!("   Running time: {:?}", duration);
            
            if stats.aof_commands > 0 {
                let commands_per_sec = stats.aof_commands as f64 / duration.as_secs_f64();
                println!("   Commands per second: {:.2}", commands_per_sec);
            }
        }
        
        if let Some(heartbeat) = self.last_heartbeat {
            let since_heartbeat = heartbeat.elapsed();
            println!("   Last heartbeat: {:?} ago", since_heartbeat);
        }
        
        // Top commands
        if !stats.command_stats.is_empty() {
            println!("\n🔥 Top Commands:");
            let mut sorted_commands: Vec<_> = stats.command_stats.iter().collect();
            sorted_commands.sort_by(|a, b| b.1.cmp(a.1));
            
            for (i, (command, count)) in sorted_commands.iter().take(10).enumerate() {
                let percentage = if stats.aof_commands > 0 {
                    (**count as f64 / stats.aof_commands as f64) * 100.0
                } else {
                    0.0
                };
                println!("   {}. {} - {} times ({:.1}%)", i + 1, command, count, percentage);
            }
        }
        
        // Database distribution
        if stats.database_stats.len() > 1 {
            println!("\n🗄️  Database Distribution:");
            let mut sorted_dbs: Vec<_> = stats.database_stats.iter().collect();
            sorted_dbs.sort_by_key(|&(db, _)| db);
            
            let total_entries = stats.rdb_entries + stats.aof_commands;
            for (db, count) in sorted_dbs {
                let percentage = if total_entries > 0 {
                    (*count as f64 / total_entries as f64) * 100.0
                } else {
                    0.0
                };
                println!("   DB{}: {} entries ({:.1}%)", db, count, percentage);
            }
        }
    }
}

/// A filtering handler that only processes specific commands or keys
#[derive(Debug)]
struct FilteringHandler {
    target_commands: Option<Vec<String>>,
    key_pattern: Option<regex::Regex>,
    matched_events: usize,
    total_events: usize,
}

impl FilteringHandler {
    fn new(commands: Option<Vec<String>>, key_pattern: Option<&str>) -> Result<Self> {
        let target_commands = commands.map(|cmds| {
            cmds.into_iter().map(|cmd| cmd.to_uppercase()).collect()
        });
        
        let key_pattern = if let Some(pattern) = key_pattern {
            Some(regex::Regex::new(pattern).map_err(|e| {
                RedisReplicatorError::config_error(&format!("Invalid regex pattern: {}", e))
            })?)
        } else {
            None
        };
        
        Ok(Self {
            target_commands,
            key_pattern,
            matched_events: 0,
            total_events: 0,
        })
    }
}

#[async_trait::async_trait]
impl ReplicationEventHandler for FilteringHandler {
    fn handle_event(&mut self, event: ReplicationEvent) -> Result<()> {
        self.total_events += 1;
        
        let should_process = match &event {
            ReplicationEvent::Command { entry, .. } => {
                if let ReplicationEntry::Command { args, .. } = entry {
                    let command_matches = if let Some(ref target_cmds) = self.target_commands {
                        if let Some(command) = args.first() {
                            target_cmds.contains(&command.to_uppercase())
                        } else {
                            false
                        }
                    } else {
                        true
                    };
                    
                    let key_matches = if let Some(ref pattern) = self.key_pattern {
                        if args.len() > 1 {
                            pattern.is_match(&args[1])
                        } else {
                            false
                        }
                    } else {
                        true
                    };
                    
                    command_matches && key_matches
                } else {
                    false
                }
            }
            ReplicationEvent::RdbData { .. } => {
                    // For RDB data, we can't filter by key pattern since it's raw data
                    true
            }
            _ => true, // Always process non-data events
        };
        
        if should_process {
            self.matched_events += 1;
            
            match event {
                ReplicationEvent::Connected { .. } => {
                    println!("🔗 Connected to Redis master (filtering enabled)");
                }
                ReplicationEvent::Command { entry, offset } => {
                    if let ReplicationEntry::Command { args, .. } = entry {
                        if let Some(command) = args.first() {
                            println!("✅ Filtered AOF: {} {} (offset: {})", 
                                command.to_uppercase(), args[1..].join(" "), offset);
                        }
                    }
                }
                ReplicationEvent::RdbData { data, offset, .. } => {
                    println!("✅ Filtered RDB data: {} bytes (offset: {})", 
                        data.len(), offset);
                }
                _ => {
                    // Handle other events normally
                }
            }
        }
        
        if self.total_events % 1000 == 0 {
            let match_rate = (self.matched_events as f64 / self.total_events as f64) * 100.0;
            println!("📊 Processed {} events, {} matched ({:.1}%)", 
                self.total_events, self.matched_events, match_rate);
        }
        
        Ok(())
    }
}

/// A backup handler that saves replication data to files
#[derive(Debug)]
struct BackupHandler {
    output_dir: String,
    rdb_file: Option<tokio::fs::File>,
    aof_file: Option<tokio::fs::File>,
    entries_written: usize,
}

impl BackupHandler {
    fn new(output_dir: &str) -> Result<Self> {
        // Create output directory if it doesn't exist
        std::fs::create_dir_all(output_dir).map_err(|e| {
            RedisReplicatorError::io_error(&format!("Failed to create output directory: {}", e))
        })?;
        
        Ok(Self {
            output_dir: output_dir.to_string(),
            rdb_file: None,
            aof_file: None,
            entries_written: 0,
        })
    }
}

#[async_trait::async_trait]
impl ReplicationEventHandler for BackupHandler {
    fn handle_event(&mut self, event: ReplicationEvent) -> Result<()> {
        match event {
            ReplicationEvent::Connected { .. } => {
                println!("🔗 Connected - starting backup to {}", self.output_dir);
            }
            ReplicationEvent::StateChanged { from: _, to: ReplicationState::FullSync } => {
                // Create RDB backup file
                let rdb_path = format!("{}/backup.rdb", self.output_dir);
                // Note: In a real implementation, you would create the file here
                println!("📦 Would create RDB backup file: {}", rdb_path);
            }
            ReplicationEvent::StateChanged { from: _, to: ReplicationState::IncrementalSync } => {
                // Create AOF backup file
                let aof_path = format!("{}/backup.aof", self.output_dir);
                // Note: In a real implementation, you would create the file here
                println!("📝 Would create AOF backup file: {}", aof_path);
            }
            ReplicationEvent::RdbData { .. } => {
                // In a real implementation, you would write the RDB data to file
                // For this example, we'll just count data chunks
                self.entries_written += 1;
                
                if self.entries_written % 10000 == 0 {
                    println!("💾 Backed up {} RDB entries", self.entries_written);
                }
            }
            ReplicationEvent::Command { entry, .. } => {
                if let ReplicationEntry::Command { .. } = entry {
                    // In a real implementation, you would write the command to the AOF file
                    // For this example, we'll just count commands
                    self.entries_written += 1;
                }
                
                if self.entries_written % 1000 == 0 {
                    println!("💾 Backed up {} AOF commands", self.entries_written);
                }
            }
            _ => {
                // Handle other events
            }
        }
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    redis_replicator_rs::init();
    
    let args: Vec<String> = env::args().collect();
    
    // Check for help flag
    if args.len() > 1 && (args[1] == "--help" || args[1] == "-h") {
        eprintln!("Usage: {} <redis_host:port> [options]", args[0]);
        eprintln!("");
        eprintln!("Options:");
        eprintln!("  --password <pass>         Redis password");
        eprintln!("  --monitor                 Monitor replication (default)");
        eprintln!("  --filter-commands <cmds>  Only replicate specific commands (comma-separated)");
        eprintln!("  --filter-keys <pattern>   Only replicate keys matching regex pattern");
        eprintln!("  --backup <dir>            Save replication data to directory");
        eprintln!("  --timeout <seconds>       Connection timeout (default: 30)");
        eprintln!("  --heartbeat <seconds>     Heartbeat interval (default: 10)");
        eprintln!("  --buffer-size <bytes>     Buffer size (default: 8192)");
        eprintln!("  --no-rdb                  Skip RDB parsing");
        eprintln!("  --no-aof                  Skip AOF parsing");
        eprintln!("  --stats-interval <sec>    Print stats every N seconds (default: 30)");
        eprintln!("");
        eprintln!("Examples:");
        eprintln!("  {} localhost:6379", args[0]);
        eprintln!("  {} redis.example.com:6379 --password mypass", args[0]);
        eprintln!("  {} localhost:6379 --filter-commands SET,GET,DEL", args[0]);
        eprintln!("  {} localhost:6379 --filter-keys '^user:.*'", args[0]);
        eprintln!("  {} localhost:6379 --backup ./backup", args[0]);
        return Ok(());
    }
    
    if args.len() < 2 {
        eprintln!("Usage: {} <redis_host:port> [options]", args[0]);
        eprintln!("");
        eprintln!("Options:");
        eprintln!("  --password <pass>         Redis password");
        eprintln!("  --monitor                 Monitor replication (default)");
        eprintln!("  --filter-commands <cmds>  Only replicate specific commands (comma-separated)");
        eprintln!("  --filter-keys <pattern>   Only replicate keys matching regex pattern");
        eprintln!("  --backup <dir>            Save replication data to directory");
        eprintln!("  --timeout <seconds>       Connection timeout (default: 30)");
        eprintln!("  --heartbeat <seconds>     Heartbeat interval (default: 10)");
        eprintln!("  --buffer-size <bytes>     Buffer size (default: 8192)");
        eprintln!("  --no-rdb                  Skip RDB parsing");
        eprintln!("  --no-aof                  Skip AOF parsing");
        eprintln!("  --stats-interval <sec>    Print stats every N seconds (default: 30)");
        eprintln!("");
        eprintln!("Examples:");
        eprintln!("  {} localhost:6379", args[0]);
        eprintln!("  {} redis.example.com:6379 --password mypass", args[0]);
        eprintln!("  {} localhost:6379 --filter-commands SET,GET,DEL", args[0]);
        eprintln!("  {} localhost:6379 --filter-keys '^user:.*'", args[0]);
        eprintln!("  {} localhost:6379 --backup ./backup", args[0]);
        return Ok(());
    }
    
    let redis_addr = &args[1];
    
    // Parse Redis address
    let (host, port) = if redis_addr.contains(':') {
        let parts: Vec<&str> = redis_addr.split(':').collect();
        if parts.len() != 2 {
            eprintln!("❌ Error: Invalid Redis address format. Use host:port");
            return Ok(());
        }
        let port: u16 = parts[1].parse().map_err(|_| {
            RedisReplicatorError::config_error("Invalid port number")
        })?;
        (parts[0].to_string(), port)
    } else {
        (redis_addr.to_string(), 6379)
    };
    
    // Parse command line options
    let mut password: Option<String> = None;
    let mut mode = "monitor";
    let mut filter_commands: Option<Vec<String>> = None;
    let mut filter_pattern: Option<String> = None;
    let mut backup_dir: Option<String> = None;
    let mut timeout_secs = 30;
    let mut heartbeat_secs = 10;
    let mut buffer_size = 8192;
    let mut parse_rdb = true;
    let mut parse_aof = true;
    let mut stats_interval = 30;
    
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--password" => {
                if i + 1 < args.len() {
                    password = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("❌ Error: --password requires a value");
                    return Ok(());
                }
            }
            "--monitor" => {
                mode = "monitor";
                i += 1;
            }
            "--filter-commands" => {
                if i + 1 < args.len() {
                    filter_commands = Some(args[i + 1].split(',').map(|s| s.trim().to_string()).collect());
                    mode = "filter";
                    i += 2;
                } else {
                    eprintln!("❌ Error: --filter-commands requires a value");
                    return Ok(());
                }
            }
            "--filter-keys" => {
                if i + 1 < args.len() {
                    filter_pattern = Some(args[i + 1].clone());
                    if mode == "monitor" {
                        mode = "filter";
                    }
                    i += 2;
                } else {
                    eprintln!("❌ Error: --filter-keys requires a value");
                    return Ok(());
                }
            }
            "--backup" => {
                if i + 1 < args.len() {
                    backup_dir = Some(args[i + 1].clone());
                    mode = "backup";
                    i += 2;
                } else {
                    eprintln!("❌ Error: --backup requires a value");
                    return Ok(());
                }
            }
            "--timeout" => {
                if i + 1 < args.len() {
                    timeout_secs = args[i + 1].parse().map_err(|_| {
                        RedisReplicatorError::config_error("Invalid timeout value")
                    })?;
                    i += 2;
                } else {
                    eprintln!("❌ Error: --timeout requires a value");
                    return Ok(());
                }
            }
            "--heartbeat" => {
                if i + 1 < args.len() {
                    heartbeat_secs = args[i + 1].parse().map_err(|_| {
                        RedisReplicatorError::config_error("Invalid heartbeat value")
                    })?;
                    i += 2;
                } else {
                    eprintln!("❌ Error: --heartbeat requires a value");
                    return Ok(());
                }
            }
            "--buffer-size" => {
                if i + 1 < args.len() {
                    buffer_size = args[i + 1].parse().map_err(|_| {
                        RedisReplicatorError::config_error("Invalid buffer size value")
                    })?;
                    i += 2;
                } else {
                    eprintln!("❌ Error: --buffer-size requires a value");
                    return Ok(());
                }
            }
            "--stats-interval" => {
                if i + 1 < args.len() {
                    stats_interval = args[i + 1].parse().map_err(|_| {
                        RedisReplicatorError::config_error("Invalid stats interval value")
                    })?;
                    i += 2;
                } else {
                    eprintln!("❌ Error: --stats-interval requires a value");
                    return Ok(());
                }
            }
            "--no-rdb" => {
                parse_rdb = false;
                i += 1;
            }
            "--no-aof" => {
                parse_aof = false;
                i += 1;
            }
            _ => {
                eprintln!("❌ Error: Unknown option '{}'", args[i]);
                return Ok(());
            }
        }
    }
    
    println!("🚀 Starting Redis replication client");
    println!("   Master: {}:{}", host, port);
    println!("   Mode: {}", mode);
    println!("   Parse RDB: {}", parse_rdb);
    println!("   Parse AOF: {}", parse_aof);
    
    // Create replication client configuration
    let addr_str = format!("{}:{}", host, port);
    println!("🔍 Resolving address: {}", addr_str);
    let master_addr = addr_str.to_socket_addrs()
        .map_err(|e| {
            eprintln!("❌ Error resolving address '{}': {:?}", addr_str, e);
            RedisReplicatorError::config_error(&format!("Cannot resolve address: {}", addr_str))
        })?
        .next()
        .ok_or_else(|| {
            eprintln!("❌ No addresses found for '{}'", addr_str);
            RedisReplicatorError::config_error(&format!("No addresses found for: {}", addr_str))
        })?;
    
    let config = ReplicationClientConfig {
        master_addr,
        password,
        connect_timeout: Duration::from_secs(timeout_secs),
        read_timeout: Duration::from_secs(timeout_secs),
        heartbeat_interval: Duration::from_secs(heartbeat_secs),
        buffer_size,
        parse_rdb,
        parse_aof,
        ..Default::default()
    };
    
    // Create and start replication client
    let mut client = ReplicationClient::new(config);
    
    // Handle Ctrl+C gracefully
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        println!("\n🛑 Received Ctrl+C, shutting down gracefully...");
        std::process::exit(0);
    });
    
    // Start replication with appropriate handler based on mode
    let result = match mode {
        "filter" => {
            let handler = FilteringHandler::new(filter_commands, filter_pattern.as_deref())?;
            client.start_replication(handler).await
        }
        "backup" => {
            let handler = BackupHandler::new(&backup_dir.unwrap())?;
            client.start_replication(handler).await
        }
        "monitor" | _ => {
            let monitor = ReplicationMonitor::new();
            
            // Start stats reporting task for monitor mode
            let monitor_clone = monitor.clone();
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_secs(stats_interval));
                loop {
                    interval.tick().await;
                    monitor_clone.print_stats();
                }
            });
            
            client.start_replication(monitor).await
        }
    };
    
    match result {
        Ok(_) => {
            println!("✅ Replication completed successfully");
        }
        Err(e) => {
            eprintln!("❌ Replication failed: {}", e);
            return Err(e);
        }
    }
    
    println!("\n✨ Done!");
    Ok(())
}