//! Example: Parsing an AOF file
//!
//! This example demonstrates how to parse a Redis AOF (Append Only File) using the redis_replicator_rs library.
//! It shows command filtering, statistics collection, and replay capabilities.

use redis_replicator_rs::prelude::*;
use redis_replicator_rs::aof::{self, AofParseOptions};
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::Path;
use tokio::fs;

/// A comprehensive AOF event handler that collects statistics and commands
#[derive(Debug, Default)]
struct AofAnalyzer {
    commands: Vec<AofEntry>,
    command_stats: HashMap<String, usize>,
    database_stats: HashMap<u32, usize>,
    key_stats: HashMap<String, usize>,
    total_commands: usize,
    start_time: Option<std::time::Instant>,
}

impl AofEventHandler for AofAnalyzer {
    fn handle_event(&mut self, event: AofEvent) -> Result<()> {
        match event {
            AofEvent::Started { .. } => {
                println!("🚀 Starting AOF parsing...");
                self.start_time = Some(std::time::Instant::now());
            }
            AofEvent::Command { entry, .. } => {
                self.total_commands += 1;
                
                // Update command statistics
                if let AofEntry::Command { args, .. } = &entry {
                    if !args.is_empty() {
                        let command = &args[0];
                        *self.command_stats.entry(command.to_uppercase()).or_insert(0) += 1;
                        
                        // Update key statistics (for commands that have keys)
                        if self.is_key_command(command) && args.len() > 1 {
                            let key = &args[1];
                            *self.key_stats.entry(key.clone()).or_insert(0) += 1;
                        }
                    }
                }
                
                self.commands.push(entry.clone());
                
                // Print command info (limit output for large files)
                if let AofEntry::Command { args, timestamp } = &entry {
                    if self.total_commands <= 100 || self.total_commands % 10000 == 0 {
                        let timestamp_str = if let Some(ts) = timestamp {
                            format!(" @{}", ts)
                        } else {
                            String::new()
                        };
                        
                        println!("📝 [{}] {} {}{}", 
                            self.total_commands, 
                            args[0].to_uppercase(), 
                            args[1..].join(" "),
                            timestamp_str
                        );
                    }
                }
            }
            AofEvent::Completed { stats: _ } => {
                if let Some(start) = self.start_time {
                    let duration = start.elapsed();
                    println!("🎉 AOF parsing completed in {:?}", duration);
                    println!("   Commands per second: {:.2}", 
                        self.total_commands as f64 / duration.as_secs_f64());
                }
            }
            AofEvent::Error { error, position } => {
                eprintln!("❌ AOF parsing error at position {}: {}", position, error);
            }
            AofEvent::Progress { .. } => {
                // Handle progress updates if needed
            }
        }
        Ok(())
    }
}

impl AofAnalyzer {
    fn is_key_command(&self, command: &str) -> bool {
        matches!(command.to_uppercase().as_str(), 
            "SET" | "GET" | "DEL" | "HSET" | "HGET" | "LPUSH" | "RPUSH" | 
            "SADD" | "ZADD" | "INCR" | "DECR" | "EXPIRE" | "TTL" | "EXISTS" |
            "APPEND" | "STRLEN" | "LLEN" | "SCARD" | "HLEN" | "ZCARD"
        )
    }
    
    fn print_analysis(&self) {
        println!("\n📊 AOF Analysis Report:");
        println!("   Total commands: {}", self.total_commands);
        println!("   Unique command types: {}", self.command_stats.len());
        println!("   Databases used: {}", self.database_stats.len());
        println!("   Unique keys accessed: {}", self.key_stats.len());
        
        // Top commands
        println!("\n🔥 Top 10 Commands:");
        let mut sorted_commands: Vec<_> = self.command_stats.iter().collect();
        sorted_commands.sort_by(|a, b| b.1.cmp(a.1));
        
        for (i, (command, count)) in sorted_commands.iter().take(10).enumerate() {
            let percentage = (**count as f64 / self.total_commands as f64) * 100.0;
            println!("   {}. {} - {} times ({:.1}%)", i + 1, command, count, percentage);
        }
        
        // Database distribution
        if self.database_stats.len() > 1 {
            println!("\n🗄️  Database Distribution:");
            let mut sorted_dbs: Vec<_> = self.database_stats.iter().collect();
            sorted_dbs.sort_by_key(|&(db, _)| db);
            
            for (db, count) in sorted_dbs {
                let percentage = (*count as f64 / self.total_commands as f64) * 100.0;
                println!("   DB{}: {} commands ({:.1}%)", db, count, percentage);
            }
        }
        
        // Hot keys
        if !self.key_stats.is_empty() {
            println!("\n🔑 Top 10 Most Accessed Keys:");
            let mut sorted_keys: Vec<_> = self.key_stats.iter().collect();
            sorted_keys.sort_by(|a, b| b.1.cmp(a.1));
            
            for (i, (key, count)) in sorted_keys.iter().take(10).enumerate() {
                println!("   {}. {} - {} accesses", i + 1, key, count);
            }
        }
    }
}

/// A command filter that only processes specific commands
#[derive(Debug)]
struct CommandFilter {
    target_commands: HashSet<String>,
    target_keys: Option<regex::Regex>,
    matched_commands: Vec<AofEntry>,
    stats: HashMap<String, usize>,
}

impl CommandFilter {
    fn new(commands: Vec<String>, key_pattern: Option<&str>) -> Result<Self> {
        let target_commands: HashSet<String> = commands.into_iter()
            .map(|cmd| cmd.to_uppercase())
            .collect();
        
        let target_keys = if let Some(pattern) = key_pattern {
            Some(regex::Regex::new(pattern).map_err(|e| {
                RedisReplicatorError::config_error(&format!("Invalid regex pattern: {}", e))
            })?)
        } else {
            None
        };
        
        Ok(Self {
            target_commands,
            target_keys,
            matched_commands: Vec::new(),
            stats: HashMap::new(),
        })
    }
}

impl AofEventHandler for CommandFilter {
    fn handle_event(&mut self, event: AofEvent) -> Result<()> {
        match event {
            AofEvent::Started { .. } => {
                println!("🔍 Starting filtered AOF parsing...");
                if !self.target_commands.is_empty() {
                    println!("   Target commands: {:?}", self.target_commands);
                }
                if let Some(ref pattern) = self.target_keys {
                    println!("   Key pattern: {}", pattern.as_str());
                }
            }
            AofEvent::Command { entry, .. } => {
                if let AofEntry::Command { args, timestamp: _ } = &entry {
                    let cmd_upper = args[0].to_uppercase();
                    
                    // Check command filter
                    let command_matches = self.target_commands.is_empty() || 
                        self.target_commands.contains(&cmd_upper);
                    
                    // Check key filter
                    let key_matches = if let Some(ref pattern) = self.target_keys {
                        if args.len() > 1 {
                            pattern.is_match(&args[1])
                        } else {
                            false
                        }
                    } else {
                        true
                    };
                    
                    if command_matches && key_matches {
                        self.matched_commands.push(entry.clone());
                        *self.stats.entry(cmd_upper.clone()).or_insert(0) += 1;
                        
                        println!("✅ Matched: {} {}", cmd_upper, args[1..].join(" "));
                    }
                }
            }
            AofEvent::Completed { stats: _ } => {
                println!("🎯 Filtering completed! Found {} matching commands", 
                    self.matched_commands.len());
                
                if !self.stats.is_empty() {
                    println!("\n📈 Matched Command Statistics:");
                    for (cmd, count) in &self.stats {
                        println!("   {}: {} times", cmd, count);
                    }
                }
            }
            AofEvent::Error { error, position } => {
                eprintln!("❌ AOF filtering error at position {}: {}", position, error);
            }
            AofEvent::Progress { .. } => {
                // Handle progress updates if needed
            }
        }
        Ok(())
    }
}

/// A replay handler that can reconstruct Redis state
#[derive(Debug, Default)]
struct ReplayHandler {
    simulated_data: HashMap<String, String>, // Simplified state tracking
    current_db: u32,
    commands_replayed: usize,
}

impl AofEventHandler for ReplayHandler {
    fn handle_event(&mut self, event: AofEvent) -> Result<()> {
        match event {
            AofEvent::Started { .. } => {
                println!("🔄 Starting AOF replay simulation...");
            }
            AofEvent::Command { entry, .. } => {
                if let AofEntry::Command { args, timestamp: _ } = &entry {
                    // Simulate command execution (simplified)
                    match args[0].to_uppercase().as_str() {
                        "SET" if args.len() >= 3 => {
                            let key = format!("db{}:{}", self.current_db, args[1]);
                            self.simulated_data.insert(key.clone(), args[2].clone());
                            println!("💾 SET {} = {}", args[1], args[2]);
                        }
                        "DEL" if args.len() >= 2 => {
                            for arg in &args[1..] {
                                let key = format!("db{}:{}", self.current_db, arg);
                                if self.simulated_data.remove(&key).is_some() {
                                    println!("🗑️  DEL {}", arg);
                                }
                            }
                        }
                        "FLUSHDB" => {
                             let keys_to_remove: Vec<_> = self.simulated_data.keys()
                                 .filter(|k| k.starts_with(&format!("db{}:", self.current_db)))
                                 .cloned()
                                 .collect();
                             
                             for key in keys_to_remove {
                                 self.simulated_data.remove(&key);
                             }
                             println!("🧹 FLUSHDB - cleared database {}", self.current_db);
                         }
                         "FLUSHALL" => {
                             self.simulated_data.clear();
                             println!("🧹 FLUSHALL - cleared all databases");
                         }
                         _ => {
                             // For other commands, just count them
                         }
                     }
                     
                     self.commands_replayed += 1;
                     
                     if self.commands_replayed % 1000 == 0 {
                         println!("📊 Replayed {} commands, current state: {} keys", 
                             self.commands_replayed, self.simulated_data.len());
                     }
                 }
            }
            AofEvent::Completed { stats: _ } => {
                println!("🎬 Replay completed!");
                println!("   Commands replayed: {}", self.commands_replayed);
                println!("   Final state: {} keys across all databases", self.simulated_data.len());
                
                // Show sample of final state
                if !self.simulated_data.is_empty() {
                    println!("\n🔍 Sample of final state:");
                    for (i, (key, value)) in self.simulated_data.iter().take(10).enumerate() {
                        let display_value = if value.len() > 50 {
                            format!("{}...", &value[..50])
                        } else {
                            value.clone()
                        };
                        println!("   {}. {} = \"{}\"", i + 1, key, display_value);
                    }
                    
                    if self.simulated_data.len() > 10 {
                        println!("   ... and {} more keys", self.simulated_data.len() - 10);
                    }
                }
            }
            AofEvent::Error { error, position } => {
                eprintln!("❌ AOF replay error at position {}: {}", position, error);
            }
            AofEvent::Progress { .. } => {
                // Handle progress updates if needed
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
    
    if args.len() < 2 {
        eprintln!("Usage: {} <aof_file> [options]", args[0]);
        eprintln!("");
        eprintln!("Options:");
        eprintln!("  --analyze                 Perform detailed analysis (default)");
        eprintln!("  --filter-commands <cmds>  Only show specific commands (comma-separated)");
        eprintln!("  --filter-keys <pattern>   Only show keys matching regex pattern");
        eprintln!("  --replay                  Simulate command replay");
        eprintln!("  --validate-only           Only validate the AOF file");
        eprintln!("  --max-commands <n>        Limit the number of commands to process");
        eprintln!("  --skip-commands <n>       Skip the first n commands");
        eprintln!("");
        eprintln!("Examples:");
        eprintln!("  {} appendonly.aof", args[0]);
        eprintln!("  {} appendonly.aof --filter-commands SET,GET,DEL", args[0]);
        eprintln!("  {} appendonly.aof --filter-keys '^user:.*'", args[0]);
        eprintln!("  {} appendonly.aof --replay", args[0]);
        return Ok(());
    }
    
    let aof_file = &args[1];
    
    // Check if file exists
    if !Path::new(aof_file).exists() {
        eprintln!("❌ Error: File '{}' does not exist", aof_file);
        return Ok(());
    }
    
    // Parse command line options
    let mut mode = "analyze";
    let mut filter_commands: Vec<String> = Vec::new();
    let mut filter_pattern: Option<String> = None;
    let mut max_commands: Option<usize> = None;
    let mut skip_commands: Option<usize> = None;
    
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--analyze" => {
                mode = "analyze";
                i += 1;
            }
            "--replay" => {
                mode = "replay";
                i += 1;
            }
            "--validate-only" => {
                mode = "validate";
                i += 1;
            }
            "--filter-commands" => {
                if i + 1 < args.len() {
                    filter_commands = args[i + 1].split(',').map(|s| s.trim().to_string()).collect();
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
                    if mode == "analyze" {
                        mode = "filter";
                    }
                    i += 2;
                } else {
                    eprintln!("❌ Error: --filter-keys requires a value");
                    return Ok(());
                }
            }
            "--max-commands" => {
                if i + 1 < args.len() {
                    max_commands = Some(args[i + 1].parse().map_err(|_| {
                        RedisReplicatorError::config_error("Invalid max-commands value")
                    })?);
                    i += 2;
                } else {
                    eprintln!("❌ Error: --max-commands requires a value");
                    return Ok(());
                }
            }
            "--skip-commands" => {
                if i + 1 < args.len() {
                    skip_commands = Some(args[i + 1].parse().map_err(|_| {
                        RedisReplicatorError::config_error("Invalid skip-commands value")
                    })?);
                    i += 2;
                } else {
                    eprintln!("❌ Error: --skip-commands requires a value");
                    return Ok(());
                }
            }
            _ => {
                eprintln!("❌ Error: Unknown option '{}'", args[i]);
                return Ok(());
            }
        }
    }
    
    println!("📂 Processing AOF file: {}", aof_file);
    
    // Get file size for progress reporting
    let metadata = fs::metadata(aof_file).await?;
    let file_size = metadata.len();
    println!("📏 File size: {} bytes ({:.2} MB)", file_size, file_size as f64 / 1024.0 / 1024.0);
    
    let options = AofParseOptions {
        validate_only: mode == "validate",
        max_entries: max_commands.unwrap_or(0) as u64,
        skip_entries: skip_commands.unwrap_or(0) as u64,
        ..Default::default()
    };
    
    match mode {
        "validate" => {
            println!("🔍 Validating AOF file...");
            
            match aof::validate_file(aof_file).await {
                Ok(info) => {
                    println!("✅ AOF file is valid!");
                    println!("   Total commands: {}", info.command_count);
                    println!("   File size: {} bytes", info.file_size);
                    println!("   Command types: {}", info.command_types.len());
                    
                    if !info.command_types.is_empty() {
                        println!("   Commands found: {:?}", info.command_types);
                    }
                }
                Err(e) => {
                    eprintln!("❌ AOF file validation failed: {}", e);
                    return Err(e);
                }
            }
        }
        "filter" => {
            let _handler = CommandFilter::new(
                filter_commands,
                filter_pattern.as_deref()
            )?;
            
            let _entries = aof::parse_file_with_options(aof_file, options).await?;
        }
        "replay" => {
            let _handler = ReplayHandler::default();
            let _entries = aof::parse_file_with_options(aof_file, options).await?;
        }
        "analyze" | _ => {
            let _handler = AofAnalyzer::default();
            let _entries = aof::parse_file_with_options(aof_file, options).await?;
            _handler.print_analysis();
        }
    }
    
    println!("\n✨ Done!");
    Ok(())
}