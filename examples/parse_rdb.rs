//! Example: Parsing an RDB file
//!
//! This example demonstrates how to parse a Redis RDB file using the redis_replicator_rs library.
//! It shows both basic parsing and advanced filtering capabilities.

use redis_replicator_rs::prelude::*;
use redis_replicator_rs::rdb::{self, RdbParseOptions};
use std::collections::HashMap;
use std::env;
use std::path::Path;
use tokio::fs;

/// A simple event handler that collects and prints RDB entries
#[derive(Debug, Default)]
struct SimpleRdbHandler {
    entries: Vec<RdbEntry>,
    stats: HashMap<String, usize>,
    total_memory: usize,
}

impl RdbEventHandler for SimpleRdbHandler {
    fn handle_event(&mut self, event: RdbEvent) -> Result<()> {
        match event {
            RdbEvent::StartOfFile { version, .. } => {
                println!("🚀 Starting RDB parsing (version: {})", version);
            }
            RdbEvent::StartOfDatabase { database_id } => {
                println!("📁 Processing database: {}", database_id);
            }
            RdbEvent::KeyValue { database_id, key, value, expiry, .. } => {
                let type_name = value.type_name();
                let memory_usage = value.memory_usage();
                
                // Update statistics
                *self.stats.entry(type_name.to_string()).or_insert(0) += 1;
                self.total_memory += memory_usage;
                
                let key_str = String::from_utf8_lossy(&key).to_string();
                
                // Create RDB entry
                let entry = RdbEntry::KeyValue {
                    key: key_str.clone(),
                    value,
                    expire_ms: expiry,
                    idle: None,
                    freq: None,
                };
                
                self.entries.push(entry);
                
                // Print key information
                println!("🔑 Key: {} (type: {}, size: {} bytes, db: {})", 
                    key_str, type_name, memory_usage, database_id);
                
                if let Some(expiry) = expiry {
                    println!("   ⏰ Expires at: {}", expiry);
                }
            }
            RdbEvent::AuxField { key, value } => {
                println!("ℹ️  Auxiliary field: {} = {}", key, value);
            }
            RdbEvent::EndOfDatabase { database_id, .. } => {
                println!("✅ Finished database: {}", database_id);
            }
            RdbEvent::EndOfFile { .. } => {
                println!("🎉 RDB parsing completed!");
            }
            RdbEvent::ResizeDatabase { .. } => {
                // Database resize hint - no action needed for simple handler
            }
            RdbEvent::Error { .. } => {
                // Error event - handled by parser
            }
         }
         Ok(())
     }
 }

impl SimpleRdbHandler {
    fn print_summary(&self) {
        println!("\n📊 Parsing Summary:");
        println!("   Total entries: {}", self.entries.len());
        println!("   Total memory usage: {} bytes ({:.2} MB)", 
            self.total_memory, 
            self.total_memory as f64 / 1024.0 / 1024.0
        );
        
        println!("\n📈 Type distribution:");
        for (type_name, count) in &self.stats {
            println!("   {}: {} entries", type_name, count);
        }
        
        if !self.entries.is_empty() {
            println!("\n🔍 Sample entries:");
            for (i, entry) in self.entries.iter().take(5).enumerate() {
                if let RdbEntry::KeyValue { key, value, .. } = entry {
                    println!("   {}. {} ({})", i + 1, key, value.type_name());
                }
            }
            
            if self.entries.len() > 5 {
                println!("   ... and {} more entries", self.entries.len() - 5);
            }
        }
    }
}

/// A filtering handler that only processes specific types of data
#[derive(Debug)]
struct FilteringRdbHandler {
    target_types: Vec<String>,
    target_pattern: Option<regex::Regex>,
    matched_entries: Vec<RdbEntry>,
}

impl FilteringRdbHandler {
    fn new(types: Vec<String>, key_pattern: Option<&str>) -> Result<Self> {
        let target_pattern = if let Some(pattern) = key_pattern {
            Some(regex::Regex::new(pattern).map_err(|e| {
                RedisReplicatorError::config_error(&format!("Invalid regex pattern: {}", e))
            })?)
        } else {
            None
        };
        
        Ok(Self {
            target_types: types,
            target_pattern,
            matched_entries: Vec::new(),
        })
    }
}

impl RdbEventHandler for FilteringRdbHandler {
    fn handle_event(&mut self, event: RdbEvent) -> Result<()> {
        match event {
            RdbEvent::StartOfFile { version, .. } => {
                println!("🔍 Starting filtered RDB parsing (version: {})", version);
                println!("   Target types: {:?}", self.target_types);
                if let Some(ref pattern) = self.target_pattern {
                    println!("   Key pattern: {}", pattern.as_str());
                }
            }
            RdbEvent::KeyValue { key, value, expiry, .. } => {
                let type_name = value.type_name();
                let key_str = String::from_utf8_lossy(&key).to_string();
                
                // Check if this entry matches our filters
                let type_matches = self.target_types.is_empty() || 
                    self.target_types.contains(&type_name.to_string());
                
                let pattern_matches = if let Some(ref pattern) = self.target_pattern {
                    pattern.is_match(&key_str)
                } else {
                    true
                };
                
                if type_matches && pattern_matches {
                    let entry = RdbEntry::KeyValue {
                        key: key_str.clone(),
                        value,
                        expire_ms: expiry,
                        idle: None,
                        freq: None,
                    };
                    
                    self.matched_entries.push(entry);
                    println!("✅ Matched: {} ({})", key_str, type_name);
                }
            }
            RdbEvent::EndOfFile { .. } => {
                println!("🎯 Filtering completed! Found {} matching entries", 
                    self.matched_entries.len());
            }
            RdbEvent::ResizeDatabase { .. } => {
                // Database resize hint - no action needed
            }
            RdbEvent::Error { .. } => {
                // Error event - handled by parser
            }
            _ => {} // Ignore other events for filtering
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
        eprintln!("Usage: {} <rdb_file> [options]", args[0]);
        eprintln!("");
        eprintln!("Options:");
        eprintln!("  --filter-types <types>    Only show specific types (comma-separated)");
        eprintln!("  --filter-keys <pattern>   Only show keys matching regex pattern");
        eprintln!("  --validate-only           Only validate the RDB file without parsing");
        eprintln!("  --max-entries <n>         Limit the number of entries to process");
        eprintln!("");
        eprintln!("Examples:");
        eprintln!("  {} dump.rdb", args[0]);
        eprintln!("  {} dump.rdb --filter-types string,hash", args[0]);
        eprintln!("  {} dump.rdb --filter-keys '^user:.*'", args[0]);
        eprintln!("  {} dump.rdb --validate-only", args[0]);
        return Ok(());
    }
    
    let rdb_file = &args[1];
    
    // Check if file exists
    if !Path::new(rdb_file).exists() {
        eprintln!("❌ Error: File '{}' does not exist", rdb_file);
        return Ok(());
    }
    
    // Parse command line options
    let mut filter_types: Vec<String> = Vec::new();
    let mut filter_pattern: Option<String> = None;
    let mut validate_only = false;
    let mut max_entries: Option<usize> = None;
    
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--filter-types" => {
                if i + 1 < args.len() {
                    filter_types = args[i + 1].split(',').map(|s| s.trim().to_string()).collect();
                    i += 2;
                } else {
                    eprintln!("❌ Error: --filter-types requires a value");
                    return Ok(());
                }
            }
            "--filter-keys" => {
                if i + 1 < args.len() {
                    filter_pattern = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("❌ Error: --filter-keys requires a value");
                    return Ok(());
                }
            }
            "--validate-only" => {
                validate_only = true;
                i += 1;
            }
            "--max-entries" => {
                if i + 1 < args.len() {
                    max_entries = Some(args[i + 1].parse().map_err(|_| {
                        RedisReplicatorError::config_error("Invalid max-entries value")
                    })?);
                    i += 2;
                } else {
                    eprintln!("❌ Error: --max-entries requires a value");
                    return Ok(());
                }
            }
            _ => {
                eprintln!("❌ Error: Unknown option '{}'", args[i]);
                return Ok(());
            }
        }
    }
    
    println!("📂 Processing RDB file: {}", rdb_file);
    
    // Get file size for progress reporting
    let metadata = fs::metadata(rdb_file).await?;
    let file_size = metadata.len();
    println!("📏 File size: {} bytes ({:.2} MB)", file_size, file_size as f64 / 1024.0 / 1024.0);
    
    if validate_only {
        // Validation mode
        println!("🔍 Validating RDB file...");
        
        match rdb::validate_file(rdb_file).await {
            Ok(info) => {
                println!("✅ RDB file is valid!");
                println!("   Version: {}", info.version);
                println!("   Databases: {}", info.database_count);
                println!("   Total keys: {}", info.key_count);
                println!("   File size: {} bytes", info.file_size);
                if let Some(checksum) = info.checksum {
                    println!("   Checksum: {:016x}", checksum);
                }
            }
            Err(e) => {
                eprintln!("❌ RDB file validation failed: {}", e);
                return Err(e);
            }
        }
    } else if !filter_types.is_empty() || filter_pattern.is_some() {
        // Filtering mode
        let handler = FilteringRdbHandler::new(
            filter_types, 
            filter_pattern.as_deref()
        )?;
        
        let options = RdbParseOptions {
            verify_checksums: true,
            max_memory: if let Some(max) = max_entries {
                max * 1024 * 1024  // Convert to approximate memory limit
            } else {
                1024 * 1024 * 1024  // Default 1GB
            },
            ..Default::default()
        };
        
        let _entries = rdb::parse_file_with_options(rdb_file, options).await?;
        
        // Print filtered results
        println!("\n🎯 Filtered Results:");
        for (i, entry) in handler.matched_entries.iter().enumerate() {
            if let RdbEntry::KeyValue { key, value, .. } = entry {
                println!("{}. {} ({})", i + 1, key, value.type_name());
                
                // Show value preview for some types
                match value {
                RedisValue::String(s) => {
                    let preview = if s.len() > 50 {
                        format!("{}...", &s[..50])
                    } else {
                        s.clone()
                    };
                    println!("   Value: \"{}\"", preview);
                }
                RedisValue::List(list) => {
                    println!("   Length: {} items", list.len());
                    if !list.is_empty() {
                        println!("   First item: \"{}\"", list[0]);
                    }
                }
                RedisValue::Set(set) => {
                    println!("   Size: {} members", set.len());
                }
                RedisValue::Hash(hash) => {
                    println!("   Fields: {} key-value pairs", hash.len());
                }
                RedisValue::ZSet(zset) => {
                    println!("   Members: {} scored items", zset.len());
                }
                _ => {}
                }
            }
        }
    } else {
        // Normal parsing mode
        let handler = SimpleRdbHandler::default();
        
        let options = RdbParseOptions {
            verify_checksums: true,
            ..Default::default()
        };
        
        let _entries = rdb::parse_file_with_options(rdb_file, options).await?;
        
        // Print summary
        handler.print_summary();
    }
    
    println!("\n✨ Done!");
    Ok(())
}