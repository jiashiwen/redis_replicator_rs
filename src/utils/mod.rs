//! Utility functions and helpers

pub mod crc;
pub mod compression;
pub mod io;
pub mod buffer;

pub use crc::{crc16, crc64};
pub use compression::{lzf_decompress, lz4_decompress};
pub use io::{read_exact, read_u8, read_u16_le, read_u32_le, read_u64_le};
pub use buffer::BufferManager;

use crate::error::{RedisReplicatorError, Result};
use std::time::{SystemTime, UNIX_EPOCH};

/// Get current timestamp in milliseconds
pub fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Get current timestamp in microseconds
pub fn current_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Convert Redis timestamp to system time
pub fn redis_timestamp_to_system_time(timestamp: u64) -> SystemTime {
    UNIX_EPOCH + std::time::Duration::from_millis(timestamp)
}

/// Check if a string needs to be represented with quotes
pub fn string_needs_repr(s: &str) -> bool {
    if s.is_empty() {
        return true;
    }
    
    for ch in s.chars() {
        if ch.is_control() || ch.is_whitespace() || ch == '"' || ch == '\\' {
            return true;
        }
    }
    
    false
}

/// Escape a string for representation
pub fn string_repr(s: &str) -> String {
    if !string_needs_repr(s) {
        return s.to_string();
    }
    
    let mut result = String::with_capacity(s.len() + 2);
    result.push('"');
    
    for ch in s.chars() {
        match ch {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c.is_control() => {
                result.push_str(&format!("\\x{:02x}", c as u8));
            },
            c => result.push(c),
        }
    }
    
    result.push('"');
    result
}

/// Parse a Redis integer from bytes
pub fn parse_redis_int(data: &[u8]) -> Result<i64> {
    let s = std::str::from_utf8(data)
        .map_err(|e| RedisReplicatorError::parse_error(0, format!("Invalid UTF-8: {}", e)))?;
    
    s.parse::<i64>()
        .map_err(|e| RedisReplicatorError::parse_error(0, format!("Invalid integer: {}", e)))
}

/// Parse a Redis float from bytes
pub fn parse_redis_float(data: &[u8]) -> Result<f64> {
    let s = std::str::from_utf8(data)
        .map_err(|e| RedisReplicatorError::parse_error(0, format!("Invalid UTF-8: {}", e)))?;
    
    match s {
        "inf" | "+inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        "nan" => Ok(f64::NAN),
        _ => s.parse::<f64>()
            .map_err(|e| RedisReplicatorError::parse_error(0, format!("Invalid float: {}", e))),
    }
}

/// Calculate the number of CPU cores
pub fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Create a file path safely
pub fn make_path(dir: &str, filename: &str) -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(dir);
    path.push(filename);
    path
}

/// Check if a file exists and is readable
pub fn file_exists_and_readable(path: &std::path::Path) -> bool {
    path.exists() && path.is_file() && 
    std::fs::metadata(path)
        .map(|m| !m.permissions().readonly())
        .unwrap_or(false)
}

/// Get file size
pub fn file_size(path: &std::path::Path) -> Result<u64> {
    std::fs::metadata(path)
        .map(|m| m.len())
        .map_err(|e| RedisReplicatorError::Io(e))
}

/// Hexdump utility for debugging
pub fn hexdump(data: &[u8], offset: usize, max_bytes: usize) -> String {
    let mut result = String::new();
    let end = std::cmp::min(data.len(), max_bytes);
    
    for (i, chunk) in data[..end].chunks(16).enumerate() {
        let addr = offset + i * 16;
        result.push_str(&format!("{:08x}: ", addr));
        
        // Hex bytes
        for (j, &byte) in chunk.iter().enumerate() {
            if j == 8 {
                result.push(' ');
            }
            result.push_str(&format!("{:02x} ", byte));
        }
        
        // Padding
        for _ in chunk.len()..16 {
            result.push_str("   ");
        }
        
        result.push_str(" |");
        
        // ASCII representation
        for &byte in chunk {
            let ch = if byte.is_ascii_graphic() || byte == b' ' {
                byte as char
            } else {
                '.'
            };
            result.push(ch);
        }
        
        result.push_str("|\n");
    }
    
    if data.len() > max_bytes {
        result.push_str(&format!("... ({} more bytes)\n", data.len() - max_bytes));
    }
    
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_repr() {
        assert_eq!(string_repr("hello"), "hello");
        assert_eq!(string_repr("hello world"), "\"hello world\"");
        assert_eq!(string_repr("hello\nworld"), "\"hello\\nworld\"");
        assert_eq!(string_repr(""), "\"\"");
    }

    #[test]
    fn test_parse_redis_int() {
        assert_eq!(parse_redis_int(b"123").unwrap(), 123);
        assert_eq!(parse_redis_int(b"-456").unwrap(), -456);
        assert!(parse_redis_int(b"abc").is_err());
    }

    #[test]
    fn test_parse_redis_float() {
        assert_eq!(parse_redis_float(b"3.14").unwrap(), 3.14);
        assert!(parse_redis_float(b"inf").unwrap().is_infinite());
        assert!(parse_redis_float(b"nan").unwrap().is_nan());
    }

    #[test]
    fn test_timestamp() {
        let ts = current_timestamp_ms();
        assert!(ts > 0);
        
        let us = current_timestamp_us();
        assert!(us > ts * 1000);
    }

    #[test]
    fn test_hexdump() {
        let data = b"Hello, World!";
        let dump = hexdump(data, 0, 32);
        assert!(dump.contains("48 65 6c 6c 6f"));
        assert!(dump.contains("Hello"));
    }
}