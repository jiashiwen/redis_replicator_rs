//! RESP (Redis Serialization Protocol) parser
//!
//! This module implements parsing for the Redis Serialization Protocol,
//! which is used in AOF files and Redis communication.

use crate::error::{RedisReplicatorError, Result};
use bytes::Bytes;
use std::str;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

/// RESP protocol value types
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// Simple string (+)
    SimpleString(String),
    /// Error (-)
    Error(String),
    /// Integer (:)
    Integer(i64),
    /// Bulk string ($)
    BulkString(Option<Bytes>),
    /// Array (*)
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    /// Check if this is a null value
    pub fn is_null(&self) -> bool {
        matches!(self, RespValue::BulkString(None) | RespValue::Array(None))
    }
    
    /// Convert to bytes if this is a bulk string
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            RespValue::BulkString(Some(bytes)) => Some(bytes),
            _ => None,
        }
    }
    
    /// Convert to string if possible
    pub fn as_string(&self) -> Option<String> {
        match self {
            RespValue::SimpleString(s) => Some(s.clone()),
            RespValue::BulkString(Some(bytes)) => {
                String::from_utf8(bytes.to_vec()).ok()
            }
            _ => None,
        }
    }
    
    /// Convert to integer if this is an integer
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            RespValue::Integer(i) => Some(*i),
            _ => None,
        }
    }
    
    /// Convert to array if this is an array
    pub fn as_array(&self) -> Option<&Vec<RespValue>> {
        match self {
            RespValue::Array(Some(arr)) => Some(arr),
            _ => None,
        }
    }
    
    /// Get the type name as a string
    pub fn type_name(&self) -> &'static str {
        match self {
            RespValue::SimpleString(_) => "simple_string",
            RespValue::Error(_) => "error",
            RespValue::Integer(_) => "integer",
            RespValue::BulkString(None) => "null_bulk_string",
            RespValue::BulkString(Some(_)) => "bulk_string",
            RespValue::Array(None) => "null_array",
            RespValue::Array(Some(_)) => "array",
        }
    }
    
    /// Estimate memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        match self {
            RespValue::SimpleString(s) => std::mem::size_of::<String>() + s.len(),
            RespValue::Error(s) => std::mem::size_of::<String>() + s.len(),
            RespValue::Integer(_) => std::mem::size_of::<i64>(),
            RespValue::BulkString(None) => std::mem::size_of::<Option<Bytes>>(),
            RespValue::BulkString(Some(bytes)) => {
                std::mem::size_of::<Option<Bytes>>() + bytes.len()
            }
            RespValue::Array(None) => std::mem::size_of::<Option<Vec<RespValue>>>(),
            RespValue::Array(Some(arr)) => {
                std::mem::size_of::<Option<Vec<RespValue>>>()
                    + arr.iter().map(|v| v.memory_usage()).sum::<usize>()
            }
        }
    }
}

/// RESP protocol parser
#[derive(Debug)]
pub struct RespParser {
    /// Maximum allowed depth for nested arrays
    max_depth: usize,
    /// Maximum allowed bulk string size
    max_bulk_size: usize,
    /// Maximum allowed array size
    max_array_size: usize,
    /// Whether to allow null values
    allow_null: bool,
}

impl Default for RespParser {
    fn default() -> Self {
        Self {
            max_depth: 32,
            max_bulk_size: 512 * 1024 * 1024, // 512MB
            max_array_size: 1024 * 1024,      // 1M elements
            allow_null: true,
        }
    }
}

impl RespParser {
    /// Create a new RESP parser with default settings
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Create a new RESP parser with custom limits
    pub fn with_limits(
        max_depth: usize,
        max_bulk_size: usize,
        max_array_size: usize,
    ) -> Self {
        Self {
            max_depth,
            max_bulk_size,
            max_array_size,
            allow_null: true,
        }
    }
    
    /// Set whether to allow null values
    pub fn allow_null(mut self, allow: bool) -> Self {
        self.allow_null = allow;
        self
    }
    
    /// Parse a RESP value from a buffered reader
    pub async fn parse_value<R: AsyncBufRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> Result<RespValue> {
        self.parse_value_with_depth(reader, 0).await
    }
    
    /// Parse a RESP value with depth tracking
    async fn parse_value_with_depth<R: AsyncBufRead + Unpin>(
        &self,
        reader: &mut R,
        depth: usize,
    ) -> Result<RespValue> {
        if depth > self.max_depth {
            return Err(RedisReplicatorError::protocol_error("protocol", "Maximum nesting depth exceeded".to_string(),
            ));
        }
        
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line).await.map_err(|e| {
            RedisReplicatorError::io_error(format!("Failed to read RESP line: {}", e))
        })?;
        
        if bytes_read == 0 {
            return Err(RedisReplicatorError::protocol_error("protocol", "Unexpected end of stream".to_string(),
            ));
        }
        
        // Remove CRLF
        if line.ends_with("\r\n") {
            line.truncate(line.len() - 2);
        } else if line.ends_with('\n') {
            line.truncate(line.len() - 1);
        }
        
        if line.is_empty() {
            return Err(RedisReplicatorError::protocol_error("data", "Empty RESP line".to_string(),
            ));
        }
        
        let type_byte = line.chars().next().unwrap();
        let content = &line[1..];
        
        match type_byte {
            '+' => Ok(RespValue::SimpleString(content.to_string())),
            '-' => Ok(RespValue::Error(content.to_string())),
            ':' => {
                let integer = content.parse::<i64>().map_err(|_| {
                    RedisReplicatorError::protocol_error(
                        "validation",
                        format!("Invalid integer: {}", content)
                    )
                })?;
                Ok(RespValue::Integer(integer))
            }
            '$' => self.parse_bulk_string(reader, content).await,
            '*' => self.parse_array(reader, content, depth).await,
            _ => Err(RedisReplicatorError::protocol_error(
                "protocol",
                format!("Unknown RESP type: {}", type_byte)
            )),
        }
    }
    
    /// Parse a bulk string
    async fn parse_bulk_string<R: AsyncBufRead + Unpin>(
        &self,
        reader: &mut R,
        length_str: &str,
    ) -> Result<RespValue> {
        let length = length_str.parse::<i64>().map_err(|_| {
            RedisReplicatorError::protocol_error(
                "validation",
                format!("Invalid bulk string length: {}", length_str)
            )
        })?;
        
        if length == -1 {
            if self.allow_null {
                return Ok(RespValue::BulkString(None));
            } else {
                return Err(RedisReplicatorError::protocol_error("protocol", "Null bulk strings not allowed".to_string(),
                ));
            }
        }
        
        if length < 0 {
            return Err(RedisReplicatorError::protocol_error(
                "validation",
                format!("Invalid bulk string length: {}", length)
            ));
        }
        
        let length = length as usize;
        if length > self.max_bulk_size {
            return Err(RedisReplicatorError::protocol_error(
                "validation",
                format!("Bulk string too large: {} > {}", length, self.max_bulk_size)
            ));
        }
        
        let mut buffer = vec![0u8; length];
        reader.read_exact(&mut buffer).await.map_err(|e| {
            RedisReplicatorError::io_error(format!(
                "Failed to read bulk string data: {}",
                e
            ))
        })?;
        
        // Read the trailing CRLF
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf).await.map_err(|e| {
            RedisReplicatorError::io_error(format!(
                "Failed to read bulk string CRLF: {}",
                e
            ))
        })?;
        
        if crlf != [b'\r', b'\n'] {
            tracing::warn!("Expected CRLF after bulk string, got: {:?}", crlf);
        }
        
        Ok(RespValue::BulkString(Some(Bytes::from(buffer))))
    }
    
    /// Parse an array
    async fn parse_array<R: AsyncBufRead + Unpin>(
        &self,
        reader: &mut R,
        length_str: &str,
        depth: usize,
    ) -> Result<RespValue> {
        let length = length_str.parse::<i64>().map_err(|_| {
            RedisReplicatorError::protocol_error(
                "validation",
                format!("Invalid array length: {}", length_str)
            )
        })?;
        
        if length == -1 {
            if self.allow_null {
                return Ok(RespValue::Array(None));
            } else {
                return Err(RedisReplicatorError::protocol_error(
                    "protocol",
                    "Null arrays not allowed".to_string()
                ));
            }
        }
        
        if length < 0 {
            return Err(RedisReplicatorError::protocol_error(
                "validation",
                format!("Invalid array length: {}", length)
            ));
        }
        
        let length = length as usize;
        if length > self.max_array_size {
            return Err(RedisReplicatorError::protocol_error(
                "validation",
                format!("Array too large: {} > {}", length, self.max_array_size)
            ));
        }
        
        let mut elements = Vec::with_capacity(length);
        for _ in 0..length {
            let element = Box::pin(self.parse_value_with_depth(reader, depth + 1)).await?;
            elements.push(element);
        }
        
        Ok(RespValue::Array(Some(elements)))
    }
    
    /// Parse multiple RESP values from a reader
    pub async fn parse_multiple<R: AsyncBufRead + Unpin>(
        &self,
        reader: &mut R,
        count: usize,
    ) -> Result<Vec<RespValue>> {
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let value = self.parse_value(reader).await?;
            values.push(value);
        }
        Ok(values)
    }
    
    /// Parse all RESP values from a reader until EOF
    pub async fn parse_all<R: AsyncBufRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> Result<Vec<RespValue>> {
        let mut values = Vec::new();
        
        loop {
            // Try to peek at the next byte to see if we're at EOF
            let _peek_buf = [0u8; 1];
            match reader.fill_buf().await {
                Ok(buf) if buf.is_empty() => break, // EOF
                Ok(_) => {
                    // More data available, parse next value
                    match self.parse_value(reader).await {
                        Ok(value) => values.push(value),
                        Err(e) => {
            tracing::debug!("Parse error: {}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    return Err(RedisReplicatorError::io_error(format!(
                        "Failed to check for more data: {}",
                        e
                    )));
                }
            }
        }
        
        Ok(values)
    }
}

/// Utility functions for RESP protocol
pub mod utils {
    use super::*;
    
    /// Convert a RESP array to a Redis command
    pub fn array_to_command(value: &RespValue) -> Result<Vec<Vec<u8>>> {
        match value {
            RespValue::Array(Some(elements)) => {
                let mut command = Vec::new();
                for element in elements {
                    match element {
                        RespValue::BulkString(Some(bytes)) => {
                            command.push(bytes.to_vec());
                        }
                        RespValue::SimpleString(s) => {
                            command.push(s.as_bytes().to_vec());
                        }
                        _ => {
                            return Err(RedisReplicatorError::protocol_error("protocol", "Command array must contain only strings".to_string(),
                            ));
                        }
                    }
                }
                Ok(command)
            }
            _ => Err(RedisReplicatorError::protocol_error("protocol", "Expected array for command".to_string(),
            )),
        }
    }
    
    /// Convert a Redis command to a RESP array
    pub fn command_to_array(command: &[Vec<u8>]) -> RespValue {
        let elements = command
            .iter()
            .map(|arg| RespValue::BulkString(Some(Bytes::from(arg.clone()))))
            .collect();
        RespValue::Array(Some(elements))
    }
    
    /// Serialize a RESP value to bytes
    pub fn serialize_value(value: &RespValue) -> Vec<u8> {
        match value {
            RespValue::SimpleString(s) => {
                format!("+{}\r\n", s).into_bytes()
            }
            RespValue::Error(s) => {
                format!("-{}\r\n", s).into_bytes()
            }
            RespValue::Integer(i) => {
                format!(":{}\r\n", i).into_bytes()
            }
            RespValue::BulkString(None) => {
                b"$-1\r\n".to_vec()
            }
            RespValue::BulkString(Some(bytes)) => {
                let mut result = format!("${}\r\n", bytes.len()).into_bytes();
                result.extend_from_slice(bytes);
                result.extend_from_slice(b"\r\n");
                result
            }
            RespValue::Array(None) => {
                b"*-1\r\n".to_vec()
            }
            RespValue::Array(Some(elements)) => {
                let mut result = format!("*{}\r\n", elements.len()).into_bytes();
                for element in elements {
                    result.extend_from_slice(&serialize_value(element));
                }
                result
            }
        }
    }
    
    /// Check if a RESP value represents a Redis command
    pub fn is_command(value: &RespValue) -> bool {
        match value {
            RespValue::Array(Some(elements)) => {
                !elements.is_empty() && elements.iter().all(|e| {
                    matches!(e, RespValue::BulkString(Some(_)) | RespValue::SimpleString(_))
                })
            }
            _ => false,
        }
    }
    
    /// Extract command name from a RESP value
    pub fn get_command_name(value: &RespValue) -> Option<String> {
        match value {
            RespValue::Array(Some(elements)) if !elements.is_empty() => {
                elements[0].as_string().map(|s| s.to_uppercase())
            }
            _ => None,
        }
    }
    
    /// Count the number of arguments in a command
    pub fn count_command_args(value: &RespValue) -> usize {
        match value {
            RespValue::Array(Some(elements)) => elements.len().saturating_sub(1),
            _ => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn test_parse_simple_string() {
        let data = b"+OK\r\n";
        let mut reader = BufReader::new(Cursor::new(data));
        let parser = RespParser::new();
        
        let value = parser.parse_value(&mut reader).await.unwrap();
        assert_eq!(value, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_parse_error() {
        let data = b"-ERR unknown command\r\n";
        let mut reader = BufReader::new(Cursor::new(data));
        let parser = RespParser::new();
        
        let value = parser.parse_value(&mut reader).await.unwrap();
        assert_eq!(value, RespValue::Error("ERR unknown command".to_string()));
    }

    #[tokio::test]
    async fn test_parse_integer() {
        let data = b":42\r\n";
        let mut reader = BufReader::new(Cursor::new(data));
        let parser = RespParser::new();
        
        let value = parser.parse_value(&mut reader).await.unwrap();
        assert_eq!(value, RespValue::Integer(42));
    }

    #[tokio::test]
    async fn test_parse_bulk_string() {
        let data = b"$5\r\nhello\r\n";
        let mut reader = BufReader::new(Cursor::new(data));
        let parser = RespParser::new();
        
        let value = parser.parse_value(&mut reader).await.unwrap();
        assert_eq!(value, RespValue::BulkString(Some(Bytes::from("hello"))));
    }

    #[tokio::test]
    async fn test_parse_null_bulk_string() {
        let data = b"$-1\r\n";
        let mut reader = BufReader::new(Cursor::new(data));
        let parser = RespParser::new();
        
        let value = parser.parse_value(&mut reader).await.unwrap();
        assert_eq!(value, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_parse_array() {
        let data = b"*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n";
        let mut reader = BufReader::new(Cursor::new(data));
        let parser = RespParser::new();
        
        let value = parser.parse_value(&mut reader).await.unwrap();
        match value {
            RespValue::Array(Some(elements)) => {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], RespValue::BulkString(Some(Bytes::from("SET"))));
                assert_eq!(elements[1], RespValue::BulkString(Some(Bytes::from("key"))));
            }
            _ => panic!("Expected array"),
        }
    }

    #[tokio::test]
    async fn test_parse_null_array() {
        let data = b"*-1\r\n";
        let mut reader = BufReader::new(Cursor::new(data));
        let parser = RespParser::new();
        
        let value = parser.parse_value(&mut reader).await.unwrap();
        assert_eq!(value, RespValue::Array(None));
    }

    #[tokio::test]
    async fn test_parse_nested_array() {
        let data = b"*2\r\n*2\r\n$3\r\nSET\r\n$3\r\nkey\r\n:42\r\n";
        let mut reader = BufReader::new(Cursor::new(data));
        let parser = RespParser::new();
        
        let value = parser.parse_value(&mut reader).await.unwrap();
        match value {
            RespValue::Array(Some(elements)) => {
                assert_eq!(elements.len(), 2);
                assert!(matches!(elements[0], RespValue::Array(Some(_))));
                assert_eq!(elements[1], RespValue::Integer(42));
            }
            _ => panic!("Expected array"),
        }
    }

    #[tokio::test]
    async fn test_parse_multiple() {
        let data = b"+OK\r\n:42\r\n$5\r\nhello\r\n";
        let mut reader = BufReader::new(Cursor::new(data));
        let parser = RespParser::new();
        
        let values = parser.parse_multiple(&mut reader, 3).await.unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], RespValue::SimpleString("OK".to_string()));
        assert_eq!(values[1], RespValue::Integer(42));
        assert_eq!(values[2], RespValue::BulkString(Some(Bytes::from("hello"))));
    }

    #[test]
    fn test_resp_value_methods() {
        let string_val = RespValue::SimpleString("hello".to_string());
        assert_eq!(string_val.as_string(), Some("hello".to_string()));
        assert_eq!(string_val.type_name(), "simple_string");
        
        let int_val = RespValue::Integer(42);
        assert_eq!(int_val.as_integer(), Some(42));
        assert_eq!(int_val.type_name(), "integer");
        
        let null_val = RespValue::BulkString(None);
        assert!(null_val.is_null());
        assert_eq!(null_val.type_name(), "null_bulk_string");
    }

    #[test]
    fn test_utils() {
        let command = vec![
            b"SET".to_vec(),
            b"key".to_vec(),
            b"value".to_vec(),
        ];
        
        let resp_array = utils::command_to_array(&command);
        assert!(utils::is_command(&resp_array));
        assert_eq!(utils::get_command_name(&resp_array), Some("SET".to_string()));
        assert_eq!(utils::count_command_args(&resp_array), 2);
        
        let converted_command = utils::array_to_command(&resp_array).unwrap();
        assert_eq!(converted_command, command);
    }

    #[test]
    fn test_serialization() {
        let value = RespValue::SimpleString("OK".to_string());
        let serialized = utils::serialize_value(&value);
        assert_eq!(serialized, b"+OK\r\n");
        
        let value = RespValue::Integer(42);
        let serialized = utils::serialize_value(&value);
        assert_eq!(serialized, b":42\r\n");
        
        let value = RespValue::BulkString(Some(Bytes::from("hello")));
        let serialized = utils::serialize_value(&value);
        assert_eq!(serialized, b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_memory_usage() {
        let simple = RespValue::SimpleString("hello".to_string());
        assert!(simple.memory_usage() > 5);
        
        let bulk = RespValue::BulkString(Some(Bytes::from("hello")));
        assert!(bulk.memory_usage() > 5);
        
        let array = RespValue::Array(Some(vec![simple, bulk]));
        assert!(array.memory_usage() > 10);
    }
}