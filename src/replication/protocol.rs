//! Redis replication protocol implementation
//!
//! This module implements the Redis replication protocol for communicating
//! with a Redis master server during replication.

use crate::error::{RedisReplicatorError, Result};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::fmt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Redis replication commands
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationCommand {
    /// PING command
    Ping,
    /// AUTH command
    Auth { 
        /// Authentication password
        password: String 
    },
    /// REPLCONF command
    ReplConf { 
        /// Configuration key
        key: String, 
        /// Configuration value
        value: String 
    },
    /// PSYNC command
    PSync { 
        /// Replication ID
        repl_id: String, 
        /// Replication offset
        offset: i64 
    },
    /// Custom command
    Custom { 
        /// Command arguments
        args: Vec<String> 
    },
}

impl ReplicationCommand {
    /// Serialize command to RESP format
    pub fn to_resp(&self) -> Vec<u8> {
        match self {
            ReplicationCommand::Ping => {
                b"*1\r\n$4\r\nPING\r\n".to_vec()
            }
            ReplicationCommand::Auth { password } => {
                format!(
                    "*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n",
                    password.len(),
                    password
                )
                .into_bytes()
            }
            ReplicationCommand::ReplConf { key, value } => {
                format!(
                    "*3\r\n$8\r\nREPLCONF\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    key.len(),
                    key,
                    value.len(),
                    value
                )
                .into_bytes()
            }
            ReplicationCommand::PSync { repl_id, offset } => {
                format!(
                    "*3\r\n$5\r\nPSYNC\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    repl_id.len(),
                    repl_id,
                    offset.to_string().len(),
                    offset
                )
                .into_bytes()
            }
            ReplicationCommand::Custom { args } => {
                let mut result = format!("*{}\r\n", args.len()).into_bytes();
                for arg in args {
                    result.extend_from_slice(
                        format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes(),
                    );
                }
                result
            }
        }
    }
    
    /// Get command name
    pub fn name(&self) -> &'static str {
        match self {
            ReplicationCommand::Ping => "PING",
            ReplicationCommand::Auth { .. } => "AUTH",
            ReplicationCommand::ReplConf { .. } => "REPLCONF",
            ReplicationCommand::PSync { .. } => "PSYNC",
            ReplicationCommand::Custom { .. } => "CUSTOM",
        }
    }
}

impl fmt::Display for ReplicationCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplicationCommand::Ping => write!(f, "PING"),
            ReplicationCommand::Auth { .. } => write!(f, "AUTH ***"),
            ReplicationCommand::ReplConf { key, value } => {
                write!(f, "REPLCONF {} {}", key, value)
            }
            ReplicationCommand::PSync { repl_id, offset } => {
                write!(f, "PSYNC {} {}", repl_id, offset)
            }
            ReplicationCommand::Custom { args } => {
                write!(f, "CUSTOM {}", args.join(" "))
            }
        }
    }
}

/// Redis replication responses
#[derive(Debug, Clone, PartialEq)]
pub enum ReplicationResponse {
    /// Simple string response (+)
    Ok(String),
    /// Error response (-)
    Error(String),
    /// Integer response (:)
    Integer(i64),
    /// Bulk string response ($)
    BulkString(Option<Bytes>),
    /// Full resync response
    FullResync { 
        /// Replication ID
        repl_id: String, 
        /// Replication offset
        offset: i64 
    },
    /// Continue response
    Continue,
    /// RDB data
    RdbData(Bytes),
    /// Command data
    Command(Vec<Bytes>),
}

impl ReplicationResponse {
    /// Parse response from RESP data
    pub fn from_resp(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(RedisReplicatorError::protocol_error("data", "Empty response data".to_string(),
            ));
        }
        
        match data[0] {
            b'+' => {
                let content = std::str::from_utf8(&data[1..])
                    .map_err(|_| {
                        RedisReplicatorError::protocol_error("validation", "Invalid UTF-8 in simple string".to_string(),
                        )
                    })?
                    .trim_end_matches("\r\n");
                
                // Check for special responses
                if content.starts_with("FULLRESYNC ") {
                    let parts: Vec<&str> = content.split_whitespace().collect();
                    if parts.len() == 3 {
                        let repl_id = parts[1].to_string();
                        let offset = parts[2].parse::<i64>().map_err(|_| {
                            RedisReplicatorError::protocol_error("validation", "Invalid offset in FULLRESYNC".to_string(),
                            )
                        })?;
                        return Ok(ReplicationResponse::FullResync { repl_id, offset });
                    }
                }
                
                if content == "CONTINUE" {
                    Ok(ReplicationResponse::Continue)
                } else {
                    Ok(ReplicationResponse::Ok(content.to_string()))
                }
            }
            b'-' => {
                let content = std::str::from_utf8(&data[1..])
                    .map_err(|_| {
                        RedisReplicatorError::protocol_error("validation", "Invalid UTF-8 in error string".to_string(),
                        )
                    })?
                    .trim_end_matches("\r\n");
                Ok(ReplicationResponse::Error(content.to_string()))
            }
            b':' => {
                let content = std::str::from_utf8(&data[1..])
                    .map_err(|_| {
                        RedisReplicatorError::protocol_error("validation", "Invalid UTF-8 in integer".to_string(),
                        )
                    })?
                    .trim_end_matches("\r\n");
                let integer = content.parse::<i64>().map_err(|_| {
                    RedisReplicatorError::protocol_error("validation", "Invalid integer format".to_string(),
                    )
                })?;
                Ok(ReplicationResponse::Integer(integer))
            }
            b'$' => {
                // Parse bulk string length
                let newline_pos = data.iter().position(|&b| b == b'\n').ok_or_else(|| {
                    RedisReplicatorError::protocol_error("protocol", "No newline found in bulk string header".to_string(),
                    )
                })?;
                
                let length_str = std::str::from_utf8(&data[1..newline_pos - 1])
                    .map_err(|_| {
                        RedisReplicatorError::protocol_error("validation", "Invalid UTF-8 in bulk string length".to_string(),
                        )
                    })?;
                
                let length = length_str.parse::<i64>().map_err(|_| {
                    RedisReplicatorError::protocol_error("validation", "Invalid bulk string length".to_string(),
                    )
                })?;
                
                if length == -1 {
                    Ok(ReplicationResponse::BulkString(None))
                } else if length < 0 {
                    Err(RedisReplicatorError::protocol_error("validation", "Invalid bulk string length".to_string(),
                    ))
                } else {
                    let start = newline_pos + 1;
                    let end = start + length as usize;
                    if end + 2 > data.len() {
                        return Err(RedisReplicatorError::protocol_error("protocol", "Incomplete bulk string data".to_string(),
                        ));
                    }
                    let content = Bytes::copy_from_slice(&data[start..end]);
                    Ok(ReplicationResponse::BulkString(Some(content)))
                }
            }
            _ => Err(RedisReplicatorError::protocol_error(
                "protocol",
                format!("Unknown response type: {}", data[0] as char)
            )),
        }
    }
    
    /// Check if this is an error response
    pub fn is_error(&self) -> bool {
        matches!(self, ReplicationResponse::Error(_))
    }
    
    /// Get error message if this is an error
    pub fn error_message(&self) -> Option<&str> {
        match self {
            ReplicationResponse::Error(msg) => Some(msg),
            _ => None,
        }
    }
    
    /// Check if this indicates a full resync
    pub fn is_full_resync(&self) -> bool {
        matches!(self, ReplicationResponse::FullResync { .. })
    }
    
    /// Get replication info if this is a full resync response
    pub fn replication_info(&self) -> Option<(String, i64)> {
        match self {
            ReplicationResponse::FullResync { repl_id, offset } => {
                Some((repl_id.clone(), *offset))
            }
            _ => None,
        }
    }
}

/// Redis replication protocol handler
#[derive(Debug)]
pub struct ReplicationProtocol {
    /// Buffer for incoming data
    read_buffer: BytesMut,
    /// Buffer for outgoing data
    write_buffer: BytesMut,
    /// Maximum buffer size
    max_buffer_size: usize,
}

impl Default for ReplicationProtocol {
    fn default() -> Self {
        Self {
            read_buffer: BytesMut::with_capacity(8192),
            write_buffer: BytesMut::with_capacity(8192),
            max_buffer_size: 64 * 1024 * 1024, // 64MB
        }
    }
}

impl ReplicationProtocol {
    /// Create a new protocol handler
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Create a protocol handler with custom buffer size
    pub fn with_buffer_size(buffer_size: usize, max_buffer_size: usize) -> Self {
        Self {
            read_buffer: BytesMut::with_capacity(buffer_size),
            write_buffer: BytesMut::with_capacity(buffer_size),
            max_buffer_size,
        }
    }
    
    /// Send a command
    pub async fn send_command<W: AsyncWrite + Unpin>(
        &mut self,
        writer: &mut W,
        command: &ReplicationCommand,
    ) -> Result<()> {
        let data = command.to_resp();
        tracing::debug!("Sending command: {}", command);
        
        writer.write_all(&data).await.map_err(|e| {
            RedisReplicatorError::io_error(format!("Failed to send command: {}", e))
        })?;
        
        writer.flush().await.map_err(|e| {
            RedisReplicatorError::io_error(format!("Failed to flush command: {}", e))
        })?;
        
        Ok(())
    }
    
    /// Read a response
    pub async fn read_response<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
    ) -> Result<ReplicationResponse> {
        // Read until we have a complete response
        loop {
            // Check if we have a complete response in the buffer
            if let Some(response) = self.try_parse_response()? {
                return Ok(response);
            }
            
            // Check buffer size limit
            if self.read_buffer.len() > self.max_buffer_size {
                return Err(RedisReplicatorError::protocol_error("protocol", "Response too large".to_string(),
                ));
            }
            
            // Read more data
            let mut temp_buf = [0u8; 8192];
            let bytes_read = reader.read(&mut temp_buf).await.map_err(|e| {
                RedisReplicatorError::io_error(format!("Failed to read response: {}", e))
            })?;
            
            if bytes_read == 0 {
                return Err(RedisReplicatorError::protocol_error("protocol", "Unexpected end of stream".to_string(),
                ));
            }
            
            self.read_buffer.extend_from_slice(&temp_buf[..bytes_read]);
        }
    }
    
    /// Try to parse a complete response from the buffer
    fn try_parse_response(&mut self) -> Result<Option<ReplicationResponse>> {
        if self.read_buffer.is_empty() {
            return Ok(None);
        }
        
        let first_byte = self.read_buffer[0];
        
        match first_byte {
            b'+' | b'-' | b':' => {
                // Simple string, error, or integer - look for CRLF
                if let Some(pos) = self.find_crlf() {
                    let line_data = self.read_buffer.split_to(pos + 2);
                    let response = ReplicationResponse::from_resp(&line_data)?;
                    return Ok(Some(response));
                }
            }
            b'$' => {
                // Bulk string - need to parse length first
                if let Some(header_end) = self.find_crlf() {
                    let length_str = std::str::from_utf8(&self.read_buffer[1..header_end])
                        .map_err(|_| {
                            RedisReplicatorError::protocol_error(
                                "parse_bulk_string",
                                "Invalid UTF-8 in bulk string length"
                            )
                        })?;
                    
                    let length = length_str.parse::<i64>().map_err(|_| {
                        RedisReplicatorError::protocol_error(
                            "parse_bulk_string",
                            "Invalid bulk string length"
                        )
                    })?;
                    
                    if length == -1 {
                        // Null bulk string
                        let line_data = self.read_buffer.split_to(header_end + 2);
                        let response = ReplicationResponse::from_resp(&line_data)?;
                        return Ok(Some(response));
                    } else if length >= 0 {
                        let total_length = header_end + 2 + length as usize + 2; // header + CRLF + data + CRLF
                        if self.read_buffer.len() >= total_length {
                            let response_data = self.read_buffer.split_to(total_length);
                            let response = ReplicationResponse::from_resp(&response_data)?;
                            return Ok(Some(response));
                        }
                    } else {
                        return Err(RedisReplicatorError::protocol_error(
                            "parse_response",
                            "Invalid bulk string length"
                        ));
                    }
                }
            }
            b'*' => {
                // Array - more complex parsing needed
                return Err(RedisReplicatorError::protocol_error(
                    "parse_response",
                    "Array responses not supported in replication protocol"
                ));
            }
            _ => {
                return Err(RedisReplicatorError::protocol_error(
                    "parse_response",
                    format!("Unknown response type: {}", first_byte as char)
                ));
            }
        }
        
        Ok(None)
    }
    
    /// Find CRLF in the buffer
    fn find_crlf(&self) -> Option<usize> {
        self.read_buffer
            .windows(2)
            .position(|window| window == b"\r\n")
    }
    
    /// Read raw data (for RDB transfer)
    pub async fn read_raw_data<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
        length: usize,
    ) -> Result<Bytes> {
        let mut data = BytesMut::with_capacity(length);
        
        // First, use any data already in the buffer
        let available = std::cmp::min(self.read_buffer.len(), length);
        if available > 0 {
            data.extend_from_slice(&self.read_buffer.split_to(available));
        }
        
        // Read remaining data
        while data.len() < length {
            let remaining = length - data.len();
            let mut temp_buf = vec![0u8; std::cmp::min(remaining, 8192)];
            
            let bytes_read = reader.read(&mut temp_buf).await.map_err(|e| {
                RedisReplicatorError::io_error(format!("Failed to read raw data: {}", e))
            })?;
            
            if bytes_read == 0 {
                return Err(RedisReplicatorError::protocol_error("protocol", "Unexpected end of stream while reading raw data".to_string(),
                ));
            }
            
            data.extend_from_slice(&temp_buf[..bytes_read]);
        }
        
        Ok(data.freeze())
    }
    
    /// Clear buffers
    pub fn clear_buffers(&mut self) {
        self.read_buffer.clear();
        self.write_buffer.clear();
    }
    
    /// Get buffer sizes
    pub fn buffer_sizes(&self) -> (usize, usize) {
        (self.read_buffer.len(), self.write_buffer.len())
    }
}

/// Utility functions for replication protocol
pub mod utils {
    use super::*;
    
    /// Create PING command
    pub fn ping() -> ReplicationCommand {
        ReplicationCommand::Ping
    }
    
    /// Create AUTH command
    pub fn auth(password: &str) -> ReplicationCommand {
        ReplicationCommand::Auth {
            password: password.to_string(),
        }
    }
    
    /// Create REPLCONF command
    pub fn replconf(key: &str, value: &str) -> ReplicationCommand {
        ReplicationCommand::ReplConf {
            key: key.to_string(),
            value: value.to_string(),
        }
    }
    
    /// Create PSYNC command
    pub fn psync(repl_id: &str, offset: i64) -> ReplicationCommand {
        ReplicationCommand::PSync {
            repl_id: repl_id.to_string(),
            offset,
        }
    }
    
    /// Create custom command
    pub fn custom(args: Vec<&str>) -> ReplicationCommand {
        ReplicationCommand::Custom {
            args: args.into_iter().map(|s| s.to_string()).collect(),
        }
    }
    
    /// Parse replication info from INFO response
    pub fn parse_info_replication(info: &str) -> HashMap<String, String> {
        let mut result = HashMap::new();
        
        for line in info.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            
            if let Some(pos) = line.find(':') {
                let key = line[..pos].to_string();
                let value = line[pos + 1..].to_string();
                result.insert(key, value);
            }
        }
        
        result
    }
    
    /// Check if response indicates authentication required
    pub fn is_auth_required(response: &ReplicationResponse) -> bool {
        match response {
            ReplicationResponse::Error(msg) => {
                msg.contains("NOAUTH") || msg.contains("authentication required")
            }
            _ => false,
        }
    }
    
    /// Check if response indicates authentication failed
    pub fn is_auth_failed(response: &ReplicationResponse) -> bool {
        match response {
            ReplicationResponse::Error(msg) => {
                msg.contains("invalid password") || msg.contains("ERR AUTH")
            }
            _ => false,
        }
    }
    
    /// Extract RDB size from FULLRESYNC response
    pub fn extract_rdb_size(_response: &ReplicationResponse) -> Option<usize> {
        // RDB size is typically sent as a bulk string length after FULLRESYNC
        // This is implementation-specific and may vary
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    

    #[test]
    fn test_command_serialization() {
        let ping = ReplicationCommand::Ping;
        assert_eq!(ping.to_resp(), b"*1\r\n$4\r\nPING\r\n");
        
        let auth = ReplicationCommand::Auth {
            password: "secret".to_string(),
        };
        assert_eq!(auth.to_resp(), b"*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n");
        
        let replconf = ReplicationCommand::ReplConf {
            key: "listening-port".to_string(),
            value: "6380".to_string(),
        };
        let expected = b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
        assert_eq!(replconf.to_resp(), expected);
    }

    #[test]
    fn test_response_parsing() {
        // Simple string
        let data = b"+OK\r\n";
        let response = ReplicationResponse::from_resp(data).unwrap();
        assert_eq!(response, ReplicationResponse::Ok("OK".to_string()));
        
        // Error
        let data = b"-ERR unknown command\r\n";
        let response = ReplicationResponse::from_resp(data).unwrap();
        assert_eq!(
            response,
            ReplicationResponse::Error("ERR unknown command".to_string())
        );
        
        // Integer
        let data = b":42\r\n";
        let response = ReplicationResponse::from_resp(data).unwrap();
        assert_eq!(response, ReplicationResponse::Integer(42));
        
        // Bulk string
        let data = b"$5\r\nhello\r\n";
        let response = ReplicationResponse::from_resp(data).unwrap();
        assert_eq!(
            response,
            ReplicationResponse::BulkString(Some(Bytes::from("hello")))
        );
        
        // Null bulk string
        let data = b"$-1\r\n";
        let response = ReplicationResponse::from_resp(data).unwrap();
        assert_eq!(response, ReplicationResponse::BulkString(None));
        
        // FULLRESYNC
        let data = b"+FULLRESYNC abc123 1000\r\n";
        let response = ReplicationResponse::from_resp(data).unwrap();
        assert_eq!(
            response,
            ReplicationResponse::FullResync {
                repl_id: "abc123".to_string(),
                offset: 1000
            }
        );
    }

    #[test]
    fn test_response_methods() {
        let ok_response = ReplicationResponse::Ok("OK".to_string());
        assert!(!ok_response.is_error());
        assert!(ok_response.error_message().is_none());
        
        let error_response = ReplicationResponse::Error("ERR test".to_string());
        assert!(error_response.is_error());
        assert_eq!(error_response.error_message(), Some("ERR test"));
        
        let fullresync = ReplicationResponse::FullResync {
            repl_id: "abc123".to_string(),
            offset: 1000,
        };
        assert!(fullresync.is_full_resync());
        assert_eq!(
            fullresync.replication_info(),
            Some(("abc123".to_string(), 1000))
        );
    }

    #[tokio::test]
    async fn test_protocol_handler() {
        let mut protocol = ReplicationProtocol::new();
        
        // Test sending command
        let mut output = Vec::new();
        let ping = ReplicationCommand::Ping;
        protocol.send_command(&mut output, &ping).await.unwrap();
        assert_eq!(output, b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn test_utils() {
        let ping = utils::ping();
        assert_eq!(ping, ReplicationCommand::Ping);
        
        let auth = utils::auth("secret");
        assert_eq!(
            auth,
            ReplicationCommand::Auth {
                password: "secret".to_string()
            }
        );
        
        let replconf = utils::replconf("listening-port", "6380");
        assert_eq!(
            replconf,
            ReplicationCommand::ReplConf {
                key: "listening-port".to_string(),
                value: "6380".to_string()
            }
        );
        
        let psync = utils::psync("abc123", 1000);
        assert_eq!(
            psync,
            ReplicationCommand::PSync {
                repl_id: "abc123".to_string(),
                offset: 1000
            }
        );
    }

    #[test]
    fn test_info_parsing() {
        let info = "# Replication\r\nrole:master\r\nconnected_slaves:1\r\nmaster_repl_offset:1000\r\n";
        let parsed = utils::parse_info_replication(info);
        
        assert_eq!(parsed.get("role"), Some(&"master".to_string()));
        assert_eq!(parsed.get("connected_slaves"), Some(&"1".to_string()));
        assert_eq!(parsed.get("master_repl_offset"), Some(&"1000".to_string()));
    }

    #[test]
    fn test_auth_detection() {
        let auth_required = ReplicationResponse::Error("NOAUTH Authentication required".to_string());
        assert!(utils::is_auth_required(&auth_required));
        
        let auth_failed = ReplicationResponse::Error("ERR AUTH invalid password".to_string());
        assert!(utils::is_auth_failed(&auth_failed));
        
        let ok_response = ReplicationResponse::Ok("OK".to_string());
        assert!(!utils::is_auth_required(&ok_response));
        assert!(!utils::is_auth_failed(&ok_response));
    }
}