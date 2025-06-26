//! Error types for the Redis Replicator library

use std::fmt;
use thiserror::Error;

/// Result type alias for the library
pub type Result<T> = std::result::Result<T, RedisReplicatorError>;

/// Main error type for the Redis Replicator library
#[derive(Error, Debug)]
pub enum RedisReplicatorError {
    /// Parse errors with context
    #[error("Parse error at offset {offset}: {message}")]
    Parse { 
        /// Error offset
        offset: u64, 
        /// Error message
        message: String 
    },

    /// Protocol errors
    #[error("Protocol error in {context}: {message}")]
    Protocol { 
        /// Error context
        context: String, 
        /// Error message
        message: String 
    },

    /// Connection errors
    #[error("Connection error: {0}")]
    Connection(String),

    /// Authentication errors
    #[error("Authentication failed: {0}")]
    Auth(String),

    /// IO errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// UTF-8 conversion errors
    #[error("UTF-8 conversion error: {0}")]
    Utf8(#[from] std::str::Utf8Error),

    /// Integer parsing errors
    #[error("Integer parsing error: {0}")]
    ParseInt(#[from] std::num::ParseIntError),

    /// Float parsing errors
    #[error("Float parsing error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),

    /// Compression/decompression errors
    #[error("Compression error: {0}")]
    Compression(String),

    /// Timeout errors
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Resource exhaustion errors
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    /// Invalid data format
    #[error("Invalid data format: {0}")]
    InvalidFormat(String),

    /// Unsupported feature
    #[error("Unsupported feature: {0}")]
    Unsupported(String),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// Generic errors
    #[error("Error: {0}")]
    Other(String),
}

impl Clone for RedisReplicatorError {
    fn clone(&self) -> Self {
        match self {
            Self::Parse { offset, message } => Self::Parse {
                offset: *offset,
                message: message.clone(),
            },
            Self::Protocol { context, message } => Self::Protocol {
                context: context.clone(),
                message: message.clone(),
            },
            Self::Connection(msg) => Self::Connection(msg.clone()),
            Self::Auth(msg) => Self::Auth(msg.clone()),
            Self::Io(err) => Self::Io(std::io::Error::new(err.kind(), err.to_string())),
            Self::Utf8(err) => Self::Utf8(*err),
            Self::ParseInt(err) => Self::ParseInt(err.clone()),
            Self::ParseFloat(err) => Self::ParseFloat(err.clone()),
            Self::Compression(msg) => Self::Compression(msg.clone()),
            Self::Timeout(msg) => Self::Timeout(msg.clone()),
            Self::ResourceExhausted(msg) => Self::ResourceExhausted(msg.clone()),
            Self::InvalidFormat(msg) => Self::InvalidFormat(msg.clone()),
            Self::Unsupported(msg) => Self::Unsupported(msg.clone()),
            Self::Config(msg) => Self::Config(msg.clone()),
            Self::Other(msg) => Self::Other(msg.clone()),
        }
    }
}

impl RedisReplicatorError {
    /// Create a parse error with offset
    pub fn parse_error(offset: u64, message: impl Into<String>) -> Self {
        Self::Parse {
            offset,
            message: message.into(),
        }
    }

    /// Create a parse error with offset and source
    pub fn parse_error_with_source(offset: u64, message: impl Into<String>) -> Self {
        Self::Parse {
            offset,
            message: message.into(),
        }
    }

    /// Create a protocol error
    pub fn protocol_error(context: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Protocol {
            context: context.into(),
            message: message.into(),
        }
    }

    /// Create a connection error
    pub fn connection_error(message: impl Into<String>) -> Self {
        Self::Connection(message.into())
    }

    /// Create an authentication error
    pub fn auth_error(message: impl Into<String>) -> Self {
        Self::Auth(message.into())
    }

    /// Create an authentication error (alias)
    pub fn authentication_error(message: impl Into<String>) -> Self {
        Self::Auth(message.into())
    }

    /// Create an IO error
    pub fn io_error(message: impl Into<String>) -> Self {
        Self::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            message.into(),
        ))
    }

    /// Create a compression error
    pub fn compression_error(message: impl Into<String>) -> Self {
        Self::Compression(message.into())
    }

    /// Create a timeout error
    pub fn timeout_error(message: impl Into<String>) -> Self {
        Self::Timeout(message.into())
    }

    /// Create a resource exhausted error
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::ResourceExhausted(message.into())
    }

    /// Create an invalid format error
    pub fn invalid_format(message: impl Into<String>) -> Self {
        Self::InvalidFormat(message.into())
    }

    /// Create an unsupported feature error
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported(message.into())
    }

    /// Create a configuration error
    pub fn config_error(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    /// Create a generic error
    pub fn other(message: impl Into<String>) -> Self {
        Self::Other(message.into())
    }

    /// Create a replication error
    pub fn replication_error(message: impl Into<String>) -> Self {
        Self::Other(format!("Replication: {}", message.into()))
    }



    /// Create an RDB error
    pub fn rdb_error(message: impl Into<String>) -> Self {
        Self::InvalidFormat(format!("RDB: {}", message.into()))
    }

    /// Create an AOF error
    pub fn aof_error(message: impl Into<String>) -> Self {
        Self::InvalidFormat(format!("AOF: {}", message.into()))
    }

    /// Create a configuration error
    pub fn configuration_error(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    /// Create a resource error
    pub fn resource_error(message: impl Into<String>) -> Self {
        Self::ResourceExhausted(message.into())
    }

    /// Check if this is a recoverable error
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::Parse { .. } => true,
            Self::Protocol { .. } => false,
            Self::Connection(_) => true,
            Self::Auth(_) => false,
            Self::Io(_) => true,
            Self::Utf8(_) => true,
            Self::ParseInt(_) => true,
            Self::ParseFloat(_) => true,
            Self::Compression(_) => true,
            Self::Timeout(_) => true,
            Self::ResourceExhausted(_) => true,
            Self::InvalidFormat(_) => true,
            Self::Unsupported(_) => false,
            Self::Config(_) => false,
            Self::Other(_) => false,
        }
    }

    /// Get the error category
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Parse { .. } => ErrorCategory::Parse,
            Self::Protocol { .. } => ErrorCategory::Protocol,
            Self::Connection(_) => ErrorCategory::Network,
            Self::Auth(_) => ErrorCategory::Auth,
            Self::Io(_) => ErrorCategory::Io,
            Self::Utf8(_) | Self::ParseInt(_) | Self::ParseFloat(_) => ErrorCategory::Parse,
            Self::Compression(_) => ErrorCategory::Compression,
            Self::Timeout(_) => ErrorCategory::Network,
            Self::ResourceExhausted(_) => ErrorCategory::Resource,
            Self::InvalidFormat(_) => ErrorCategory::Parse,
            Self::Unsupported(_) => ErrorCategory::Unsupported,
            Self::Config(_) => ErrorCategory::Config,
            Self::Other(_) => ErrorCategory::Other,
        }
    }
}

/// Error categories for classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    /// Parse error
    Parse,
    /// Protocol error
    Protocol,
    /// Network error
    Network,
    /// Authentication error
    Auth,
    /// I/O error
    Io,
    /// Compression error
    Compression,
    /// Resource error
    Resource,
    /// Unsupported operation
    Unsupported,
    /// Configuration error
    Config,
    /// Other error
    Other,
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse => write!(f, "parse"),
            Self::Protocol => write!(f, "protocol"),
            Self::Network => write!(f, "network"),
            Self::Auth => write!(f, "auth"),
            Self::Io => write!(f, "io"),
            Self::Compression => write!(f, "compression"),
            Self::Resource => write!(f, "resource"),
            Self::Unsupported => write!(f, "unsupported"),
            Self::Config => write!(f, "config"),
            Self::Other => write!(f, "other"),
        }
    }
}

/// Error builder for constructing complex errors
pub struct ErrorBuilder {
    kind: ErrorKind,
    context: Vec<String>,
    offset: Option<u64>,
}

#[derive(Debug)]
#[allow(dead_code)]
enum ErrorKind {
    Parse,
    Protocol,
    Connection,
    Auth,
    Compression,
    Timeout,
    ResourceExhausted,
    InvalidFormat,
    Unsupported,
    Config,
    Other,
}

impl ErrorBuilder {
    /// Create a parse error builder
    pub fn parse() -> Self {
        Self {
            kind: ErrorKind::Parse,
            context: Vec::new(),
            offset: None,
        }
    }

    /// Create a protocol error builder
    pub fn protocol() -> Self {
        Self {
            kind: ErrorKind::Protocol,
            context: Vec::new(),
            offset: None,
        }
    }

    /// Create a connection error builder
    pub fn connection() -> Self {
        Self {
            kind: ErrorKind::Connection,
            context: Vec::new(),
            offset: None,
        }
    }

    /// Add context to the error
    pub fn with_context(mut self, ctx: impl Into<String>) -> Self {
        self.context.push(ctx.into());
        self
    }

    /// Add offset to the error
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Build the error with a message
    pub fn build(self, message: impl Into<String>) -> RedisReplicatorError {
        let message = if self.context.is_empty() {
            message.into()
        } else {
            format!("{}: {}", self.context.join(" -> "), message.into())
        };

        match self.kind {
            ErrorKind::Parse => RedisReplicatorError::Parse {
                offset: self.offset.unwrap_or(0),
                message,
            },
            ErrorKind::Protocol => RedisReplicatorError::Protocol {
                context: self.context.join(" -> "),
                message,
            },
            ErrorKind::Connection => RedisReplicatorError::Connection(message),
            ErrorKind::Auth => RedisReplicatorError::Auth(message),
            ErrorKind::Compression => RedisReplicatorError::Compression(message),
            ErrorKind::Timeout => RedisReplicatorError::Timeout(message),
            ErrorKind::ResourceExhausted => RedisReplicatorError::ResourceExhausted(message),
            ErrorKind::InvalidFormat => RedisReplicatorError::InvalidFormat(message),
            ErrorKind::Unsupported => RedisReplicatorError::Unsupported(message),
            ErrorKind::Config => RedisReplicatorError::Config(message),
            ErrorKind::Other => RedisReplicatorError::Other(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = RedisReplicatorError::parse_error(100, "Invalid opcode");
        assert!(matches!(
            err,
            RedisReplicatorError::Parse { offset: 100, .. }
        ));
    }

    #[test]
    fn test_error_builder() {
        let err = ErrorBuilder::parse()
            .with_context("RDB parsing")
            .with_offset(42)
            .build("Invalid data");

        assert!(matches!(
            err,
            RedisReplicatorError::Parse { offset: 42, .. }
        ));
    }

    #[test]
    fn test_error_category() {
        let err = RedisReplicatorError::parse_error(0, "test");
        assert_eq!(err.category(), ErrorCategory::Parse);
    }

    #[test]
    fn test_recoverable() {
        let parse_err = RedisReplicatorError::parse_error(0, "test");
        assert!(parse_err.is_recoverable());

        let auth_err = RedisReplicatorError::auth_error("test");
        assert!(!auth_err.is_recoverable());
    }
}
