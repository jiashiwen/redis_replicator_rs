//! RDB type definitions and constants

use crate::error::{RedisReplicatorError, Result};
use std::fmt;

/// RDB file format version constants
pub mod version {
    /// Minimum supported RDB version
    pub const RDB_VERSION_MIN: u32 = 1;
    /// Maximum supported RDB version
    pub const RDB_VERSION_MAX: u32 = 11;
    /// Current RDB version
    pub const RDB_VERSION_CURRENT: u32 = 9;
}

/// RDB opcodes for different data types and operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RdbOpcode {
    /// String encoding
    String = 0,
    /// List encoding
    List = 1,
    /// Set encoding
    Set = 2,
    /// Sorted set encoding
    ZSet = 3,
    /// Hash encoding
    Hash = 4,
    /// Zipmap encoding (deprecated)
    Zipmap = 9,
    /// Ziplist encoding
    Ziplist = 10,
    /// Intset encoding
    Intset = 11,
    /// Sorted set in ziplist format
    ZSetZiplist = 12,
    /// Hash in ziplist format
    HashZiplist = 13,
    /// List in quicklist format
    ListQuicklist = 14,
    /// Stream encoding
    Stream = 15,
    /// Module data
    Module = 6,
    /// Module data (version 2)
    Module2 = 7,
    
    // Special opcodes
    /// Expire time in seconds
    ExpireTime = 253,
    /// Expire time in milliseconds
    ExpireTimeMs = 252,
    /// Database selector
    SelectDb = 254,
    /// End of file
    Eof = 255,
    /// Auxiliary field
    Aux = 250,
    /// Resize database hint
    ResizeDb = 251,
}

impl TryFrom<u8> for RdbOpcode {
    type Error = RedisReplicatorError;
    
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(RdbOpcode::String),
            1 => Ok(RdbOpcode::List),
            2 => Ok(RdbOpcode::Set),
            3 => Ok(RdbOpcode::ZSet),
            4 => Ok(RdbOpcode::Hash),
            6 => Ok(RdbOpcode::Module),
            7 => Ok(RdbOpcode::Module2),
            9 => Ok(RdbOpcode::Zipmap),
            10 => Ok(RdbOpcode::Ziplist),
            11 => Ok(RdbOpcode::Intset),
            12 => Ok(RdbOpcode::ZSetZiplist),
            13 => Ok(RdbOpcode::HashZiplist),
            14 => Ok(RdbOpcode::ListQuicklist),
            15 => Ok(RdbOpcode::Stream),
            250 => Ok(RdbOpcode::Aux),
            251 => Ok(RdbOpcode::ResizeDb),
            252 => Ok(RdbOpcode::ExpireTimeMs),
            253 => Ok(RdbOpcode::ExpireTime),
            254 => Ok(RdbOpcode::SelectDb),
            255 => Ok(RdbOpcode::Eof),
            _ => Err(RedisReplicatorError::parse_error(
                0,
                format!("Unknown RDB opcode: {}", value),
            )),
        }
    }
}

impl fmt::Display for RdbOpcode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdbOpcode::String => write!(f, "STRING"),
            RdbOpcode::List => write!(f, "LIST"),
            RdbOpcode::Set => write!(f, "SET"),
            RdbOpcode::ZSet => write!(f, "ZSET"),
            RdbOpcode::Hash => write!(f, "HASH"),
            RdbOpcode::Module => write!(f, "MODULE"),
            RdbOpcode::Module2 => write!(f, "MODULE2"),
            RdbOpcode::Zipmap => write!(f, "ZIPMAP"),
            RdbOpcode::Ziplist => write!(f, "ZIPLIST"),
            RdbOpcode::Intset => write!(f, "INTSET"),
            RdbOpcode::ZSetZiplist => write!(f, "ZSET_ZIPLIST"),
            RdbOpcode::HashZiplist => write!(f, "HASH_ZIPLIST"),
            RdbOpcode::ListQuicklist => write!(f, "LIST_QUICKLIST"),
            RdbOpcode::Stream => write!(f, "STREAM"),
            RdbOpcode::ExpireTime => write!(f, "EXPIRE_TIME"),
            RdbOpcode::ExpireTimeMs => write!(f, "EXPIRE_TIME_MS"),
            RdbOpcode::SelectDb => write!(f, "SELECT_DB"),
            RdbOpcode::Eof => write!(f, "EOF"),
            RdbOpcode::Aux => write!(f, "AUX"),
            RdbOpcode::ResizeDb => write!(f, "RESIZE_DB"),
        }
    }
}

/// RDB data type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RdbType {
    /// String data type
    String,
    /// List data type
    List,
    /// Set data type
    Set,
    /// Sorted set data type
    ZSet,
    /// Hash data type
    Hash,
    /// Stream data type
    Stream,
    /// Module data type
    Module,
    /// HyperLogLog data type
    HyperLogLog,
}

impl TryFrom<u8> for RdbType {
    type Error = RedisReplicatorError;
    
    fn try_from(opcode: u8) -> Result<Self> {
        match RdbOpcode::try_from(opcode)? {
            RdbOpcode::String => Ok(RdbType::String),
            RdbOpcode::List | RdbOpcode::Ziplist | RdbOpcode::ListQuicklist => Ok(RdbType::List),
            RdbOpcode::Set | RdbOpcode::Intset => Ok(RdbType::Set),
            RdbOpcode::ZSet | RdbOpcode::ZSetZiplist => Ok(RdbType::ZSet),
            RdbOpcode::Hash | RdbOpcode::Zipmap | RdbOpcode::HashZiplist => Ok(RdbType::Hash),
            RdbOpcode::Stream => Ok(RdbType::Stream),
            RdbOpcode::Module | RdbOpcode::Module2 => Ok(RdbType::Module),
            _ => Err(RedisReplicatorError::parse_error(
                0,
                format!("Cannot convert opcode {} to RdbType", opcode),
            )),
        }
    }
}

impl fmt::Display for RdbType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RdbType::String => write!(f, "string"),
            RdbType::List => write!(f, "list"),
            RdbType::Set => write!(f, "set"),
            RdbType::ZSet => write!(f, "zset"),
            RdbType::Hash => write!(f, "hash"),
            RdbType::Stream => write!(f, "stream"),
            RdbType::Module => write!(f, "module"),
            RdbType::HyperLogLog => write!(f, "hyperloglog"),
        }
    }
}

/// Length encoding types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LengthEncoding {
    /// 6-bit length (0-63)
    Len6Bit(u8),
    /// 14-bit length (0-16383)
    Len14Bit(u16),
    /// 32-bit length
    Len32Bit(u32),
    /// 64-bit length
    Len64Bit(u64),
    /// Special encoding
    Special(u8),
}

impl LengthEncoding {
    /// Get the actual length value
    pub fn value(&self) -> u64 {
        match self {
            LengthEncoding::Len6Bit(v) => *v as u64,
            LengthEncoding::Len14Bit(v) => *v as u64,
            LengthEncoding::Len32Bit(v) => *v as u64,
            LengthEncoding::Len64Bit(v) => *v,
            LengthEncoding::Special(v) => *v as u64,
        }
    }
    
    /// Check if this is a special encoding
    pub fn is_special(&self) -> bool {
        matches!(self, LengthEncoding::Special(_))
    }
}

/// String encoding types for special integer encodings
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringEncoding {
    /// Raw string data
    Raw,
    /// 8-bit signed integer
    Int8,
    /// 16-bit signed integer
    Int16,
    /// 32-bit signed integer
    Int32,
    /// LZF compressed string
    Lzf,
}

impl TryFrom<u8> for StringEncoding {
    type Error = RedisReplicatorError;
    
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(StringEncoding::Int8),
            1 => Ok(StringEncoding::Int16),
            2 => Ok(StringEncoding::Int32),
            3 => Ok(StringEncoding::Lzf),
            _ => Err(RedisReplicatorError::parse_error(
                0,
                format!("Unknown string encoding: {}", value),
            )),
        }
    }
}

/// RDB auxiliary field keys
pub mod aux_fields {
    /// Redis version
    pub const REDIS_VERSION: &str = "redis-ver";
    /// Redis architecture bits
    pub const REDIS_BITS: &str = "redis-bits";
    /// Creation time
    pub const CREATION_TIME: &str = "ctime";
    /// Used memory
    pub const USED_MEMORY: &str = "used-mem";
    /// AOF preamble flag
    pub const AOF_PREAMBLE: &str = "aof-preamble";
    /// Replication stream database
    pub const REPL_STREAM_DB: &str = "repl-stream-db";
    /// Replication ID
    pub const REPL_ID: &str = "repl-id";
    /// Replication offset
    pub const REPL_OFFSET: &str = "repl-offset";
}

/// Module type IDs
pub mod module_types {
    /// HyperLogLog module type
    pub const HYPERLOGLOG: u64 = 0x484c4c0000000000; // "HLL\0\0\0\0\0"
    /// RedisJSON module type
    pub const REJSON: u64 = 0x4a534f4e00000000;     // "JSON\0\0\0\0"
    /// RediSearch module type
    pub const SEARCH: u64 = 0x5345415243480000;     // "SEARCH\0\0"
    /// RedisTimeSeries module type
    pub const TIMESERIES: u64 = 0x5453444200000000;  // "TSDB\0\0\0\0"
    /// RedisBloom module type
    pub const BLOOM: u64 = 0x424c4f4f4d000000;      // "BLOOM\0\0\0"
}

/// Stream constants
pub mod stream {
    /// No flags
    pub const STREAM_ITEM_FLAG_NONE: u8 = 0;
    /// Item is deleted
    pub const STREAM_ITEM_FLAG_DELETED: u8 = 1;
    /// Item has same fields as previous
    pub const STREAM_ITEM_FLAG_SAMEFIELDS: u8 = 2;
}

/// Ziplist constants
pub mod ziplist {
    /// End of ziplist marker
    pub const ZIPLIST_END: u8 = 0xFF;
    /// Big previous length marker
    pub const ZIPLIST_BIG_PREVLEN: u8 = 0xFE;
    
    // Encoding constants
    /// String encoding mask
    pub const ZIP_STR_MASK: u8 = 0xC0;
    /// Integer encoding mask
    pub const ZIP_INT_MASK: u8 = 0x30;
    
    /// 6-bit string encoding
    pub const ZIP_STR_06B: u8 = 0x00;
    /// 14-bit string encoding
    pub const ZIP_STR_14B: u8 = 0x40;
    /// 32-bit string encoding
    pub const ZIP_STR_32B: u8 = 0x80;
    
    /// 16-bit integer encoding
    pub const ZIP_INT_16B: u8 = 0xC0;
    /// 32-bit integer encoding
    pub const ZIP_INT_32B: u8 = 0xD0;
    /// 64-bit integer encoding
    pub const ZIP_INT_64B: u8 = 0xE0;
    /// 24-bit integer encoding
    pub const ZIP_INT_24B: u8 = 0xF0;
    /// 8-bit integer encoding
    pub const ZIP_INT_8B: u8 = 0xFE;
    
    /// Immediate integer mask
    pub const ZIP_INT_IMM_MASK: u8 = 0x0F;
    /// Minimum immediate integer value
    pub const ZIP_INT_IMM_MIN: u8 = 0xF1;
    /// Maximum immediate integer value
    pub const ZIP_INT_IMM_MAX: u8 = 0xFD;
}

/// Intset constants
pub mod intset {
    /// 16-bit integer encoding
    pub const INTSET_ENC_INT16: u32 = 2;
    /// 32-bit integer encoding
    pub const INTSET_ENC_INT32: u32 = 4;
    /// 64-bit integer encoding
    pub const INTSET_ENC_INT64: u32 = 8;
}

/// Quicklist constants
pub mod quicklist {
    /// No container type
    pub const QUICKLIST_NODE_CONTAINER_NONE: u8 = 1;
    /// Ziplist container type
    pub const QUICKLIST_NODE_CONTAINER_ZIPLIST: u8 = 2;
}

/// RDB parsing context
#[derive(Debug, Clone)]
pub struct RdbContext {
    /// Current RDB version
    pub version: u32,
    /// Current database ID
    pub database_id: u32,
    /// Current position in the file
    pub position: u64,
    /// Whether checksums are enabled
    pub checksum_enabled: bool,
    /// Current CRC64 checksum
    pub checksum: u64,
}

impl RdbContext {
    /// Create a new RDB context
    pub fn new(version: u32) -> Self {
        Self {
            version,
            database_id: 0,
            position: 0,
            checksum_enabled: version >= 5,
            checksum: 0,
        }
    }
    
    /// Update the checksum with new data
    pub fn update_checksum(&mut self, data: &[u8]) {
        if self.checksum_enabled {
            self.checksum = crate::utils::crc64(data);
        }
    }
    
    /// Advance the position
    pub fn advance(&mut self, bytes: u64) {
        self.position += bytes;
    }
    
    /// Set the current database
    pub fn set_database(&mut self, db_id: u32) {
        self.database_id = db_id;
    }
}

/// RDB value size estimation
pub trait RdbSizeEstimate {
    /// Estimate the memory size of this value in bytes
    fn estimate_size(&self) -> usize;
}

impl RdbSizeEstimate for Vec<u8> {
    fn estimate_size(&self) -> usize {
        std::mem::size_of::<Vec<u8>>() + self.len()
    }
}

impl RdbSizeEstimate for String {
    fn estimate_size(&self) -> usize {
        std::mem::size_of::<String>() + self.len()
    }
}

impl<T: RdbSizeEstimate> RdbSizeEstimate for Vec<T> {
    fn estimate_size(&self) -> usize {
        std::mem::size_of::<Vec<T>>() + 
        self.iter().map(|item| item.estimate_size()).sum::<usize>()
    }
}

impl<K: RdbSizeEstimate, V: RdbSizeEstimate> RdbSizeEstimate for std::collections::HashMap<K, V> {
    fn estimate_size(&self) -> usize {
        std::mem::size_of::<std::collections::HashMap<K, V>>() +
        self.iter().map(|(k, v)| k.estimate_size() + v.estimate_size()).sum::<usize>()
    }
}

/// Utility functions for RDB types
pub mod utils {
    use super::*;
    
    /// Check if an opcode represents a value type
    pub fn is_value_type(opcode: u8) -> bool {
        matches!(opcode, 0..=15)
    }
    
    /// Check if an opcode represents an expiry
    pub fn is_expiry_type(opcode: u8) -> bool {
        matches!(opcode, 252 | 253)
    }
    
    /// Check if an opcode is a special command
    pub fn is_special_opcode(opcode: u8) -> bool {
        matches!(opcode, 250..=255)
    }
    
    /// Get the string representation of an opcode
    pub fn opcode_name(opcode: u8) -> &'static str {
        match RdbOpcode::try_from(opcode) {
            Ok(op) => match op {
                RdbOpcode::String => "STRING",
                RdbOpcode::List => "LIST",
                RdbOpcode::Set => "SET",
                RdbOpcode::ZSet => "ZSET",
                RdbOpcode::Hash => "HASH",
                RdbOpcode::Module => "MODULE",
                RdbOpcode::Module2 => "MODULE2",
                RdbOpcode::Zipmap => "ZIPMAP",
                RdbOpcode::Ziplist => "ZIPLIST",
                RdbOpcode::Intset => "INTSET",
                RdbOpcode::ZSetZiplist => "ZSET_ZIPLIST",
                RdbOpcode::HashZiplist => "HASH_ZIPLIST",
                RdbOpcode::ListQuicklist => "LIST_QUICKLIST",
                RdbOpcode::Stream => "STREAM",
                RdbOpcode::ExpireTime => "EXPIRE_TIME",
                RdbOpcode::ExpireTimeMs => "EXPIRE_TIME_MS",
                RdbOpcode::SelectDb => "SELECT_DB",
                RdbOpcode::Eof => "EOF",
                RdbOpcode::Aux => "AUX",
                RdbOpcode::ResizeDb => "RESIZE_DB",
            },
            Err(_) => "UNKNOWN",
        }
    }
    
    /// Check if a version supports a specific feature
    pub fn version_supports_feature(version: u32, feature: &str) -> bool {
        match feature {
            "checksum" => version >= 5,
            "aux_fields" => version >= 7,
            "modules" => version >= 8,
            "streams" => version >= 9,
            "expire_ms" => version >= 3,
            "ziplist" => version >= 2,
            "intset" => version >= 2,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdb_opcode_conversion() {
        assert_eq!(RdbOpcode::try_from(0).unwrap(), RdbOpcode::String);
        assert_eq!(RdbOpcode::try_from(1).unwrap(), RdbOpcode::List);
        assert_eq!(RdbOpcode::try_from(255).unwrap(), RdbOpcode::Eof);
        
        assert!(RdbOpcode::try_from(100).is_err());
    }

    #[test]
    fn test_rdb_type_conversion() {
        assert_eq!(RdbType::try_from(0).unwrap(), RdbType::String);
        assert_eq!(RdbType::try_from(1).unwrap(), RdbType::List);
        assert_eq!(RdbType::try_from(10).unwrap(), RdbType::List); // Ziplist -> List
        assert_eq!(RdbType::try_from(11).unwrap(), RdbType::Set);  // Intset -> Set
    }

    #[test]
    fn test_length_encoding() {
        let len = LengthEncoding::Len6Bit(42);
        assert_eq!(len.value(), 42);
        assert!(!len.is_special());
        
        let special = LengthEncoding::Special(3);
        assert_eq!(special.value(), 3);
        assert!(special.is_special());
    }

    #[test]
    fn test_string_encoding() {
        assert_eq!(StringEncoding::try_from(0).unwrap(), StringEncoding::Int8);
        assert_eq!(StringEncoding::try_from(1).unwrap(), StringEncoding::Int16);
        assert_eq!(StringEncoding::try_from(3).unwrap(), StringEncoding::Lzf);
        
        assert!(StringEncoding::try_from(10).is_err());
    }

    #[test]
    fn test_rdb_context() {
        let mut ctx = RdbContext::new(9);
        assert_eq!(ctx.version, 9);
        assert_eq!(ctx.database_id, 0);
        assert!(ctx.checksum_enabled);
        
        ctx.set_database(5);
        assert_eq!(ctx.database_id, 5);
        
        ctx.advance(100);
        assert_eq!(ctx.position, 100);
    }

    #[test]
    fn test_utils() {
        assert!(utils::is_value_type(0));
        assert!(utils::is_value_type(15));
        assert!(!utils::is_value_type(250));
        
        assert!(utils::is_expiry_type(252));
        assert!(utils::is_expiry_type(253));
        assert!(!utils::is_expiry_type(0));
        
        assert!(utils::is_special_opcode(255));
        assert!(!utils::is_special_opcode(0));
        
        assert_eq!(utils::opcode_name(0), "STRING");
        assert_eq!(utils::opcode_name(255), "EOF");
        
        assert!(utils::version_supports_feature(9, "streams"));
        assert!(!utils::version_supports_feature(8, "streams"));
    }

    #[test]
    fn test_size_estimation() {
        let vec = vec![1u8, 2, 3, 4, 5];
        let size = vec.estimate_size();
        assert!(size > 5); // Should include Vec overhead
        
        let string = "hello".to_string();
        let size = string.estimate_size();
        assert!(size > 5); // Should include String overhead
    }
}