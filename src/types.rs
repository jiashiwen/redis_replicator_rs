//! Redis data types and structures


use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Unified entry type for all Redis operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Entry {
    /// RDB entry
    Rdb(RdbEntry),
    /// AOF entry
    Aof(AofEntry),
    /// Replication entry
    Replication(ReplicationEntry),
}

/// RDB file entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RdbEntry {
    /// Database selection
    SelectDb { 
        /// Database number
        db: u32 
    },
    /// Key-value pair
    KeyValue {
        /// Key name
        key: String,
        /// Key value
        value: RedisValue,
        /// Expiration time in milliseconds
        expire_ms: Option<u64>,
        /// Idle time
        idle: Option<u64>,
        /// Access frequency
        freq: Option<u8>,
    },
    /// Function definition
    Function {
        /// Function name
        name: String,
        /// Script engine
        engine: String,
        /// Function code
        code: String,
    },
    /// Module auxiliary data
    ModuleAux {
        /// Module ID
        module_id: u64,
        /// Module data
        data: Vec<u8>,
    },
    /// Auxiliary field
    Aux {
        /// Field key
        key: String,
        /// Field value
        value: String,
    },
    /// Database resize hint
    ResizeDb {
        /// Database size
        db_size: u32,
        /// Expires size
        expires_size: u32,
    },
    /// End of file
    Eof {
        /// File checksum
        checksum: Option<u64>,
    },
}

/// AOF file entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AofEntry {
    /// Redis command
    Command {
        /// Command arguments
        args: Vec<String>,
        /// Command timestamp
        timestamp: Option<DateTime<Utc>>,
    },
    /// Multi command start
    Multi,
    /// Multi command end
    Exec,
    /// Database selection
    Select { 
        /// Database number
        db: u32 
    },
}

/// Replication stream entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicationEntry {
    /// RDB data
    Rdb(RdbEntry),
    /// AOF command
    Command {
        /// Command arguments
        args: Vec<String>,
        /// Command timestamp
        timestamp: Option<DateTime<Utc>>,
    },
    /// Ping command
    Ping,
    /// Replication offset update
    Offset { 
        /// Replication offset
        offset: u64 
    },
}

/// Redis value types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RedisValue {
    /// String value
    String(String),
    /// List value
    List(Vec<String>),
    /// Set value
    Set(Vec<String>),
    /// Hash value
    Hash(HashMap<String, String>),
    /// Sorted set value
    ZSet(Vec<ZSetEntry>),
    /// Stream value
    Stream(StreamValue),
    /// Module value
    Module(ModuleValue),
    /// HyperLogLog value
    HyperLogLog(Vec<u8>),
}

/// Sorted set entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ZSetEntry {
    /// Member value
    pub member: String,
    /// Score value
    pub score: f64,
}

/// Stream value structure
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamValue {
    /// Stream entries
    pub entries: Vec<StreamEntry>,
    /// Consumer groups
    pub groups: Vec<StreamGroup>,
    /// Stream length
    pub length: u64,
    /// Last entry ID
    pub last_id: StreamId,
    /// First entry ID
    pub first_id: StreamId,
    /// Maximum deleted entry ID
    pub max_deleted_id: StreamId,
    /// Number of entries added
    pub entries_added: u64,
}

/// Stream entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamEntry {
    /// Stream entry ID
    pub id: StreamId,
    /// Entry fields
    pub fields: HashMap<String, String>,
}

/// Stream ID
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StreamId {
    /// Milliseconds timestamp
    pub ms: u64,
    /// Sequence number
    pub seq: u64,
}

impl StreamId {
    /// Create a new StreamId
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }
    
    /// Parse StreamId from string format "ms-seq"
    pub fn from_string(s: &str) -> Result<Self, crate::error::RedisReplicatorError> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 2 {
            return Err(crate::error::RedisReplicatorError::invalid_format(
                format!("Invalid stream ID format: {}", s)
            ));
        }
        
        let ms = parts[0].parse::<u64>()?;
        let seq = parts[1].parse::<u64>()?;
        Ok(Self::new(ms, seq))
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// Stream consumer group
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamGroup {
    /// Group name
    pub name: String,
    /// Last delivered ID
    pub last_delivered_id: StreamId,
    /// Number of entries read
    pub entries_read: u64,
    /// Lag count
    pub lag: u64,
    /// List of consumers
    pub consumers: Vec<StreamConsumer>,
    /// Pending entries
    pub pending: Vec<StreamPendingEntry>,
}

/// Stream consumer
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamConsumer {
    /// Consumer name
    pub name: String,
    /// Last seen time
    pub seen_time: u64,
    /// Last active time
    pub active_time: u64,
    /// Number of pending messages
    pub pending_count: u64,
}

/// Stream pending entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamPendingEntry {
    /// Stream entry ID
    pub id: StreamId,
    /// Consumer name
    pub consumer: String,
    /// Delivery time
    pub delivery_time: u64,
    /// Delivery count
    pub delivery_count: u64,
}

/// Module value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModuleValue {
    /// Module ID
    pub module_id: u64,
    /// Module name
    pub module_name: String,
    /// Module data
    pub data: Vec<u8>,
    /// Module version
    pub version: u32,
}

/// RDB type constants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RdbType {
    /// String data type
    String = 0,
    /// List data type
    List = 1,
    /// Set data type
    Set = 2,
    /// Sorted set data type
    ZSet = 3,
    /// Hash data type
    Hash = 4,
    /// Sorted set data type version 2
    ZSet2 = 5,
    /// Module data type
    Module = 6,
    /// Module data type version 2
    Module2 = 7,
    /// Hash encoded as zipmap
    HashZipmap = 9,
    /// List encoded as ziplist
    ListZiplist = 10,
    /// Set encoded as intset
    SetIntset = 11,
    /// Sorted set encoded as ziplist
    ZSetZiplist = 12,
    /// Hash encoded as ziplist
    HashZiplist = 13,
    /// List encoded as quicklist
    ListQuicklist = 14,
    /// Stream encoded as listpacks
    StreamListpacks = 15,
    /// Hash encoded as listpack
    HashListpack = 16,
    /// Sorted set encoded as listpack
    ZSetListpack = 17,
    /// List encoded as quicklist version 2
    ListQuicklist2 = 18,
    /// Stream encoded as listpacks version 2
    StreamListpacks2 = 19,
    /// Set encoded as listpack
    SetListpack = 20,
    /// Stream with listpacks version 3
    StreamListpacks3 = 21,
}

impl TryFrom<u8> for RdbType {
    type Error = crate::error::RedisReplicatorError;
    
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RdbType::String),
            1 => Ok(RdbType::List),
            2 => Ok(RdbType::Set),
            3 => Ok(RdbType::ZSet),
            4 => Ok(RdbType::Hash),
            5 => Ok(RdbType::ZSet2),
            6 => Ok(RdbType::Module),
            7 => Ok(RdbType::Module2),
            9 => Ok(RdbType::HashZipmap),
            10 => Ok(RdbType::ListZiplist),
            11 => Ok(RdbType::SetIntset),
            12 => Ok(RdbType::ZSetZiplist),
            13 => Ok(RdbType::HashZiplist),
            14 => Ok(RdbType::ListQuicklist),
            15 => Ok(RdbType::StreamListpacks),
            16 => Ok(RdbType::HashListpack),
            17 => Ok(RdbType::ZSetListpack),
            18 => Ok(RdbType::ListQuicklist2),
            19 => Ok(RdbType::StreamListpacks2),
            20 => Ok(RdbType::SetListpack),
            21 => Ok(RdbType::StreamListpacks3),
            _ => Err(crate::error::RedisReplicatorError::invalid_format(
                format!("Unknown RDB type: {}", value)
            )),
        }
    }
}

/// RDB opcode constants
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RdbOpcode {
    /// Auxiliary field
    Aux = 0xFA,
    /// Resize database
    ResizeDb = 0xFB,
    /// Expire time in milliseconds
    ExpireMs = 0xFC,
    /// Expire time in seconds
    Expire = 0xFD,
    /// Select database
    SelectDb = 0xFE,
    /// End of file
    Eof = 0xFF,
    /// Slot information
    SlotInfo = 0xF0,
    /// Function definition
    Function = 0xF1,
    /// Function definition version 2
    Function2 = 0xF2,
    /// Module auxiliary data
    ModuleAux = 0xF7,
}

impl TryFrom<u8> for RdbOpcode {
    type Error = crate::error::RedisReplicatorError;
    
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0xFA => Ok(RdbOpcode::Aux),
            0xFB => Ok(RdbOpcode::ResizeDb),
            0xFC => Ok(RdbOpcode::ExpireMs),
            0xFD => Ok(RdbOpcode::Expire),
            0xFE => Ok(RdbOpcode::SelectDb),
            0xFF => Ok(RdbOpcode::Eof),
            0xF0 => Ok(RdbOpcode::SlotInfo),
            0xF1 => Ok(RdbOpcode::Function),
            0xF2 => Ok(RdbOpcode::Function2),
            0xF7 => Ok(RdbOpcode::ModuleAux),
            _ => Err(crate::error::RedisReplicatorError::invalid_format(
                format!("Unknown RDB opcode: 0x{:02X}", value)
            )),
        }
    }
}

/// Length encoding types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LengthEncoding {
    /// 6-bit length
    Len6Bit(u8),
    /// 14-bit length
    Len14Bit(u16),
    /// 32-bit length
    Len32Bit(u32),
    /// 64-bit length
    Len64Bit(u64),
    /// Special encoding
    Special(u8),
}

/// String encoding types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringEncoding {
    /// Raw string
    Raw,
    /// Integer encoding (8-bit)
    Int8,
    /// Integer encoding (16-bit)
    Int16,
    /// Integer encoding (32-bit)
    Int32,
    /// LZF compression
    Lzf,
}

impl RedisValue {
    /// Get the type name of the value
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Set(_) => "set",
            RedisValue::Hash(_) => "hash",
            RedisValue::ZSet(_) => "zset",
            RedisValue::Stream(_) => "stream",
            RedisValue::Module(_) => "module",
            RedisValue::HyperLogLog(_) => "hyperloglog",
        }
    }
    
    /// Get the approximate memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        match self {
            RedisValue::String(s) => s.len(),
            RedisValue::List(list) => list.iter().map(|s| s.len()).sum::<usize>() + list.len() * 8,
            RedisValue::Set(set) => set.iter().map(|s| s.len()).sum::<usize>() + set.len() * 8,
            RedisValue::Hash(hash) => {
                hash.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() + hash.len() * 16
            },
            RedisValue::ZSet(zset) => {
                zset.iter().map(|e| e.member.len()).sum::<usize>() + zset.len() * 16
            },
            RedisValue::Stream(stream) => {
                stream.entries.iter()
                    .map(|e| e.fields.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>())
                    .sum::<usize>() + stream.entries.len() * 32
            },
            RedisValue::Module(module) => module.data.len() + module.module_name.len(),
            RedisValue::HyperLogLog(data) => data.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id() {
        let id = StreamId::new(1234567890, 1);
        assert_eq!(id.to_string(), "1234567890-1");
        
        let parsed = StreamId::from_string("1234567890-1").unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_rdb_type_conversion() {
        assert_eq!(RdbType::try_from(0).unwrap(), RdbType::String);
        assert_eq!(RdbType::try_from(1).unwrap(), RdbType::List);
        assert!(RdbType::try_from(255).is_err());
    }

    #[test]
    fn test_redis_value_type_name() {
        assert_eq!(RedisValue::String("test".to_string()).type_name(), "string");
        assert_eq!(RedisValue::List(vec![]).type_name(), "list");
    }

    #[test]
    fn test_memory_usage() {
        let value = RedisValue::String("hello".to_string());
        assert_eq!(value.memory_usage(), 5);
        
        let list = RedisValue::List(vec!["a".to_string(), "bb".to_string()]);
        assert!(list.memory_usage() > 0);
    }
}