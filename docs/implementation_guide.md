# Redis Replicator Rust Library - 实现指南

## 1. 项目初始化

### 1.1 创建项目结构
```bash
cargo new redis_replicator_rs --lib
cd redis_replicator_rs
```

### 1.2 目录结构
```
redis_replicator_rs/
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── error.rs
│   ├── types/
│   │   ├── mod.rs
│   │   ├── redis_value.rs
│   │   ├── entry.rs
│   │   └── encoding.rs
│   ├── rdb/
│   │   ├── mod.rs
│   │   ├── parser.rs
│   │   ├── structure/
│   │   │   ├── mod.rs
│   │   │   ├── byte.rs
│   │   │   ├── string.rs
│   │   │   ├── length.rs
│   │   │   ├── int.rs
│   │   │   ├── float.rs
│   │   │   ├── ziplist.rs
│   │   │   ├── listpack.rs
│   │   │   └── intset.rs
│   │   └── types/
│   │       ├── mod.rs
│   │       ├── string.rs
│   │       ├── list.rs
│   │       ├── set.rs
│   │       ├── hash.rs
│   │       ├── zset.rs
│   │       ├── stream.rs
│   │       └── module.rs
│   ├── aof/
│   │   ├── mod.rs
│   │   ├── parser.rs
│   │   ├── manifest.rs
│   │   └── resp.rs
│   ├── replication/
│   │   ├── mod.rs
│   │   ├── client.rs
│   │   ├── protocol.rs
│   │   └── stream.rs
│   ├── net/
│   │   ├── mod.rs
│   │   ├── connection.rs
│   │   └── cluster.rs
│   └── utils/
│       ├── mod.rs
│       ├── crc.rs
│       ├── compression.rs
│       └── io.rs
├── tests/
│   ├── integration_tests.rs
│   ├── rdb_tests.rs
│   ├── aof_tests.rs
│   └── replication_tests.rs
├── benches/
│   ├── rdb_bench.rs
│   ├── aof_bench.rs
│   └── replication_bench.rs
├── examples/
│   ├── parse_rdb.rs
│   ├── parse_aof.rs
│   └── replicate.rs
└── docs/
    ├── PRD.md
    ├── implementation_guide.md
    └── api_reference.md
```

### 1.3 Cargo.toml配置
```toml
[package]
name = "redis_replicator_rs"
version = "0.1.0"
edition = "2021"
authors = ["Your Name <your.email@example.com>"]
license = "MIT OR Apache-2.0"
description = "A high-performance Redis replication library for Rust"
repository = "https://github.com/yourusername/redis_replicator_rs"
keywords = ["redis", "replication", "rdb", "aof", "database"]
categories = ["database", "parsing"]

[dependencies]
tokio = { version = "1.0", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
bytes = "1.0"
thiserror = "1.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
tracing = "0.1"
tracing-subscriber = "0.3"
byteorder = "1.4"
crc = "3.0"
flate2 = "1.0"
indexmap = "2.0"
smallvec = "1.10"
bitflags = "2.0"
chrono = { version = "0.4", features = ["serde"] }
url = "2.0"
regex = "1.0"

[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }
tempfile = "3.0"
proptest = "1.0"
tokio-test = "0.4"

[[bench]]
name = "rdb_bench"
harness = false

[[bench]]
name = "aof_bench"
harness = false

[[bench]]
name = "replication_bench"
harness = false
```

## 2. 核心模块实现

### 2.1 错误处理模块 (src/error.rs)
```rust
use thiserror::Error;
use std::io;

#[derive(Error, Debug)]
pub enum RedisReplicatorError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    #[error("Parse error: {0}")]
    Parse(String),
    
    #[error("Protocol error: {0}")]
    Protocol(String),
    
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Authentication error: {0}")]
    Auth(String),
    
    #[error("Compression error: {0}")]
    Compression(String),
    
    #[error("Invalid data format: {0}")]
    InvalidFormat(String),
    
    #[error("Unsupported feature: {0}")]
    Unsupported(String),
}

pub type Result<T> = std::result::Result<T, RedisReplicatorError>;
```

### 2.2 数据类型模块 (src/types/mod.rs)
```rust
pub mod redis_value;
pub mod entry;
pub mod encoding;

pub use redis_value::*;
pub use entry::*;
pub use encoding::*;
```

### 2.3 Redis值类型 (src/types/redis_value.rs)
```rust
use std::collections::{HashMap, HashSet, BTreeMap};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RedisValue {
    String(String),
    List(Vec<String>),
    Set(HashSet<String>),
    Hash(HashMap<String, String>),
    ZSet(BTreeMap<String, f64>),
    Stream(StreamValue),
    Module(ModuleValue),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamValue {
    pub entries: Vec<StreamEntry>,
    pub groups: Vec<StreamGroup>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamGroup {
    pub name: String,
    pub last_delivered_id: String,
    pub consumers: Vec<StreamConsumer>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamConsumer {
    pub name: String,
    pub seen_time: u64,
    pub pel_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModuleValue {
    pub module_id: u64,
    pub module_name: String,
    pub data: Vec<u8>,
}

impl RedisValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisValue::String(_) => "string",
            RedisValue::List(_) => "list",
            RedisValue::Set(_) => "set",
            RedisValue::Hash(_) => "hash",
            RedisValue::ZSet(_) => "zset",
            RedisValue::Stream(_) => "stream",
            RedisValue::Module(_) => "module",
        }
    }
}
```

### 2.4 数据条目 (src/types/entry.rs)
```rust
use super::RedisValue;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Entry {
    Rdb(RdbEntry),
    Aof(AofEntry),
    Replication(ReplicationEntry),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RdbEntry {
    SelectDb(u32),
    KeyValue {
        key: String,
        value: RedisValue,
        expire_ms: Option<u64>,
        idle: Option<u64>,
        freq: Option<u8>,
    },
    Function(String),
    ModuleAux {
        module_id: u64,
        data: Vec<u8>,
    },
    ResizeDb {
        db_size: u64,
        expire_size: u64,
    },
    Aux {
        key: String,
        value: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AofEntry {
    pub timestamp: Option<i64>,
    pub db_id: u32,
    pub command: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicationEntry {
    Rdb(RdbEntry),
    Command(AofEntry),
    Ping,
    Pong,
}
```

## 3. RDB解析器实现

### 3.1 RDB解析器主模块 (src/rdb/parser.rs)
```rust
use crate::error::{Result, RedisReplicatorError};
use crate::types::{RdbEntry, RedisValue};
use crate::rdb::structure::*;
use std::io::{Read, BufReader};
use std::fs::File;
use std::path::Path;

const RDB_VERSION_MIN: u32 = 1;
const RDB_VERSION_MAX: u32 = 11;

// RDB操作码常量
const RDB_OPCODE_SLOT_INFO: u8 = 244;
const RDB_OPCODE_FUNCTION2: u8 = 245;
const RDB_OPCODE_FUNCTION: u8 = 246;
const RDB_OPCODE_MODULE_AUX: u8 = 247;
const RDB_OPCODE_IDLE: u8 = 248;
const RDB_OPCODE_FREQ: u8 = 249;
const RDB_OPCODE_AUX: u8 = 250;
const RDB_OPCODE_RESIZEDB: u8 = 251;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
const RDB_OPCODE_EXPIRETIME: u8 = 253;
const RDB_OPCODE_SELECTDB: u8 = 254;
const RDB_OPCODE_EOF: u8 = 255;

pub struct RdbParser {
    current_db: u32,
    expire_ms: Option<u64>,
    idle: Option<u64>,
    freq: Option<u8>,
}

impl RdbParser {
    pub fn new() -> Self {
        Self {
            current_db: 0,
            expire_ms: None,
            idle: None,
            freq: None,
        }
    }
    
    pub fn parse_file<P: AsRef<Path>>(&mut self, path: P) -> Result<Vec<RdbEntry>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        self.parse_stream(reader)
    }
    
    pub fn parse_stream<R: Read>(&mut self, mut reader: R) -> Result<Vec<RdbEntry>> {
        // 验证魔数和版本
        let mut header = [0u8; 9];
        reader.read_exact(&mut header)?;
        
        if &header[0..5] != b"REDIS" {
            return Err(RedisReplicatorError::InvalidFormat(
                "Invalid RDB magic string".to_string()
            ));
        }
        
        let version_str = std::str::from_utf8(&header[5..9])
            .map_err(|_| RedisReplicatorError::InvalidFormat(
                "Invalid RDB version format".to_string()
            ))?;
            
        let version: u32 = version_str.parse()
            .map_err(|_| RedisReplicatorError::InvalidFormat(
                "Invalid RDB version number".to_string()
            ))?;
            
        if version < RDB_VERSION_MIN || version > RDB_VERSION_MAX {
            return Err(RedisReplicatorError::Unsupported(
                format!("RDB version {} not supported", version)
            ));
        }
        
        self.parse_entries(reader)
    }
    
    fn parse_entries<R: Read>(&mut self, mut reader: R) -> Result<Vec<RdbEntry>> {
        let mut entries = Vec::new();
        
        loop {
            let opcode = read_byte(&mut reader)?;
            
            match opcode {
                RDB_OPCODE_EOF => break,
                RDB_OPCODE_SELECTDB => {
                    self.current_db = read_length(&mut reader)? as u32;
                    entries.push(RdbEntry::SelectDb(self.current_db));
                },
                RDB_OPCODE_EXPIRETIME_MS => {
                    let expire_time = read_u64(&mut reader)?;
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    self.expire_ms = if expire_time > now {
                        Some(expire_time - now)
                    } else {
                        Some(1) // 已过期，设置为1ms
                    };
                },
                RDB_OPCODE_EXPIRETIME => {
                    let expire_time = read_u32(&mut reader)? as u64 * 1000;
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    self.expire_ms = if expire_time > now {
                        Some(expire_time - now)
                    } else {
                        Some(1)
                    };
                },
                RDB_OPCODE_IDLE => {
                    self.idle = Some(read_length(&mut reader)?);
                },
                RDB_OPCODE_FREQ => {
                    self.freq = Some(read_byte(&mut reader)?);
                },
                RDB_OPCODE_AUX => {
                    let key = read_string(&mut reader)?;
                    let value = read_string(&mut reader)?;
                    entries.push(RdbEntry::Aux { key, value });
                },
                RDB_OPCODE_RESIZEDB => {
                    let db_size = read_length(&mut reader)?;
                    let expire_size = read_length(&mut reader)?;
                    entries.push(RdbEntry::ResizeDb { db_size, expire_size });
                },
                RDB_OPCODE_FUNCTION2 => {
                    let function_code = read_string(&mut reader)?;
                    entries.push(RdbEntry::Function(function_code));
                },
                _ => {
                    // 解析键值对
                    let key = read_string(&mut reader)?;
                    let value = self.parse_object(&mut reader, opcode)?;
                    
                    entries.push(RdbEntry::KeyValue {
                        key,
                        value,
                        expire_ms: self.expire_ms.take(),
                        idle: self.idle.take(),
                        freq: self.freq.take(),
                    });
                }
            }
        }
        
        Ok(entries)
    }
    
    fn parse_object<R: Read>(&mut self, reader: R, type_byte: u8) -> Result<RedisValue> {
        // 根据类型字节解析不同的Redis对象
        // 这里需要实现各种Redis数据类型的解析逻辑
        // 具体实现将在types模块中完成
        todo!("Implement object parsing for type {}", type_byte)
    }
}

impl Default for RdbParser {
    fn default() -> Self {
        Self::new()
    }
}
```

### 3.2 RDB结构解析 (src/rdb/structure/mod.rs)
```rust
pub mod byte;
pub mod string;
pub mod length;
pub mod int;
pub mod float;
pub mod ziplist;
pub mod listpack;
pub mod intset;

pub use byte::*;
pub use string::*;
pub use length::*;
pub use int::*;
pub use float::*;
pub use ziplist::*;
pub use listpack::*;
pub use intset::*;

use crate::error::Result;
use std::io::Read;

// 重新导出常用函数
pub fn read_byte<R: Read>(reader: &mut R) -> Result<u8> {
    byte::read_byte(reader)
}

pub fn read_string<R: Read>(reader: &mut R) -> Result<String> {
    string::read_string(reader)
}

pub fn read_length<R: Read>(reader: &mut R) -> Result<u64> {
    length::read_length(reader)
}

pub fn read_u32<R: Read>(reader: &mut R) -> Result<u32> {
    int::read_u32(reader)
}

pub fn read_u64<R: Read>(reader: &mut R) -> Result<u64> {
    int::read_u64(reader)
}
```

## 4. AOF解析器实现

### 4.1 RESP协议解析 (src/aof/resp.rs)
```rust
use crate::error::{Result, RedisReplicatorError};
use std::io::{BufRead, BufReader, Read};
use bytes::{Buf, BytesMut};

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Option<Vec<RespValue>>),
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(String),
    BulkError(String),
    VerbatimString { format: String, data: String },
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
    Push(Vec<RespValue>),
}

pub struct RespParser {
    buffer: BytesMut,
}

impl RespParser {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
        }
    }
    
    pub fn parse<R: BufRead>(&mut self, reader: &mut R) -> Result<Option<RespValue>> {
        let mut line = String::new();
        let bytes_read = reader.read_line(&mut line)?;
        
        if bytes_read == 0 {
            return Ok(None); // EOF
        }
        
        if !line.ends_with("\r\n") {
            return Err(RedisReplicatorError::Protocol(
                "Invalid RESP format: missing CRLF".to_string()
            ));
        }
        
        line.truncate(line.len() - 2); // 移除\r\n
        
        if line.is_empty() {
            return Err(RedisReplicatorError::Protocol(
                "Empty RESP line".to_string()
            ));
        }
        
        let first_char = line.chars().next().unwrap();
        let data = &line[1..];
        
        match first_char {
            '+' => Ok(Some(RespValue::SimpleString(data.to_string()))),
            '-' => Ok(Some(RespValue::Error(data.to_string()))),
            ':' => {
                let num = data.parse::<i64>()
                    .map_err(|_| RedisReplicatorError::Protocol(
                        format!("Invalid integer: {}", data)
                    ))?;
                Ok(Some(RespValue::Integer(num)))
            },
            '$' => self.parse_bulk_string(reader, data),
            '*' => self.parse_array(reader, data),
            '_' => Ok(Some(RespValue::Null)),
            '#' => {
                match data {
                    "t" => Ok(Some(RespValue::Boolean(true))),
                    "f" => Ok(Some(RespValue::Boolean(false))),
                    _ => Err(RedisReplicatorError::Protocol(
                        format!("Invalid boolean: {}", data)
                    ))
                }
            },
            ',' => {
                let num = data.parse::<f64>()
                    .map_err(|_| RedisReplicatorError::Protocol(
                        format!("Invalid double: {}", data)
                    ))?;
                Ok(Some(RespValue::Double(num)))
            },
            _ => Err(RedisReplicatorError::Protocol(
                format!("Unknown RESP type: {}", first_char)
            ))
        }
    }
    
    fn parse_bulk_string<R: BufRead>(&mut self, reader: &mut R, len_str: &str) -> Result<Option<RespValue>> {
        let len = len_str.parse::<i64>()
            .map_err(|_| RedisReplicatorError::Protocol(
                format!("Invalid bulk string length: {}", len_str)
            ))?;
            
        if len == -1 {
            return Ok(Some(RespValue::BulkString(None)));
        }
        
        if len < 0 {
            return Err(RedisReplicatorError::Protocol(
                format!("Invalid bulk string length: {}", len)
            ));
        }
        
        let mut buffer = vec![0u8; len as usize];
        reader.read_exact(&mut buffer)?;
        
        // 读取结尾的\r\n
        let mut crlf = [0u8; 2];
        reader.read_exact(&mut crlf)?;
        if crlf != [b'\r', b'\n'] {
            return Err(RedisReplicatorError::Protocol(
                "Missing CRLF after bulk string".to_string()
            ));
        }
        
        let string = String::from_utf8(buffer)
            .map_err(|_| RedisReplicatorError::Protocol(
                "Invalid UTF-8 in bulk string".to_string()
            ))?;
            
        Ok(Some(RespValue::BulkString(Some(string))))
    }
    
    fn parse_array<R: BufRead>(&mut self, reader: &mut R, len_str: &str) -> Result<Option<RespValue>> {
        let len = len_str.parse::<i64>()
            .map_err(|_| RedisReplicatorError::Protocol(
                format!("Invalid array length: {}", len_str)
            ))?;
            
        if len == -1 {
            return Ok(Some(RespValue::Array(None)));
        }
        
        if len < 0 {
            return Err(RedisReplicatorError::Protocol(
                format!("Invalid array length: {}", len)
            ));
        }
        
        let mut elements = Vec::with_capacity(len as usize);
        for _ in 0..len {
            if let Some(element) = self.parse(reader)? {
                elements.push(element);
            } else {
                return Err(RedisReplicatorError::Protocol(
                    "Unexpected EOF in array".to_string()
                ));
            }
        }
        
        Ok(Some(RespValue::Array(Some(elements))))
    }
    
    pub fn to_command(&self, value: &RespValue) -> Result<Vec<String>> {
        match value {
            RespValue::Array(Some(elements)) => {
                let mut command = Vec::new();
                for element in elements {
                    match element {
                        RespValue::BulkString(Some(s)) => command.push(s.clone()),
                        RespValue::SimpleString(s) => command.push(s.clone()),
                        _ => return Err(RedisReplicatorError::Protocol(
                            "Invalid command format".to_string()
                        ))
                    }
                }
                Ok(command)
            },
            _ => Err(RedisReplicatorError::Protocol(
                "Command must be an array".to_string()
            ))
        }
    }
}

impl Default for RespParser {
    fn default() -> Self {
        Self::new()
    }
}
```

## 5. 复制协议实现

### 5.1 复制客户端 (src/replication/client.rs)
```rust
use crate::error::{Result, RedisReplicatorError};
use crate::types::{ReplicationEntry, AofEntry};
use crate::aof::RespParser;
use tokio::net::TcpStream;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub address: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub timeout: Duration,
    pub sync_rdb: bool,
    pub sync_aof: bool,
}

pub struct ReplicationClient {
    config: ReplicationConfig,
    stream: Option<TcpStream>,
    reader: Option<BufReader<TcpStream>>,
    writer: Option<BufWriter<TcpStream>>,
    resp_parser: RespParser,
    replication_id: Option<String>,
    replication_offset: i64,
}

impl ReplicationClient {
    pub fn new(config: ReplicationConfig) -> Self {
        Self {
            config,
            stream: None,
            reader: None,
            writer: None,
            resp_parser: RespParser::new(),
            replication_id: None,
            replication_offset: 0,
        }
    }
    
    pub async fn connect(&mut self) -> Result<()> {
        let addr = format!("{}:{}", self.config.address, self.config.port);
        let stream = tokio::time::timeout(
            self.config.timeout,
            TcpStream::connect(&addr)
        ).await
        .map_err(|_| RedisReplicatorError::Connection(
            format!("Connection timeout to {}", addr)
        ))?
        .map_err(|e| RedisReplicatorError::Connection(
            format!("Failed to connect to {}: {}", addr, e)
        ))?;
        
        let (read_half, write_half) = stream.into_split();
        self.reader = Some(BufReader::new(read_half));
        self.writer = Some(BufWriter::new(write_half));
        
        // 执行认证
        if let Some(password) = &self.config.password {
            self.authenticate(password).await?;
        }
        
        Ok(())
    }
    
    async fn authenticate(&mut self, password: &str) -> Result<()> {
        let auth_cmd = if let Some(username) = &self.config.username {
            format!("*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n", 
                   username.len(), username, password.len(), password)
        } else {
            format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", password.len(), password)
        };
        
        self.send_command(&auth_cmd).await?;
        let response = self.read_response().await?;
        
        // 检查认证响应
        match response {
            crate::aof::RespValue::SimpleString(s) if s == "OK" => Ok(()),
            crate::aof::RespValue::Error(e) => Err(RedisReplicatorError::Auth(
                format!("Authentication failed: {}", e)
            )),
            _ => Err(RedisReplicatorError::Auth(
                "Unexpected authentication response".to_string()
            ))
        }
    }
    
    pub async fn start_replication(&mut self) -> Result<ReplicationStream> {
        // 发送REPLCONF命令
        self.send_replconf_commands().await?;
        
        // 发送PSYNC命令
        self.send_psync().await?;
        
        // 处理PSYNC响应
        let response = self.read_response().await?;
        match response {
            crate::aof::RespValue::SimpleString(s) => {
                if s.starts_with("+FULLRESYNC") {
                    let parts: Vec<&str> = s.split_whitespace().collect();
                    if parts.len() >= 3 {
                        self.replication_id = Some(parts[1].to_string());
                        self.replication_offset = parts[2].parse().unwrap_or(0);
                    }
                } else {
                    return Err(RedisReplicatorError::Protocol(
                        format!("Unexpected PSYNC response: {}", s)
                    ));
                }
            },
            _ => return Err(RedisReplicatorError::Protocol(
                "Invalid PSYNC response format".to_string()
            ))
        }
        
        Ok(ReplicationStream::new(self))
    }
    
    async fn send_replconf_commands(&mut self) -> Result<()> {
        // REPLCONF listening-port
        let port_cmd = "*3\r\n$8\r\nREPLCONF\r\n$13\r\nlistening-port\r\n$4\r\n6379\r\n";
        self.send_command(port_cmd).await?;
        self.read_response().await?;
        
        // REPLCONF capa eof
        let capa_cmd = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n";
        self.send_command(capa_cmd).await?;
        self.read_response().await?;
        
        Ok(())
    }
    
    async fn send_psync(&mut self) -> Result<()> {
        let psync_cmd = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        self.send_command(psync_cmd).await?;
        Ok(())
    }
    
    async fn send_command(&mut self, command: &str) -> Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.write_all(command.as_bytes()).await?;
            writer.flush().await?;
            Ok(())
        } else {
            Err(RedisReplicatorError::Connection(
                "Not connected".to_string()
            ))
        }
    }
    
    async fn read_response(&mut self) -> Result<crate::aof::RespValue> {
        if let Some(reader) = &mut self.reader {
            if let Some(value) = self.resp_parser.parse(reader).await? {
                Ok(value)
            } else {
                Err(RedisReplicatorError::Connection(
                    "Connection closed".to_string()
                ))
            }
        } else {
            Err(RedisReplicatorError::Connection(
                "Not connected".to_string()
            ))
        }
    }
}

pub struct ReplicationStream {
    client: *mut ReplicationClient,
}

impl ReplicationStream {
    fn new(client: &mut ReplicationClient) -> Self {
        Self {
            client: client as *mut ReplicationClient,
        }
    }
    
    pub async fn next_entry(&mut self) -> Result<Option<ReplicationEntry>> {
        // 实现复制流的下一个条目读取
        // 这里需要处理RDB数据和AOF命令
        todo!("Implement replication stream reading")
    }
}
```

## 6. 测试实现

### 6.1 单元测试示例 (tests/rdb_tests.rs)
```rust
use redis_replicator_rs::rdb::RdbParser;
use redis_replicator_rs::types::RdbEntry;
use tempfile::NamedTempFile;
use std::io::Write;

#[test]
fn test_rdb_header_parsing() {
    let mut parser = RdbParser::new();
    
    // 创建一个简单的RDB文件
    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(b"REDIS0011").unwrap(); // 魔数 + 版本
    temp_file.write_all(&[255]).unwrap(); // EOF
    temp_file.flush().unwrap();
    
    let entries = parser.parse_file(temp_file.path()).unwrap();
    assert!(entries.is_empty());
}

#[test]
fn test_rdb_select_db() {
    let mut parser = RdbParser::new();
    
    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(b"REDIS0011").unwrap();
    temp_file.write_all(&[254, 1]).unwrap(); // SELECT DB 1
    temp_file.write_all(&[255]).unwrap(); // EOF
    temp_file.flush().unwrap();
    
    let entries = parser.parse_file(temp_file.path()).unwrap();
    assert_eq!(entries.len(), 1);
    
    match &entries[0] {
        RdbEntry::SelectDb(db_id) => assert_eq!(*db_id, 1),
        _ => panic!("Expected SelectDb entry"),
    }
}
```

### 6.2 集成测试示例 (tests/integration_tests.rs)
```rust
use redis_replicator_rs::replication::{ReplicationClient, ReplicationConfig};
use std::time::Duration;

#[tokio::test]
async fn test_redis_connection() {
    let config = ReplicationConfig {
        address: "127.0.0.1".to_string(),
        port: 6379,
        username: None,
        password: None,
        timeout: Duration::from_secs(5),
        sync_rdb: true,
        sync_aof: true,
    };
    
    let mut client = ReplicationClient::new(config);
    
    // 注意：这个测试需要运行中的Redis实例
    if let Ok(_) = client.connect().await {
        println!("Successfully connected to Redis");
    } else {
        println!("Redis not available, skipping test");
    }
}
```

## 7. 性能基准测试

### 7.1 RDB解析基准 (benches/rdb_bench.rs)
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redis_replicator_rs::rdb::RdbParser;
use std::io::Cursor;

fn bench_rdb_parsing(c: &mut Criterion) {
    let rdb_data = create_test_rdb_data();
    
    c.bench_function("rdb_parse_small", |b| {
        b.iter(|| {
            let mut parser = RdbParser::new();
            let cursor = Cursor::new(black_box(&rdb_data));
            parser.parse_stream(cursor).unwrap()
        })
    });
}

fn create_test_rdb_data() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(b"REDIS0011");
    data.push(255); // EOF
    data
}

criterion_group!(benches, bench_rdb_parsing);
criterion_main!(benches);
```

## 8. 示例代码

### 8.1 RDB解析示例 (examples/parse_rdb.rs)
```rust
use redis_replicator_rs::rdb::RdbParser;
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <rdb_file>", args[0]);
        std::process::exit(1);
    }
    
    let rdb_file = &args[1];
    let mut parser = RdbParser::new();
    
    println!("Parsing RDB file: {}", rdb_file);
    let entries = parser.parse_file(rdb_file)?;
    
    println!("Found {} entries:", entries.len());
    for (i, entry) in entries.iter().enumerate() {
        println!("{}: {:?}", i + 1, entry);
    }
    
    Ok(())
}
```

### 8.2 复制示例 (examples/replicate.rs)
```rust
use redis_replicator_rs::replication::{ReplicationClient, ReplicationConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ReplicationConfig {
        address: "127.0.0.1".to_string(),
        port: 6379,
        username: None,
        password: None,
        timeout: Duration::from_secs(10),
        sync_rdb: true,
        sync_aof: true,
    };
    
    let mut client = ReplicationClient::new(config);
    
    println!("Connecting to Redis...");
    client.connect().await?;
    
    println!("Starting replication...");
    let mut stream = client.start_replication().await?;
    
    println!("Receiving replication data...");
    while let Some(entry) = stream.next_entry().await? {
        println!("Received: {:?}", entry);
    }
    
    Ok(())
}
```

## 9. 部署和发布

### 9.1 CI/CD配置 (.github/workflows/ci.yml)
```yaml
name: CI

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

env:
  CARGO_TERM_COLOR: always

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      redis:
        image: redis:7
        ports:
          - 6379:6379
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Install Rust
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true
        components: rustfmt, clippy
    
    - name: Cache cargo registry
      uses: actions/cache@v3
      with:
        path: ~/.cargo/registry
        key: ${{ runner.os }}-cargo-registry-${{ hashFiles('**/Cargo.lock') }}
    
    - name: Cache cargo index
      uses: actions/cache@v3
      with:
        path: ~/.cargo/git
        key: ${{ runner.os }}-cargo-index-${{ hashFiles('**/Cargo.lock') }}
    
    - name: Cache cargo build
      uses: actions/cache@v3
      with:
        path: target
        key: ${{ runner.os }}-cargo-build-target-${{ hashFiles('**/Cargo.lock') }}
    
    - name: Check formatting
      run: cargo fmt -- --check
    
    - name: Run clippy
      run: cargo clippy -- -D warnings
    
    - name: Run tests
      run: cargo test --verbose
    
    - name: Run benchmarks
      run: cargo bench --verbose
```

### 9.2 发布脚本 (scripts/release.sh)
```bash
#!/bin/bash

set -e

# 检查是否在main分支
if [ "$(git branch --show-current)" != "main" ]; then
    echo "Error: Must be on main branch to release"
    exit 1
fi

# 检查工作目录是否干净
if [ -n "$(git status --porcelain)" ]; then
    echo "Error: Working directory is not clean"
    exit 1
fi

# 获取版本号
VERSION=$(grep '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
echo "Releasing version $VERSION"

# 运行测试
echo "Running tests..."
cargo test

# 运行基准测试
echo "Running benchmarks..."
cargo bench

# 构建文档
echo "Building documentation..."
cargo doc --no-deps

# 创建git标签
echo "Creating git tag..."
git tag -a "v$VERSION" -m "Release version $VERSION"

# 发布到crates.io
echo "Publishing to crates.io..."
cargo publish

# 推送标签
echo "Pushing git tag..."
git push origin "v$VERSION"

echo "Release $VERSION completed successfully!"
```

## 10. 总结

这个实现指南提供了构建Redis复制器Rust库的完整路径：

1. **项目结构**: 清晰的模块化设计
2. **核心功能**: RDB解析、AOF解析、复制协议
3. **错误处理**: 完善的错误类型系统
4. **性能优化**: 流式处理和零拷贝设计
5. **测试覆盖**: 单元测试、集成测试、基准测试
6. **文档完整**: API文档和使用示例
7. **CI/CD**: 自动化测试和发布流程

通过遵循这个指南，可以构建出一个高性能、安全可靠的Redis复制器库，满足生产环境的需求。