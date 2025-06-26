# Redis Replicator Rust Library - API 参考文档

## 概述

本文档详细描述了Redis Replicator Rust库的公共API接口。该库提供了解析Redis RDB文件、AOF文件以及实现Redis复制协议的功能。

## 模块结构

```rust
pub mod error;      // 错误类型定义
pub mod types;      // 数据类型定义
pub mod rdb;        // RDB解析器
pub mod aof;        // AOF解析器
pub mod replication; // 复制协议实现
pub mod utils;      // 工具函数
```

## 错误处理

### `error::RedisReplicatorError`

主要的错误类型枚举，包含所有可能的错误情况。

```rust
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
```

### `error::Result<T>`

库中使用的标准Result类型别名。

```rust
pub type Result<T> = std::result::Result<T, RedisReplicatorError>;
```

## 数据类型

### `types::RedisValue`

Redis数据值的枚举类型，支持所有Redis数据类型。

```rust
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
```

#### 方法

- `type_name(&self) -> &'static str`
  - 返回Redis数据类型的名称字符串
  - 返回值: `"string"`, `"list"`, `"set"`, `"hash"`, `"zset"`, `"stream"`, `"module"`

### `types::StreamValue`

Redis Stream数据类型的表示。

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamValue {
    pub entries: Vec<StreamEntry>,
    pub groups: Vec<StreamGroup>,
}
```

### `types::StreamEntry`

Stream中的单个条目。

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamEntry {
    pub id: String,
    pub fields: HashMap<String, String>,
}
```

### `types::StreamGroup`

Stream消费者组。

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamGroup {
    pub name: String,
    pub last_delivered_id: String,
    pub consumers: Vec<StreamConsumer>,
}
```

### `types::StreamConsumer`

Stream消费者。

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamConsumer {
    pub name: String,
    pub seen_time: u64,
    pub pel_count: u64,
}
```

### `types::ModuleValue`

Redis模块数据类型。

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModuleValue {
    pub module_id: u64,
    pub module_name: String,
    pub data: Vec<u8>,
}
```

### `types::Entry`

统一的数据条目枚举。

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Entry {
    Rdb(RdbEntry),
    Aof(AofEntry),
    Replication(ReplicationEntry),
}
```

### `types::RdbEntry`

RDB文件中的条目类型。

```rust
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
```

### `types::AofEntry`

AOF文件中的命令条目。

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AofEntry {
    pub timestamp: Option<i64>,
    pub db_id: u32,
    pub command: Vec<String>,
}
```

### `types::ReplicationEntry`

复制流中的条目类型。

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReplicationEntry {
    Rdb(RdbEntry),
    Command(AofEntry),
    Ping,
    Pong,
}
```

## RDB解析器

### `rdb::RdbParser`

RDB文件解析器的主要结构体。

```rust
pub struct RdbParser {
    // 内部字段（私有）
}
```

#### 构造函数

- `new() -> Self`
  - 创建新的RDB解析器实例
  - 返回值: 新的`RdbParser`实例

#### 方法

- `parse_file<P: AsRef<Path>>(&mut self, path: P) -> Result<Vec<RdbEntry>>`
  - 解析RDB文件
  - 参数: `path` - RDB文件路径
  - 返回值: RDB条目的向量
  - 错误: 文件不存在、格式错误、IO错误等

- `parse_stream<R: Read>(&mut self, reader: R) -> Result<Vec<RdbEntry>>`
  - 从流中解析RDB数据
  - 参数: `reader` - 实现了`Read` trait的数据源
  - 返回值: RDB条目的向量
  - 错误: 格式错误、IO错误等

#### 示例

```rust
use redis_replicator_rs::rdb::RdbParser;

let mut parser = RdbParser::new();
let entries = parser.parse_file("/path/to/dump.rdb")?;

for entry in entries {
    match entry {
        RdbEntry::KeyValue { key, value, .. } => {
            println!("Key: {}, Type: {}", key, value.type_name());
        },
        RdbEntry::SelectDb(db_id) => {
            println!("Selected database: {}", db_id);
        },
        _ => {}
    }
}
```

## AOF解析器

### `aof::AofParser`

AOF文件解析器。

```rust
pub struct AofParser {
    // 内部字段（私有）
}
```

#### 构造函数

- `new() -> Self`
  - 创建新的AOF解析器实例

#### 方法

- `parse_file<P: AsRef<Path>>(&mut self, path: P) -> Result<Vec<AofEntry>>`
  - 解析AOF文件
  - 参数: `path` - AOF文件路径
  - 返回值: AOF条目的向量

- `parse_manifest<P: AsRef<Path>>(&mut self, manifest_path: P) -> Result<Vec<AofEntry>>`
  - 解析多部分AOF的清单文件
  - 参数: `manifest_path` - AOF清单文件路径
  - 返回值: 所有AOF条目的向量

- `parse_stream<R: BufRead>(&mut self, reader: R) -> Result<Vec<AofEntry>>`
  - 从流中解析AOF数据
  - 参数: `reader` - 实现了`BufRead` trait的数据源
  - 返回值: AOF条目的向量

#### 示例

```rust
use redis_replicator_rs::aof::AofParser;

let mut parser = AofParser::new();
let entries = parser.parse_file("/path/to/appendonly.aof")?;

for entry in entries {
    println!("DB: {}, Command: {:?}", entry.db_id, entry.command);
}
```

### `aof::RespParser`

RESP协议解析器。

```rust
pub struct RespParser {
    // 内部字段（私有）
}
```

#### 构造函数

- `new() -> Self`
  - 创建新的RESP解析器实例

#### 方法

- `parse<R: BufRead>(&mut self, reader: &mut R) -> Result<Option<RespValue>>`
  - 解析单个RESP值
  - 参数: `reader` - 缓冲读取器
  - 返回值: 解析的RESP值，如果到达EOF则返回None

- `to_command(&self, value: &RespValue) -> Result<Vec<String>>`
  - 将RESP值转换为Redis命令
  - 参数: `value` - RESP值
  - 返回值: 命令参数的字符串向量

### `aof::RespValue`

RESP协议值的枚举。

```rust
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
```

## 复制协议

### `replication::ReplicationConfig`

复制客户端的配置结构体。

```rust
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
```

#### 字段说明

- `address`: Redis服务器地址
- `port`: Redis服务器端口
- `username`: 可选的用户名（Redis 6.0+）
- `password`: 可选的密码
- `timeout`: 连接超时时间
- `sync_rdb`: 是否同步RDB数据
- `sync_aof`: 是否同步AOF数据

### `replication::ReplicationClient`

Redis复制客户端。

```rust
pub struct ReplicationClient {
    // 内部字段（私有）
}
```

#### 构造函数

- `new(config: ReplicationConfig) -> Self`
  - 创建新的复制客户端
  - 参数: `config` - 复制配置

#### 方法

- `async fn connect(&mut self) -> Result<()>`
  - 连接到Redis服务器
  - 错误: 连接失败、认证失败等

- `async fn start_replication(&mut self) -> Result<ReplicationStream>`
  - 开始复制过程
  - 返回值: 复制数据流
  - 错误: 协议错误、连接错误等

#### 示例

```rust
use redis_replicator_rs::replication::{ReplicationClient, ReplicationConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ReplicationConfig {
        address: "127.0.0.1".to_string(),
        port: 6379,
        username: None,
        password: Some("mypassword".to_string()),
        timeout: Duration::from_secs(10),
        sync_rdb: true,
        sync_aof: true,
    };
    
    let mut client = ReplicationClient::new(config);
    client.connect().await?;
    
    let mut stream = client.start_replication().await?;
    
    while let Some(entry) = stream.next_entry().await? {
        match entry {
            ReplicationEntry::Rdb(rdb_entry) => {
                println!("RDB: {:?}", rdb_entry);
            },
            ReplicationEntry::Command(aof_entry) => {
                println!("Command: {:?}", aof_entry.command);
            },
            _ => {}
        }
    }
    
    Ok(())
}
```

### `replication::ReplicationStream`

复制数据流。

```rust
pub struct ReplicationStream {
    // 内部字段（私有）
}
```

#### 方法

- `async fn next_entry(&mut self) -> Result<Option<ReplicationEntry>>`
  - 获取下一个复制条目
  - 返回值: 复制条目，如果流结束则返回None
  - 错误: 网络错误、协议错误等

## 工具函数

### `utils::crc`

CRC校验相关函数。

- `crc64(data: &[u8]) -> u64`
  - 计算CRC64校验和
  - 参数: `data` - 要校验的数据
  - 返回值: CRC64校验和

- `crc16(data: &[u8]) -> u16`
  - 计算CRC16校验和
  - 参数: `data` - 要校验的数据
  - 返回值: CRC16校验和

### `utils::compression`

压缩相关函数。

- `lzf_decompress(input: &[u8], output_len: usize) -> Result<Vec<u8>>`
  - LZF解压缩
  - 参数: `input` - 压缩数据，`output_len` - 预期输出长度
  - 返回值: 解压缩后的数据
  - 错误: 解压缩失败

### `utils::io`

IO相关工具函数。

- `read_exact<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<()>`
  - 精确读取指定字节数
  - 参数: `reader` - 读取器，`buf` - 缓冲区
  - 错误: IO错误、EOF等

## 特性实现

### 序列化支持

所有主要的数据类型都实现了`Serialize`和`Deserialize` trait，支持JSON序列化。

```rust
use serde_json;

let value = RedisValue::String("hello".to_string());
let json = serde_json::to_string(&value)?;
let deserialized: RedisValue = serde_json::from_str(&json)?;
```

### 调试支持

所有公共类型都实现了`Debug` trait，支持调试输出。

```rust
let entry = RdbEntry::SelectDb(1);
println!("{:?}", entry);
```

### 克隆支持

大部分数据类型都实现了`Clone` trait，支持深拷贝。

```rust
let original = RedisValue::String("test".to_string());
let cloned = original.clone();
```

## 错误处理最佳实践

### 错误传播

使用`?`操作符进行错误传播：

```rust
fn parse_and_process(path: &str) -> Result<()> {
    let mut parser = RdbParser::new();
    let entries = parser.parse_file(path)?;
    
    for entry in entries {
        process_entry(entry)?;
    }
    
    Ok(())
}
```

### 错误匹配

根据错误类型进行不同处理：

```rust
match parser.parse_file(path) {
    Ok(entries) => process_entries(entries),
    Err(RedisReplicatorError::Io(e)) => {
        eprintln!("IO error: {}", e);
    },
    Err(RedisReplicatorError::InvalidFormat(msg)) => {
        eprintln!("Invalid format: {}", msg);
    },
    Err(e) => {
        eprintln!("Other error: {}", e);
    }
}
```

## 性能考虑

### 内存使用

- 使用流式解析避免加载整个文件到内存
- 对于大型数据集，考虑使用迭代器模式

### 并发处理

- 解析器不是线程安全的，每个线程应使用独立的解析器实例
- 可以并行处理多个文件

```rust
use rayon::prelude::*;

let files = vec!["file1.rdb", "file2.rdb", "file3.rdb"];
let results: Vec<_> = files.par_iter()
    .map(|file| {
        let mut parser = RdbParser::new();
        parser.parse_file(file)
    })
    .collect();
```

## 版本兼容性

### Redis版本支持

- RDB格式: 支持Redis 2.6 - 7.x的所有版本
- AOF格式: 支持Redis 2.8 - 7.x的所有版本
- 复制协议: 支持PSYNC和SYNC命令

### Rust版本要求

- 最低支持Rust 1.70.0
- 推荐使用最新稳定版本

## 许可证

本库采用MIT或Apache-2.0双重许可证。详见LICENSE文件。