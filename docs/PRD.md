# Redis Replicator Rust Library - 产品需求文档 (PRD)

## 1. 项目概述

### 1.1 项目背景
基于对RedisShake项目的深入分析，我们需要构建一个高性能的Rust版本Redis复制器库，用于解析和处理Redis的AOF（Append Only File）和RDB（Redis Database）文件，以及实现Redis复制协议。

### 1.2 项目目标
- 提供高性能的Redis数据格式解析能力
- 支持完整的Redis复制协议实现
- 提供易用的Rust API接口
- 确保内存安全和并发安全
- 支持Redis 2.8+版本的所有特性

### 1.3 核心价值
- **性能优势**: 利用Rust的零成本抽象和内存安全特性
- **兼容性**: 完全兼容Redis官方协议和数据格式
- **可扩展性**: 模块化设计，支持自定义扩展
- **生产就绪**: 提供完整的错误处理和监控能力

## 2. 功能需求

### 2.1 RDB解析模块

#### 2.1.1 核心功能
- **RDB文件格式解析**
  - 支持Redis 2.6+的所有RDB版本
  - 解析RDB文件头（魔数、版本号）
  - 处理各种操作码（OPCODE）
  - 支持数据库选择、过期时间、LRU/LFU信息

- **数据类型支持**
  - String: 普通字符串、整数编码、LZF压缩
  - List: 普通列表、压缩列表、快速列表
  - Set: 普通集合、整数集合、列表包装
  - Hash: 普通哈希、压缩映射、压缩列表、列表包装
  - ZSet: 有序集合、压缩列表、列表包装
  - Stream: 流数据结构
  - Module: 模块数据类型

- **高级特性**
  - 函数库数据解析
  - 模块辅助数据处理
  - 槽位信息解析（Redis Cluster）

#### 2.1.2 API设计
```rust
pub struct RdbParser {
    // 解析器状态
}

impl RdbParser {
    pub fn new() -> Self;
    pub fn parse_file<P: AsRef<Path>>(&mut self, path: P) -> Result<RdbEntries>;
    pub fn parse_stream<R: Read>(&mut self, reader: R) -> Result<RdbEntries>;
}

pub enum RdbEntry {
    SelectDb(u32),
    KeyValue { key: String, value: RedisValue, expire: Option<u64> },
    Function(String),
    // ...
}
```

### 2.2 AOF解析模块

#### 2.2.1 核心功能
- **AOF文件格式解析**
  - 支持单文件AOF和多部分AOF
  - 解析AOF清单文件
  - 处理基础文件、历史文件、增量文件
  - 支持时间戳过滤

- **RESP协议解析**
  - 完整的RESP2/RESP3协议支持
  - 命令参数解析
  - 错误处理和恢复
  - 流式解析支持

- **命令处理**
  - Redis命令识别和分类
  - 参数验证和转换
  - 批量操作优化

#### 2.2.2 API设计
```rust
pub struct AofParser {
    // 解析器状态
}

impl AofParser {
    pub fn new() -> Self;
    pub fn parse_file<P: AsRef<Path>>(&mut self, path: P) -> Result<AofEntries>;
    pub fn parse_manifest<P: AsRef<Path>>(&mut self, manifest_path: P) -> Result<AofEntries>;
}

pub struct AofEntry {
    pub timestamp: Option<i64>,
    pub db_id: u32,
    pub command: Vec<String>,
}
```

### 2.3 Redis复制协议模块

#### 2.3.1 核心功能
- **连接管理**
  - TCP连接建立和维护
  - TLS支持
  - 连接池管理
  - 心跳和重连机制

- **认证和握手**
  - 用户名/密码认证
  - REPLCONF命令处理
  - 主从协商

- **同步协议**
  - PSYNC/SYNC命令支持
  - RDB传输处理
  - AOF流式同步
  - 偏移量管理

- **集群支持**
  - Redis Cluster协议
  - 槽位管理
  - 节点发现

#### 2.3.2 API设计
```rust
pub struct RedisReplicator {
    // 复制器状态
}

impl RedisReplicator {
    pub fn new(config: ReplicatorConfig) -> Self;
    pub async fn connect(&mut self) -> Result<()>;
    pub async fn start_replication(&mut self) -> Result<ReplicationStream>;
}

pub struct ReplicationStream {
    // 复制流
}

impl ReplicationStream {
    pub async fn next_entry(&mut self) -> Result<Option<ReplicationEntry>>;
}
```

### 2.4 数据结构模块

#### 2.4.1 Redis数据类型
```rust
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

#### 2.4.2 编码格式支持
- 整数编码
- LZF压缩
- 压缩列表（Ziplist）
- 列表包装（Listpack）
- 整数集合（Intset）

## 3. 非功能需求

### 3.1 性能要求
- **吞吐量**: 支持每秒处理100MB+的数据流
- **延迟**: 单条命令处理延迟<1ms
- **内存使用**: 流式处理，内存使用可控
- **并发**: 支持多线程并发处理

### 3.2 可靠性要求
- **错误处理**: 完善的错误类型和恢复机制
- **数据完整性**: 确保解析数据的正确性
- **容错性**: 处理损坏或不完整的数据文件

### 3.3 兼容性要求
- **Redis版本**: 支持Redis 2.8+所有版本
- **平台支持**: Linux、macOS、Windows
- **Rust版本**: 支持Rust 1.70+

### 3.4 可维护性要求
- **模块化设计**: 清晰的模块边界
- **文档完整**: 完整的API文档和示例
- **测试覆盖**: 单元测试覆盖率>90%
- **基准测试**: 性能回归检测

## 4. 技术架构

### 4.1 整体架构
```
┌─────────────────────────────────────────────────────────────┐
│                    Redis Replicator Rust                   │
├─────────────────────────────────────────────────────────────┤
│  Public API Layer                                          │
├─────────────────┬─────────────────┬─────────────────────────┤
│   RDB Parser    │   AOF Parser    │  Replication Client     │
├─────────────────┼─────────────────┼─────────────────────────┤
│  Data Structures│   RESP Protocol │   Network Layer         │
├─────────────────┼─────────────────┼─────────────────────────┤
│           Common Utilities & Error Handling                │
└─────────────────────────────────────────────────────────────┘
```

### 4.2 核心组件

#### 4.2.1 解析器层
- `rdb::parser`: RDB文件解析
- `aof::parser`: AOF文件解析
- `resp::parser`: RESP协议解析

#### 4.2.2 数据层
- `types::redis_value`: Redis数据类型
- `types::encoding`: 编码格式处理
- `types::entry`: 统一的数据条目

#### 4.2.3 网络层
- `net::client`: Redis客户端
- `net::replication`: 复制协议实现
- `net::cluster`: 集群支持

#### 4.2.4 工具层
- `utils::crc`: CRC校验
- `utils::compression`: 压缩算法
- `utils::io`: IO工具

## 5. 实施计划

### 5.1 第一阶段：基础框架（2周）
- 项目结构搭建
- 基础数据类型定义
- 错误处理框架
- 基础IO工具

### 5.2 第二阶段：RDB解析器（3周）
- RDB文件格式解析
- 基础数据类型支持
- 压缩和编码处理
- 单元测试

### 5.3 第三阶段：AOF解析器（2周）
- RESP协议解析
- AOF文件处理
- 多部分AOF支持
- 单元测试

### 5.4 第四阶段：复制协议（3周）
- 网络连接管理
- 认证和握手
- 同步协议实现
- 集成测试

### 5.5 第五阶段：优化和完善（2周）
- 性能优化
- 文档完善
- 示例代码
- 发布准备

## 6. 风险评估

### 6.1 技术风险
- **协议复杂性**: Redis协议细节较多，需要仔细实现
- **性能要求**: 需要在安全性和性能之间找到平衡
- **兼容性**: 不同Redis版本的差异处理

### 6.2 缓解措施
- 参考RedisShake的成熟实现
- 建立完善的测试体系
- 分阶段实施，逐步验证

## 7. 成功标准

### 7.1 功能标准
- 完整支持Redis 2.8+的RDB和AOF格式
- 实现完整的复制协议
- 提供易用的Rust API

### 7.2 性能标准
- 解析性能不低于RedisShake的80%
- 内存使用效率优于Go版本
- 支持高并发场景

### 7.3 质量标准
- 单元测试覆盖率>90%
- 通过Redis官方测试套件
- 文档完整，示例丰富