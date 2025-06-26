# Redis Replicator Rust Library - 架构设计文档

## 1. 架构概览

### 1.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Redis Replicator Rust Library                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                              Public API Layer                              │
│  ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐  │
│  │   RDB Parser    │   AOF Parser    │  Replication    │   Utilities     │  │
│  │                 │                 │    Client       │                 │  │
│  └─────────────────┴─────────────────┴─────────────────┴─────────────────┘  │
├─────────────────────────────────────────────────────────────────────────────┤
│                            Core Processing Layer                           │
│  ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐  │
│  │ RDB Structure   │ RESP Protocol   │ Network Layer   │ Data Types      │  │
│  │ Parsing         │ Parsing         │                 │                 │  │
│  └─────────────────┴─────────────────┴─────────────────┴─────────────────┘  │
├─────────────────────────────────────────────────────────────────────────────┤
│                           Foundation Layer                                 │
│  ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐  │
│  │ Error Handling  │ IO Utilities    │ Compression     │ Serialization   │  │
│  │                 │                 │                 │                 │  │
│  └─────────────────┴─────────────────┴─────────────────┴─────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 设计原则

1. **模块化设计**: 清晰的模块边界，低耦合高内聚
2. **性能优先**: 零拷贝、流式处理、内存效率
3. **类型安全**: 利用Rust的类型系统确保安全性
4. **错误透明**: 完善的错误处理和传播机制
5. **异步友好**: 支持异步IO和并发处理
6. **可扩展性**: 支持自定义扩展和插件

## 2. 核心模块设计

### 2.1 RDB解析模块

#### 2.1.1 模块结构

```
rdb/
├── mod.rs              # 模块导出
├── parser.rs           # 主解析器
├── structure/          # 底层结构解析
│   ├── mod.rs
│   ├── byte.rs         # 字节读取
│   ├── string.rs       # 字符串解析
│   ├── length.rs       # 长度编码
│   ├── int.rs          # 整数解析
│   ├── float.rs        # 浮点数解析
│   ├── ziplist.rs      # 压缩列表
│   ├── listpack.rs     # 列表包装
│   └── intset.rs       # 整数集合
└── types/              # 数据类型解析
    ├── mod.rs
    ├── string.rs       # 字符串类型
    ├── list.rs         # 列表类型
    ├── set.rs          # 集合类型
    ├── hash.rs         # 哈希类型
    ├── zset.rs         # 有序集合
    ├── stream.rs       # 流类型
    └── module.rs       # 模块类型
```

#### 2.1.2 解析流程

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ File/Stream │───▶│ Header      │───▶│ Entry       │───▶│ Object      │
│ Input       │    │ Validation  │    │ Parsing     │    │ Construction│
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                           │                   │                   │
                           ▼                   ▼                   ▼
                   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
                   │ Magic +     │    │ Opcode      │    │ Type-specific│
                   │ Version     │    │ Dispatch    │    │ Parsing     │
                   └─────────────┘    └─────────────┘    └─────────────┘
```

#### 2.1.3 关键设计决策

**流式解析**
- 不将整个文件加载到内存
- 使用`BufReader`进行缓冲读取
- 支持大文件处理

**零拷贝优化**
- 尽可能避免数据复制
- 使用`Bytes`类型管理内存
- 延迟字符串构造

**错误恢复**
- 在遇到损坏数据时尝试恢复
- 提供详细的错误上下文
- 支持部分解析结果

### 2.2 AOF解析模块

#### 2.2.1 模块结构

```
aof/
├── mod.rs              # 模块导出
├── parser.rs           # AOF解析器
├── manifest.rs         # 清单文件处理
├── resp.rs             # RESP协议解析
└── command.rs          # 命令处理
```

#### 2.2.2 RESP协议解析

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Raw Bytes   │───▶│ Line        │───▶│ Type        │───▶│ Value       │
│             │    │ Reading     │    │ Detection   │    │ Construction│
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                           │                   │                   │
                           ▼                   ▼                   ▼
                   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
                   │ CRLF        │    │ First Byte  │    │ Recursive   │
                   │ Handling    │    │ Analysis    │    │ Parsing     │
                   └─────────────┘    └─────────────┘    └─────────────┘
```

#### 2.2.3 多部分AOF支持

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Manifest    │───▶│ File List   │───▶│ Sequential  │
│ File        │    │ Parsing     │    │ Processing  │
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Base Files  │    │ Incremental │    │ History     │
│             │    │ Files       │    │ Files       │
└─────────────┘    └─────────────┘    └─────────────┘
```

### 2.3 复制协议模块

#### 2.3.1 模块结构

```
replication/
├── mod.rs              # 模块导出
├── client.rs           # 复制客户端
├── protocol.rs         # 协议实现
├── stream.rs           # 数据流处理
└── cluster.rs          # 集群支持
```

#### 2.3.2 复制流程

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Connection  │───▶│ Handshake   │───▶│ RDB Sync    │───▶│ AOF Stream  │
│ Establish   │    │ & Auth      │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │                   │
       ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ TCP Socket  │    │ REPLCONF    │    │ PSYNC       │    │ Command     │
│ + TLS       │    │ Commands    │    │ Response    │    │ Processing  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

#### 2.3.3 异步处理

```rust
// 异步复制客户端设计
pub struct ReplicationClient {
    connection: Arc<Mutex<Connection>>,
    config: ReplicationConfig,
    state: Arc<RwLock<ReplicationState>>,
}

// 使用tokio进行异步处理
impl ReplicationClient {
    pub async fn start_replication(&mut self) -> Result<ReplicationStream> {
        // 建立连接
        self.connect().await?;
        
        // 执行握手
        self.handshake().await?;
        
        // 开始数据同步
        let stream = self.create_stream().await?;
        Ok(stream)
    }
}
```

## 3. 数据流设计

### 3.1 数据流架构

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Data Source │───▶│ Parser      │───▶│ Transform   │───▶│ Consumer    │
│ (File/Net)  │    │             │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │                   │
       ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ • RDB File  │    │ • Structure │    │ • Filter    │    │ • Writer    │
│ • AOF File  │    │   Parsing   │    │ • Convert   │    │ • Callback  │
│ • Network   │    │ • Protocol  │    │ • Validate  │    │ • Stream    │
│   Stream    │    │   Parsing   │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### 3.2 流式处理

```rust
// 流式处理接口设计
pub trait DataStream {
    type Item;
    type Error;
    
    async fn next(&mut self) -> Result<Option<Self::Item>, Self::Error>;
}

// RDB流实现
pub struct RdbStream<R: AsyncRead> {
    reader: R,
    parser: RdbParser,
    buffer: BytesMut,
}

impl<R: AsyncRead + Unpin> DataStream for RdbStream<R> {
    type Item = RdbEntry;
    type Error = RedisReplicatorError;
    
    async fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        // 流式解析实现
        self.parser.parse_next(&mut self.reader).await
    }
}
```

### 3.3 背压处理

```rust
// 背压控制
pub struct BackpressureConfig {
    pub max_buffer_size: usize,
    pub high_watermark: usize,
    pub low_watermark: usize,
}

// 在流处理中实现背压
impl ReplicationStream {
    async fn handle_backpressure(&mut self) -> Result<()> {
        if self.buffer.len() > self.config.high_watermark {
            // 暂停读取，等待消费
            self.pause_reading().await?;
        }
        
        if self.buffer.len() < self.config.low_watermark {
            // 恢复读取
            self.resume_reading().await?;
        }
        
        Ok(())
    }
}
```

## 4. 内存管理

### 4.1 内存分配策略

**小对象池化**
```rust
use object_pool::Pool;

// 字符串对象池
static STRING_POOL: Lazy<Pool<String>> = Lazy::new(|| {
    Pool::new(100, || String::with_capacity(64))
});

// 向量对象池
static VEC_POOL: Lazy<Pool<Vec<u8>>> = Lazy::new(|| {
    Pool::new(50, || Vec::with_capacity(1024))
});
```

**缓冲区重用**
```rust
pub struct BufferManager {
    buffers: Vec<BytesMut>,
    current: usize,
}

impl BufferManager {
    pub fn get_buffer(&mut self, min_size: usize) -> BytesMut {
        // 寻找合适大小的缓冲区
        for buffer in &mut self.buffers {
            if buffer.capacity() >= min_size {
                buffer.clear();
                return std::mem::take(buffer);
            }
        }
        
        // 创建新缓冲区
        BytesMut::with_capacity(min_size.max(4096))
    }
    
    pub fn return_buffer(&mut self, mut buffer: BytesMut) {
        if buffer.capacity() <= 64 * 1024 {
            buffer.clear();
            self.buffers.push(buffer);
        }
    }
}
```

### 4.2 零拷贝优化

**引用计数字符串**
```rust
use bytes::Bytes;
use std::sync::Arc;

#[derive(Clone)]
pub enum StringRef {
    Owned(String),
    Shared(Arc<str>),
    Bytes(Bytes),
}

impl StringRef {
    pub fn as_str(&self) -> &str {
        match self {
            StringRef::Owned(s) => s,
            StringRef::Shared(s) => s,
            StringRef::Bytes(b) => std::str::from_utf8(b).unwrap(),
        }
    }
}
```

**视图类型**
```rust
// 避免复制的数据视图
pub struct DataView<'a> {
    data: &'a [u8],
    offset: usize,
    length: usize,
}

impl<'a> DataView<'a> {
    pub fn slice(&self, start: usize, end: usize) -> DataView<'a> {
        DataView {
            data: self.data,
            offset: self.offset + start,
            length: end - start,
        }
    }
    
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[self.offset..self.offset + self.length]
    }
}
```

## 5. 错误处理架构

### 5.1 错误分层

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Errors                      │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐  │
│  │ Parse Error │ Protocol    │ Connection  │ Auth Error  │  │
│  │             │ Error       │ Error       │             │  │
│  └─────────────┴─────────────┴─────────────┴─────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                     System Errors                          │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐  │
│  │ IO Error    │ Network     │ Timeout     │ Resource    │  │
│  │             │ Error       │ Error       │ Error       │  │
│  └─────────────┴─────────────┴─────────────┴─────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 错误上下文

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedisReplicatorError {
    #[error("Parse error at offset {offset}: {message}")]
    Parse {
        offset: u64,
        message: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    
    #[error("Protocol error in {context}: {message}")]
    Protocol {
        context: String,
        message: String,
    },
    
    #[error("Connection error: {0}")]
    Connection(#[from] ConnectionError),
}

// 错误构建器
pub struct ErrorBuilder {
    kind: ErrorKind,
    context: Vec<String>,
    offset: Option<u64>,
}

impl ErrorBuilder {
    pub fn parse() -> Self {
        Self {
            kind: ErrorKind::Parse,
            context: Vec::new(),
            offset: None,
        }
    }
    
    pub fn with_context(mut self, ctx: impl Into<String>) -> Self {
        self.context.push(ctx.into());
        self
    }
    
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }
    
    pub fn build(self, message: impl Into<String>) -> RedisReplicatorError {
        // 构建具体错误
        match self.kind {
            ErrorKind::Parse => RedisReplicatorError::Parse {
                offset: self.offset.unwrap_or(0),
                message: message.into(),
                source: None,
            },
            // ...
        }
    }
}
```

### 5.3 错误恢复策略

```rust
// 可恢复的解析器
pub struct RecoverableParser {
    parser: RdbParser,
    recovery_strategy: RecoveryStrategy,
}

pub enum RecoveryStrategy {
    SkipEntry,
    SkipToNextOpcode,
    Abort,
}

impl RecoverableParser {
    pub fn parse_with_recovery<R: Read>(
        &mut self, 
        reader: R
    ) -> (Vec<RdbEntry>, Vec<RedisReplicatorError>) {
        let mut entries = Vec::new();
        let mut errors = Vec::new();
        
        loop {
            match self.parser.parse_next_entry(&mut reader) {
                Ok(Some(entry)) => entries.push(entry),
                Ok(None) => break, // EOF
                Err(e) => {
                    errors.push(e);
                    
                    match self.recovery_strategy {
                        RecoveryStrategy::SkipEntry => {
                            // 尝试跳过当前条目
                            if self.skip_current_entry(&mut reader).is_err() {
                                break;
                            }
                        },
                        RecoveryStrategy::Abort => break,
                        // ...
                    }
                }
            }
        }
        
        (entries, errors)
    }
}
```

## 6. 性能优化

### 6.1 CPU优化

**SIMD指令使用**
```rust
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

// 快速字节搜索
pub fn find_crlf_simd(data: &[u8]) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("sse2") {
            return unsafe { find_crlf_sse2(data) };
        }
    }
    
    // 回退到标准实现
    find_crlf_scalar(data)
}

#[cfg(target_arch = "x86_64")]
unsafe fn find_crlf_sse2(data: &[u8]) -> Option<usize> {
    // SSE2优化的CRLF搜索
    let cr = _mm_set1_epi8(b'\r' as i8);
    let lf = _mm_set1_epi8(b'\n' as i8);
    
    for chunk in data.chunks_exact(16) {
        let bytes = _mm_loadu_si128(chunk.as_ptr() as *const __m128i);
        let cr_mask = _mm_cmpeq_epi8(bytes, cr);
        let lf_mask = _mm_cmpeq_epi8(bytes, lf);
        
        // 检查连续的CR LF
        // ...
    }
    
    None
}
```

**分支预测优化**
```rust
// 使用likely/unlikely宏
macro_rules! likely {
    ($expr:expr) => {
        std::intrinsics::likely($expr)
    };
}

macro_rules! unlikely {
    ($expr:expr) => {
        std::intrinsics::unlikely($expr)
    };
}

// 在热路径中使用
fn parse_length(reader: &mut impl Read) -> Result<u64> {
    let first_byte = read_byte(reader)?;
    
    if likely!(first_byte < 0x80) {
        // 最常见的情况：小整数
        Ok(first_byte as u64)
    } else if unlikely!(first_byte == 0xFF) {
        // 罕见情况：特殊值
        handle_special_length(reader)
    } else {
        // 其他情况
        parse_extended_length(reader, first_byte)
    }
}
```

### 6.2 内存优化

**内存预分配**
```rust
pub struct PreallocatedParser {
    string_buffer: String,
    byte_buffer: Vec<u8>,
    entry_buffer: Vec<RdbEntry>,
}

impl PreallocatedParser {
    pub fn with_capacity(estimated_entries: usize) -> Self {
        Self {
            string_buffer: String::with_capacity(1024),
            byte_buffer: Vec::with_capacity(4096),
            entry_buffer: Vec::with_capacity(estimated_entries),
        }
    }
    
    pub fn parse_reusing_buffers(&mut self, data: &[u8]) -> &[RdbEntry] {
        self.entry_buffer.clear();
        
        // 重用缓冲区进行解析
        // ...
        
        &self.entry_buffer
    }
}
```

**内存映射文件**
```rust
use memmap2::MmapOptions;

pub struct MmapRdbParser {
    mmap: memmap2::Mmap,
    offset: usize,
}

impl MmapRdbParser {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        Ok(Self {
            mmap,
            offset: 0,
        })
    }
    
    pub fn parse_entries(&mut self) -> Result<Vec<RdbEntry>> {
        // 直接从内存映射读取，避免系统调用
        let data = &self.mmap[self.offset..];
        self.parse_from_slice(data)
    }
}
```

### 6.3 IO优化

**异步IO**
```rust
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct AsyncRdbParser {
    buffer: BytesMut,
    state: ParserState,
}

impl AsyncRdbParser {
    pub async fn parse_async<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R
    ) -> Result<Option<RdbEntry>> {
        loop {
            match self.state {
                ParserState::ReadingHeader => {
                    self.buffer.reserve(9);
                    reader.read_buf(&mut self.buffer).await?;
                    
                    if self.buffer.len() >= 9 {
                        self.parse_header()?;
                        self.state = ParserState::ReadingEntry;
                    }
                },
                ParserState::ReadingEntry => {
                    // 异步读取条目
                    return self.parse_next_entry(reader).await;
                },
                // ...
            }
        }
    }
}
```

**批量IO**
```rust
pub struct BatchedReader<R> {
    inner: R,
    buffer: Vec<u8>,
    pos: usize,
    end: usize,
}

impl<R: Read> BatchedReader<R> {
    pub fn new(inner: R, buffer_size: usize) -> Self {
        Self {
            inner,
            buffer: vec![0; buffer_size],
            pos: 0,
            end: 0,
        }
    }
    
    pub fn read_batch(&mut self) -> Result<&[u8]> {
        if self.pos >= self.end {
            // 重新填充缓冲区
            self.end = self.inner.read(&mut self.buffer)?;
            self.pos = 0;
        }
        
        Ok(&self.buffer[self.pos..self.end])
    }
}
```

## 7. 并发设计

### 7.1 线程安全

```rust
use std::sync::{Arc, RwLock, Mutex};
use tokio::sync::{mpsc, oneshot};

// 线程安全的解析器
pub struct ConcurrentParser {
    workers: Vec<WorkerHandle>,
    task_queue: Arc<Mutex<VecDeque<ParseTask>>>,
    result_sender: mpsc::UnboundedSender<ParseResult>,
}

struct ParseTask {
    data: Vec<u8>,
    task_id: u64,
    response: oneshot::Sender<ParseResult>,
}

impl ConcurrentParser {
    pub fn new(worker_count: usize) -> Self {
        let task_queue = Arc::new(Mutex::new(VecDeque::new()));
        let (result_sender, result_receiver) = mpsc::unbounded_channel();
        
        let workers = (0..worker_count)
            .map(|id| WorkerHandle::spawn(id, task_queue.clone()))
            .collect();
        
        Self {
            workers,
            task_queue,
            result_sender,
        }
    }
    
    pub async fn parse_concurrent(&self, data: Vec<u8>) -> Result<ParseResult> {
        let (tx, rx) = oneshot::channel();
        let task = ParseTask {
            data,
            task_id: self.next_task_id(),
            response: tx,
        };
        
        self.task_queue.lock().unwrap().push_back(task);
        rx.await.map_err(|_| RedisReplicatorError::Connection(
            "Worker disconnected".to_string()
        ))
    }
}
```

### 7.2 无锁数据结构

```rust
use crossbeam::queue::SegQueue;
use std::sync::atomic::{AtomicU64, Ordering};

// 无锁任务队列
pub struct LockFreeTaskQueue<T> {
    queue: SegQueue<T>,
    pending_count: AtomicU64,
}

impl<T> LockFreeTaskQueue<T> {
    pub fn new() -> Self {
        Self {
            queue: SegQueue::new(),
            pending_count: AtomicU64::new(0),
        }
    }
    
    pub fn push(&self, item: T) {
        self.queue.push(item);
        self.pending_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn pop(&self) -> Option<T> {
        match self.queue.pop() {
            Some(item) => {
                self.pending_count.fetch_sub(1, Ordering::Relaxed);
                Some(item)
            },
            None => None,
        }
    }
    
    pub fn len(&self) -> u64 {
        self.pending_count.load(Ordering::Relaxed)
    }
}
```

## 8. 测试架构

### 8.1 测试分层

```
┌─────────────────────────────────────────────────────────────┐
│                    End-to-End Tests                        │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐  │
│  │ Real Redis  │ Large Files │ Network     │ Performance │  │
│  │ Integration │ Processing  │ Scenarios   │ Regression  │  │
│  └─────────────┴─────────────┴─────────────┴─────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                   Integration Tests                        │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐  │
│  │ Module      │ Error       │ Concurrency │ Resource    │  │
│  │ Interaction │ Handling    │ Testing     │ Management  │  │
│  └─────────────┴─────────────┴─────────────┴─────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                     Unit Tests                             │
│  ┌─────────────┬─────────────┬─────────────┬─────────────┐  │
│  │ Function    │ Edge Cases  │ Error       │ Data        │  │
│  │ Correctness │             │ Conditions  │ Validation  │  │
│  └─────────────┴─────────────┴─────────────┴─────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 8.2 属性测试

```rust
use proptest::prelude::*;

// 生成随机RDB数据
fn arbitrary_rdb_entry() -> impl Strategy<Value = RdbEntry> {
    prop_oneof![
        any::<u32>().prop_map(RdbEntry::SelectDb),
        (any::<String>(), arbitrary_redis_value()).prop_map(|(key, value)| {
            RdbEntry::KeyValue {
                key,
                value,
                expire_ms: None,
                idle: None,
                freq: None,
            }
        }),
        any::<String>().prop_map(RdbEntry::Function),
    ]
}

proptest! {
    #[test]
    fn test_rdb_roundtrip(entry in arbitrary_rdb_entry()) {
        // 序列化然后反序列化，应该得到相同结果
        let serialized = serialize_rdb_entry(&entry)?;
        let deserialized = parse_rdb_entry(&serialized)?;
        prop_assert_eq!(entry, deserialized);
    }
    
    #[test]
    fn test_parser_never_panics(data in prop::collection::vec(any::<u8>(), 0..10000)) {
        // 解析器不应该因为任何输入而panic
        let mut parser = RdbParser::new();
        let _ = parser.parse_stream(std::io::Cursor::new(data));
    }
}
```

### 8.3 基准测试

```rust
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn bench_rdb_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("rdb_parsing");
    
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        let data = generate_rdb_data(*size);
        
        group.bench_with_input(
            BenchmarkId::new("parse_entries", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut parser = RdbParser::new();
                    parser.parse_stream(std::io::Cursor::new(data))
                })
            },
        );
    }
    
    group.finish();
}

fn bench_memory_usage(c: &mut Criterion) {
    c.bench_function("memory_efficiency", |b| {
        b.iter_custom(|iters| {
            let start_memory = get_memory_usage();
            let start_time = std::time::Instant::now();
            
            for _ in 0..iters {
                let mut parser = RdbParser::new();
                let data = generate_large_rdb_data();
                let _ = parser.parse_stream(std::io::Cursor::new(data));
            }
            
            let end_memory = get_memory_usage();
            let duration = start_time.elapsed();
            
            // 记录内存使用情况
            println!("Memory delta: {} bytes", end_memory - start_memory);
            
            duration
        })
    });
}
```

## 9. 部署和运维

### 9.1 监控和指标

```rust
use prometheus::{Counter, Histogram, Gauge, register_counter, register_histogram, register_gauge};

// 定义指标
lazy_static! {
    static ref PARSE_OPERATIONS: Counter = register_counter!(
        "redis_replicator_parse_operations_total",
        "Total number of parse operations"
    ).unwrap();
    
    static ref PARSE_DURATION: Histogram = register_histogram!(
        "redis_replicator_parse_duration_seconds",
        "Time spent parsing data"
    ).unwrap();
    
    static ref ACTIVE_CONNECTIONS: Gauge = register_gauge!(
        "redis_replicator_active_connections",
        "Number of active replication connections"
    ).unwrap();
}

// 在代码中使用指标
impl RdbParser {
    pub fn parse_with_metrics<R: Read>(&mut self, reader: R) -> Result<Vec<RdbEntry>> {
        let _timer = PARSE_DURATION.start_timer();
        PARSE_OPERATIONS.inc();
        
        let result = self.parse_stream(reader);
        
        match &result {
            Ok(entries) => {
                tracing::info!("Parsed {} entries successfully", entries.len());
            },
            Err(e) => {
                tracing::error!("Parse failed: {}", e);
            }
        }
        
        result
    }
}
```

### 9.2 配置管理

```rust
use serde::{Deserialize, Serialize};
use config::{Config, ConfigError, Environment, File};

#[derive(Debug, Deserialize, Serialize)]
pub struct RedisReplicatorConfig {
    pub rdb: RdbConfig,
    pub aof: AofConfig,
    pub replication: ReplicationConfig,
    pub performance: PerformanceConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PerformanceConfig {
    pub worker_threads: usize,
    pub buffer_size: usize,
    pub batch_size: usize,
    pub memory_limit: usize,
}

impl RedisReplicatorConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut config = Config::builder()
            .add_source(File::with_name("config/default"))
            .add_source(File::with_name("config/local").required(false))
            .add_source(Environment::with_prefix("REDIS_REPLICATOR"))
            .build()?;
            
        config.try_deserialize()
    }
}
```

### 9.3 健康检查

```rust
use axum::{response::Json, routing::get, Router};
use serde_json::{json, Value};

#[derive(Clone)]
pub struct HealthChecker {
    parser_status: Arc<RwLock<ParserStatus>>,
    connection_status: Arc<RwLock<ConnectionStatus>>,
}

impl HealthChecker {
    pub async fn health_check(&self) -> Json<Value> {
        let parser_ok = self.parser_status.read().await.is_healthy();
        let connection_ok = self.connection_status.read().await.is_healthy();
        
        let status = if parser_ok && connection_ok {
            "healthy"
        } else {
            "unhealthy"
        };
        
        Json(json!({
            "status": status,
            "parser": parser_ok,
            "connection": connection_ok,
            "timestamp": chrono::Utc::now().to_rfc3339()
        }))
    }
    
    pub fn create_router(self) -> Router {
        Router::new()
            .route("/health", get(move || self.health_check()))
            .route("/metrics", get(prometheus_metrics))
    }
}
```

## 10. 总结

本架构设计文档详细描述了Redis Replicator Rust库的技术架构，包括：

1. **模块化设计**: 清晰的分层架构和模块边界
2. **性能优化**: 零拷贝、SIMD、异步IO等优化技术
3. **内存管理**: 对象池化、缓冲区重用、内存映射
4. **并发安全**: 线程安全设计和无锁数据结构
5. **错误处理**: 分层错误处理和恢复策略
6. **测试策略**: 全面的测试覆盖和性能基准
7. **运维支持**: 监控、配置和健康检查

这个架构设计确保了库的高性能、可靠性和可维护性，为生产环境的使用提供了坚实的基础。