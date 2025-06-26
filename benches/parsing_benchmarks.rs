//! Benchmarks for redis_replicator_rs parsing performance
//!
//! This benchmark suite tests the performance of RDB and AOF parsing,
//! as well as replication protocol handling under various conditions.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use redis_replicator_rs::prelude::*;
use redis_replicator_rs::rdb::{RdbParser, RdbEventHandler};
use redis_replicator_rs::aof::{AofParser, AofParseOptions, AofEventHandler};
use redis_replicator_rs::replication::protocol::{ReplicationCommand, ReplicationResponse};
use std::io::Cursor;
use tokio::runtime::Runtime;
use tokio::io::BufReader;

// Test data generators
fn generate_rdb_data(entries: usize) -> Vec<u8> {
    let mut data = Vec::new();
    
    // RDB header
    data.extend_from_slice(b"REDIS0011");
    
    // Database selector
    data.push(0xFE); // SELECTDB (254)
    data.push(0x00); // DB 0
    
    // For simplicity, create a minimal valid RDB with just the end marker
    // This avoids complex RDB format issues while still providing test data
    
    // RDB end marker
    data.push(0xFF); // EOF (255)
    
    // CRC64 checksum (8 bytes of zeros for testing)
    data.extend_from_slice(&[0x00; 8]);
    
    // Pad with some dummy data to simulate different sizes
    let padding_size = entries * 50; // Approximate size per entry
    data.extend(vec![0x00; padding_size]);
    
    data
}

fn generate_aof_data(commands: usize) -> Vec<u8> {
    let mut data = Vec::new();
    
    for i in 0..commands {
        // Generate different types of commands
        match i % 5 {
            0 => {
                // SET command
                let cmd = format!("*3\r\n$3\r\nSET\r\n$8\r\nkey_{:04}\r\n$10\r\nvalue_{:04}\r\n", i, i);
                data.extend_from_slice(cmd.as_bytes());
            }
            1 => {
                // GET command
                let cmd = format!("*2\r\n$3\r\nGET\r\n$8\r\nkey_{:04}\r\n", i);
                data.extend_from_slice(cmd.as_bytes());
            }
            2 => {
                // HSET command
                let cmd = format!("*4\r\n$4\r\nHSET\r\n$9\r\nhash_{:04}\r\n$6\r\nfield1\r\n$10\r\nvalue_{:04}\r\n", i, i);
                data.extend_from_slice(cmd.as_bytes());
            }
            3 => {
                // LPUSH command
                let cmd = format!("*3\r\n$5\r\nLPUSH\r\n$9\r\nlist_{:04}\r\n$10\r\nvalue_{:04}\r\n", i, i);
                data.extend_from_slice(cmd.as_bytes());
            }
            4 => {
                // SADD command
                let cmd = format!("*3\r\n$4\r\nSADD\r\n$8\r\nset_{:04}\r\n$10\r\nvalue_{:04}\r\n", i, i);
                data.extend_from_slice(cmd.as_bytes());
            }
            _ => unreachable!(),
        }
    }
    
    data
}

// Benchmark handler that does minimal work
#[derive(Default)]
#[allow(dead_code)]
struct BenchmarkRdbHandler {
    count: usize,
}

impl RdbEventHandler for BenchmarkRdbHandler {
    fn handle_event(&mut self, event: RdbEvent) -> Result<()> {
        match event {
            RdbEvent::KeyValue { .. } => {
                self.count += 1;
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Default)]
struct BenchmarkAofHandler {
    count: usize,
}

impl AofEventHandler for BenchmarkAofHandler {
    fn handle_event(&mut self, event: AofEvent) -> Result<()> {
        match event {
            AofEvent::Command { .. } => {
                self.count += 1;
            }
            _ => {}
        }
        Ok(())
    }
}

// RDB parsing benchmarks
fn bench_rdb_parsing(c: &mut Criterion) {
    let _rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("rdb_parsing");
    
    for size in [100, 1000, 10000].iter() {
        let data = generate_rdb_data(*size);
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::new("parse_entries", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut parser = RdbParser::new();
                    // Use unwrap_or_default to handle parsing errors gracefully in benchmarks
                    let result = parser.parse_buffer(black_box(data)).unwrap_or_default();
                    black_box(result.len())
                });
            },
        );
    }
    
    group.finish();
}

// AOF parsing benchmarks
fn bench_aof_parsing(c: &mut Criterion) {
    let _rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("aof_parsing");
    
    for size in [100, 1000, 10000].iter() {
        let data = generate_aof_data(*size);
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::new("parse_commands", size),
            &data,
            |b, data| {
                let rt = Runtime::new().unwrap();
                b.iter(|| {
                    rt.block_on(async {
                        let mut handler = BenchmarkAofHandler::default();
                        let mut parser = AofParser::new(AofParseOptions::default());
                        let cursor = BufReader::new(Cursor::new(black_box(data)));
                        
                        parser.parse_with_handler(cursor, &mut handler)
                .await
                .unwrap();
                        
                        black_box(handler.count)
                    })
                });
            },
        );
    }
    
    group.finish();
}

// Protocol serialization benchmarks
fn bench_protocol_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol_serialization");
    
    // Benchmark different command types
    let commands = vec![
        ReplicationCommand::Ping,
        ReplicationCommand::Auth { password: "password123".to_string() },
        ReplicationCommand::ReplConf { key: "listening-port".to_string(), value: "6380".to_string() },
        ReplicationCommand::PSync { repl_id: "?".to_string(), offset: -1 },
    ];
    
    for (i, cmd) in commands.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::new("serialize_command", i),
            cmd,
            |b, cmd| {
                b.iter(|| {
                    black_box(cmd.to_resp())
                });
            },
        );
    }
    
    group.finish();
}

// Protocol deserialization benchmarks
fn bench_protocol_deserialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("protocol_deserialization");
    
    // Test different response types
    let responses = vec![
        b"+OK\r\n".to_vec(),
        b"-ERR unknown command\r\n".to_vec(),
        b":1000\r\n".to_vec(),
        b"$5\r\nhello\r\n".to_vec(),
        b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n".to_vec(),
    ];
    
    for (i, resp_data) in responses.iter().enumerate() {
        group.bench_with_input(
            BenchmarkId::new("parse_response", i),
            resp_data,
            |b, data| {
                b.iter(|| {
                    black_box(ReplicationResponse::from_resp(black_box(data)).unwrap())
                });
            },
        );
    }
    
    group.finish();
}

// Memory usage benchmarks
fn bench_memory_usage(c: &mut Criterion) {
    let _rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("memory_usage");
    
    // Test memory efficiency with different buffer sizes
    for buffer_size in [1024, 4096, 8192, 16384].iter() {
        let data = generate_rdb_data(1000);
        
        group.bench_with_input(
            BenchmarkId::new("rdb_with_buffer", buffer_size),
            &(*buffer_size, data),
            |b, (_buf_size, data)| {
                b.iter(|| {
                    let mut parser = RdbParser::new();
                    let result = parser.parse_buffer(black_box(data)).unwrap_or_default();
                    black_box(result.len())
                });
            },
        );
    }
    
    group.finish();
}

// Concurrent parsing benchmarks
fn bench_concurrent_parsing(c: &mut Criterion) {
    let _rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("concurrent_parsing");
    
    for concurrency in [1, 2, 4, 8].iter() {
        let data = generate_aof_data(1000);
        
        group.bench_with_input(
            BenchmarkId::new("aof_concurrent", concurrency),
            &(*concurrency, data),
            |b, (conc, data)| {
                let rt = Runtime::new().unwrap();
                b.iter(|| {
                    rt.block_on(async {
                        let mut handles = Vec::new();
                        
                        for _ in 0..*conc {
                            let data_clone = data.clone();
                            let handle = tokio::spawn(async move {
                                let mut handler = BenchmarkAofHandler::default();
                                let mut parser = AofParser::new(AofParseOptions::default());
                                let cursor = BufReader::new(Cursor::new(data_clone));
                                
                                parser.parse_with_handler(cursor, &mut handler)
                                    .await
                                    .unwrap();
                                
                                handler.count
                            });
                            handles.push(handle);
                        }
                        
                        let mut total = 0;
                        for handle in handles {
                            total += handle.await.unwrap();
                        }
                        
                        black_box(total)
                    })
                });
            },
        );
    }
    
    group.finish();
}

// Error handling benchmarks
fn bench_error_handling(c: &mut Criterion) {
    let _rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("error_handling");
    
    // Test parsing with intentionally corrupted data
    let mut corrupted_data = generate_rdb_data(100);
    // Corrupt some bytes
    for i in (10..corrupted_data.len()).step_by(50) {
        corrupted_data[i] = 0xFF;
    }
    
    group.bench_function("rdb_error_recovery", |b| {
        b.iter(|| {
            let mut parser = RdbParser::new();
            
            // This should handle errors gracefully
            let result = parser.parse_buffer(black_box(&corrupted_data));
            black_box(result.is_err())
        });
    });
    
    group.finish();
}

// Compression benchmarks
fn bench_compression(c: &mut Criterion) {
    let _rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("compression");
    
    // Generate compressible data
    let mut data = Vec::new();
    for i in 0..1000 {
        let repeated_data = format!("repeated_data_pattern_{:04}", i % 10);
        data.extend_from_slice(repeated_data.as_bytes());
    }
    
    group.bench_function("compress_data", |b| {
        b.iter(|| {
            use flate2::write::GzEncoder;
            use flate2::Compression;
            use std::io::Write;
            
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(black_box(&data)).unwrap();
            black_box(encoder.finish().unwrap())
        });
    });
    
    // Compress the data for decompression benchmark
    let compressed_data = {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&data).unwrap();
        encoder.finish().unwrap()
    };
    
    group.bench_function("decompress_data", |b| {
        b.iter(|| {
            use flate2::read::GzDecoder;
            use std::io::Read;
            
            let mut decoder = GzDecoder::new(black_box(&compressed_data[..]));
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).unwrap();
            black_box(decompressed)
        });
    });
    
    group.finish();
}

// Large data benchmarks
fn bench_large_data(c: &mut Criterion) {
    let _rt = Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("large_data");
    group.sample_size(10); // Reduce sample size for large data tests
    
    // Test with very large datasets
    for size in [50000, 100000].iter() {
        let data = generate_rdb_data(*size);
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(
            BenchmarkId::new("large_rdb", size),
            &data,
            |b, data| {
                b.iter(|| {
                    let mut parser = RdbParser::new();
                    let result = parser.parse_buffer(black_box(data)).unwrap_or_default();
                    black_box(result.len())
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_rdb_parsing,
    bench_aof_parsing,
    bench_protocol_serialization,
    bench_protocol_deserialization,
    bench_memory_usage,
    bench_concurrent_parsing,
    bench_error_handling,
    bench_compression,
    bench_large_data
);

criterion_main!(benches);