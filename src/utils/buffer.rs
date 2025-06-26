//! Buffer management utilities for efficient memory handling

use crate::error::{RedisReplicatorError, Result};
use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Buffer pool for reusing byte buffers
pub struct BufferPool {
    small_buffers: Arc<Mutex<VecDeque<BytesMut>>>,  // 1KB buffers
    medium_buffers: Arc<Mutex<VecDeque<BytesMut>>>, // 8KB buffers
    large_buffers: Arc<Mutex<VecDeque<BytesMut>>>,  // 64KB buffers
    max_pool_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(max_pool_size: usize) -> Self {
        Self {
            small_buffers: Arc::new(Mutex::new(VecDeque::new())),
            medium_buffers: Arc::new(Mutex::new(VecDeque::new())),
            large_buffers: Arc::new(Mutex::new(VecDeque::new())),
            max_pool_size,
        }
    }

    /// Get a buffer of the appropriate size
    pub fn get_buffer(&self, min_size: usize) -> BytesMut {
        if min_size <= 1024 {
            if let Ok(mut pool) = self.small_buffers.lock() {
                if let Some(mut buf) = pool.pop_front() {
                    buf.clear();
                    if buf.capacity() >= min_size {
                        return buf;
                    }
                }
            }
            BytesMut::with_capacity(1024.max(min_size))
        } else if min_size <= 8192 {
            if let Ok(mut pool) = self.medium_buffers.lock() {
                if let Some(mut buf) = pool.pop_front() {
                    buf.clear();
                    if buf.capacity() >= min_size {
                        return buf;
                    }
                }
            }
            BytesMut::with_capacity(8192.max(min_size))
        } else if min_size <= 65536 {
            if let Ok(mut pool) = self.large_buffers.lock() {
                if let Some(mut buf) = pool.pop_front() {
                    buf.clear();
                    if buf.capacity() >= min_size {
                        return buf;
                    }
                }
            }
            BytesMut::with_capacity(65536.max(min_size))
        } else {
            BytesMut::with_capacity(min_size)
        }
    }

    /// Return a buffer to the pool
    pub fn return_buffer(&self, buf: BytesMut) {
        let capacity = buf.capacity();
        
        if capacity <= 1024 {
            if let Ok(mut pool) = self.small_buffers.lock() {
                if pool.len() < self.max_pool_size {
                    pool.push_back(buf);
                }
            }
        } else if capacity <= 8192 {
            if let Ok(mut pool) = self.medium_buffers.lock() {
                if pool.len() < self.max_pool_size {
                    pool.push_back(buf);
                }
            }
        } else if capacity <= 65536 {
            if let Ok(mut pool) = self.large_buffers.lock() {
                if pool.len() < self.max_pool_size {
                    pool.push_back(buf);
                }
            }
        }
        // Buffers larger than 64KB are not pooled
    }

    /// Get pool statistics
    pub fn stats(&self) -> BufferPoolStats {
        let small_count = self.small_buffers.lock().map(|p| p.len()).unwrap_or(0);
        let medium_count = self.medium_buffers.lock().map(|p| p.len()).unwrap_or(0);
        let large_count = self.large_buffers.lock().map(|p| p.len()).unwrap_or(0);
        
        BufferPoolStats {
            small_buffers: small_count,
            medium_buffers: medium_count,
            large_buffers: large_count,
            total_buffers: small_count + medium_count + large_count,
        }
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(32) // Default to 32 buffers per size class
    }
}

/// Buffer pool statistics
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    /// Number of small buffers
    pub small_buffers: usize,
    /// Number of medium buffers
    pub medium_buffers: usize,
    /// Number of large buffers
    pub large_buffers: usize,
    /// Total number of buffers
    pub total_buffers: usize,
}

/// Managed buffer that automatically returns to pool when dropped
pub struct ManagedBuffer {
    buffer: Option<BytesMut>,
    pool: Arc<BufferPool>,
}

impl ManagedBuffer {
    /// Create a new managed buffer
    pub fn new(pool: Arc<BufferPool>, min_size: usize) -> Self {
        let buffer = pool.get_buffer(min_size);
        Self {
            buffer: Some(buffer),
            pool,
        }
    }

    /// Get a mutable reference to the buffer
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        self.buffer.as_mut().expect("Buffer already taken")
    }

    /// Get an immutable reference to the buffer
    pub fn buffer(&self) -> &BytesMut {
        self.buffer.as_ref().expect("Buffer already taken")
    }

    /// Take the buffer, consuming the managed buffer
    pub fn take(mut self) -> BytesMut {
        self.buffer.take().expect("Buffer already taken")
    }

    /// Get the current length of the buffer
    pub fn len(&self) -> usize {
        self.buffer().len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer().is_empty()
    }

    /// Get the capacity of the buffer
    pub fn capacity(&self) -> usize {
        self.buffer().capacity()
    }
}

impl Drop for ManagedBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.return_buffer(buffer);
        }
    }
}

/// Buffer manager for handling multiple buffers efficiently
pub struct BufferManager {
    pool: Arc<BufferPool>,
    active_buffers: Vec<ManagedBuffer>,
    total_allocated: usize,
    max_memory: usize,
}

impl BufferManager {
    /// Create a new buffer manager
    pub fn new(max_memory: usize) -> Self {
        Self {
            pool: Arc::new(BufferPool::default()),
            active_buffers: Vec::new(),
            total_allocated: 0,
            max_memory,
        }
    }

    /// Create a buffer manager with a custom pool
    pub fn with_pool(pool: Arc<BufferPool>, max_memory: usize) -> Self {
        Self {
            pool,
            active_buffers: Vec::new(),
            total_allocated: 0,
            max_memory,
        }
    }

    /// Allocate a new buffer
    pub fn allocate(&mut self, min_size: usize) -> Result<&mut ManagedBuffer> {
        if self.total_allocated + min_size > self.max_memory {
            return Err(RedisReplicatorError::connection_error(
                "Buffer manager memory limit exceeded"
            ));
        }

        let managed_buffer = ManagedBuffer::new(self.pool.clone(), min_size);
        self.total_allocated += managed_buffer.capacity();
        self.active_buffers.push(managed_buffer);
        
        Ok(self.active_buffers.last_mut().unwrap())
    }

    /// Get the number of active buffers
    pub fn active_count(&self) -> usize {
        self.active_buffers.len()
    }

    /// Get total allocated memory
    pub fn total_allocated(&self) -> usize {
        self.total_allocated
    }

    /// Get memory usage percentage
    pub fn memory_usage_percent(&self) -> f64 {
        (self.total_allocated as f64 / self.max_memory as f64) * 100.0
    }

    /// Clear all buffers
    pub fn clear(&mut self) {
        self.active_buffers.clear();
        self.total_allocated = 0;
    }

    /// Compact buffers by removing empty ones
    pub fn compact(&mut self) {
        let mut new_allocated = 0;
        self.active_buffers.retain(|buf| {
            if buf.is_empty() {
                false
            } else {
                new_allocated += buf.capacity();
                true
            }
        });
        self.total_allocated = new_allocated;
    }

    /// Get buffer pool statistics
    pub fn pool_stats(&self) -> BufferPoolStats {
        self.pool.stats()
    }
}

/// A growable buffer that can efficiently append data
pub struct GrowableBuffer {
    chunks: Vec<Bytes>,
    current_chunk: BytesMut,
    total_len: usize,
    chunk_size: usize,
}

impl GrowableBuffer {
    /// Create a new growable buffer
    pub fn new(initial_capacity: usize) -> Self {
        Self {
            chunks: Vec::new(),
            current_chunk: BytesMut::with_capacity(initial_capacity),
            total_len: 0,
            chunk_size: initial_capacity,
        }
    }

    /// Append data to the buffer
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        let mut remaining = data;
        
        while !remaining.is_empty() {
            let available = self.current_chunk.capacity() - self.current_chunk.len();
            
            if available == 0 {
                // Current chunk is full, move it to chunks and create a new one
                let chunk = std::mem::replace(
                    &mut self.current_chunk,
                    BytesMut::with_capacity(self.chunk_size)
                );
                self.chunks.push(chunk.freeze());
            }
            
            let to_copy = std::cmp::min(remaining.len(), 
                self.current_chunk.capacity() - self.current_chunk.len());
            
            self.current_chunk.extend_from_slice(&remaining[..to_copy]);
            remaining = &remaining[to_copy..];
            self.total_len += to_copy;
        }
    }

    /// Get the total length of the buffer
    pub fn len(&self) -> usize {
        self.total_len
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    /// Convert to a single contiguous byte vector
    pub fn to_vec(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.total_len);
        
        for chunk in &self.chunks {
            result.extend_from_slice(chunk);
        }
        
        result.extend_from_slice(&self.current_chunk);
        result
    }

    /// Convert to Bytes (may require copying if fragmented)
    pub fn to_bytes(&self) -> Bytes {
        if self.chunks.is_empty() {
            self.current_chunk.clone().freeze()
        } else {
            Bytes::from(self.to_vec())
        }
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.chunks.clear();
        self.current_chunk.clear();
        self.total_len = 0;
    }

    /// Get the number of chunks
    pub fn chunk_count(&self) -> usize {
        self.chunks.len() + if self.current_chunk.is_empty() { 0 } else { 1 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(2);
        
        // Get a small buffer
        let buf1 = pool.get_buffer(512);
        assert!(buf1.capacity() >= 512);
        
        // Return it
        pool.return_buffer(buf1);
        
        // Get it back
        let buf2 = pool.get_buffer(512);
        assert!(buf2.capacity() >= 512);
        
        let stats = pool.stats();
        assert_eq!(stats.small_buffers, 0); // buf2 is still out
    }

    #[test]
    fn test_managed_buffer() {
        let pool = Arc::new(BufferPool::new(2));
        let mut managed = ManagedBuffer::new(pool.clone(), 1024);
        
        managed.buffer_mut().extend_from_slice(b"hello");
        assert_eq!(managed.len(), 5);
        assert!(!managed.is_empty());
        
        let buffer = managed.take();
        assert_eq!(buffer.len(), 5);
    }

    #[test]
    fn test_buffer_manager() {
        let mut manager = BufferManager::new(10240); // 10KB limit
        
        let buf1 = manager.allocate(1024).unwrap();
        assert_eq!(manager.active_count(), 1);
        assert!(manager.total_allocated() >= 1024);
        
        let buf2 = manager.allocate(2048).unwrap();
        assert_eq!(manager.active_count(), 2);
        
        // Should fail due to memory limit
        assert!(manager.allocate(8192).is_err());
        
        manager.clear();
        assert_eq!(manager.active_count(), 0);
        assert_eq!(manager.total_allocated(), 0);
    }

    #[test]
    fn test_growable_buffer() {
        let mut buffer = GrowableBuffer::new(4);
        
        buffer.extend_from_slice(b"hello");
        assert_eq!(buffer.len(), 5);
        
        buffer.extend_from_slice(b" world");
        assert_eq!(buffer.len(), 11);
        
        let data = buffer.to_vec();
        assert_eq!(data, b"hello world");
        
        assert!(buffer.chunk_count() >= 1);
        
        buffer.clear();
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_buffer_pool_size_classes() {
        let pool = BufferPool::new(5);
        
        // Test different size classes
        let small = pool.get_buffer(100);   // Should get 1KB buffer
        let medium = pool.get_buffer(2000); // Should get 8KB buffer
        let large = pool.get_buffer(20000); // Should get 64KB buffer
        let huge = pool.get_buffer(100000); // Should get exact size
        
        assert!(small.capacity() >= 100);
        assert!(medium.capacity() >= 2000);
        assert!(large.capacity() >= 20000);
        assert!(huge.capacity() >= 100000);
        
        // Return buffers
        pool.return_buffer(small);
        pool.return_buffer(medium);
        pool.return_buffer(large);
        pool.return_buffer(huge); // This won't be pooled due to size
        
        let stats = pool.stats();
        assert_eq!(stats.total_buffers, 3); // huge buffer not pooled
    }
}