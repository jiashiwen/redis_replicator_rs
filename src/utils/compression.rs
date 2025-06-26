//! Compression and decompression utilities

use crate::error::{RedisReplicatorError, Result};
use flate2::read::GzDecoder;
use std::io::Read;

/// LZF decompression implementation
/// Based on the LZF algorithm used by Redis
pub fn lzf_decompress(compressed: &[u8], uncompressed_len: usize) -> Result<Vec<u8>> {
    if compressed.is_empty() {
        return Ok(Vec::new());
    }

    let mut output = Vec::with_capacity(uncompressed_len);
    let mut input_pos = 0;
    
    while input_pos < compressed.len() && output.len() < uncompressed_len {
        let ctrl = compressed[input_pos];
        input_pos += 1;
        
        if ctrl < 32 {
            // Literal run
            let len = (ctrl + 1) as usize;
            if input_pos + len > compressed.len() {
                return Err(RedisReplicatorError::compression_error(
                    "LZF: Not enough input data for literal run"
                ));
            }
            
            if output.len() + len > uncompressed_len {
                return Err(RedisReplicatorError::compression_error(
                    "LZF: Output would exceed expected length"
                ));
            }
            
            output.extend_from_slice(&compressed[input_pos..input_pos + len]);
            input_pos += len;
        } else {
            // Back reference
            let len = (ctrl >> 5) as usize;
            let len = if len == 7 {
                if input_pos >= compressed.len() {
                    return Err(RedisReplicatorError::compression_error(
                        "LZF: Not enough input data for extended length"
                    ));
                }
                let extra = compressed[input_pos] as usize;
                input_pos += 1;
                len + extra
            } else {
                len
            };
            
            if input_pos >= compressed.len() {
                return Err(RedisReplicatorError::compression_error(
                    "LZF: Not enough input data for offset"
                ));
            }
            
            let offset = (((ctrl & 31) as usize) << 8) | (compressed[input_pos] as usize);
            input_pos += 1;
            
            if offset == 0 || offset > output.len() {
                return Err(RedisReplicatorError::compression_error(
                    "LZF: Invalid back reference offset"
                ));
            }
            
            let actual_len = len + 2;
            if output.len() + actual_len > uncompressed_len {
                return Err(RedisReplicatorError::compression_error(
                    "LZF: Output would exceed expected length"
                ));
            }
            
            // Copy from back reference
            let start_pos = output.len() - offset;
            for i in 0..actual_len {
                let byte = output[start_pos + (i % offset)];
                output.push(byte);
            }
        }
    }
    
    if output.len() != uncompressed_len {
        return Err(RedisReplicatorError::compression_error(
            &format!("LZF: Expected {} bytes, got {}", uncompressed_len, output.len())
        ));
    }
    
    Ok(output)
}

/// LZ4 decompression
pub fn lz4_decompress(compressed: &[u8], uncompressed_len: usize) -> Result<Vec<u8>> {
    let mut output = vec![0u8; uncompressed_len];
    let decompressed_size = lz4::block::decompress_to_buffer(
        compressed,
        Some(uncompressed_len as i32),
        &mut output,
    ).map_err(|e| RedisReplicatorError::compression_error(
        &format!("LZ4 decompression failed: {}", e)
    ))?;
    
    if decompressed_size != uncompressed_len {
        return Err(RedisReplicatorError::compression_error(
            &format!("LZ4: Expected {} bytes, got {}", uncompressed_len, decompressed_size)
        ));
    }
    
    output.truncate(decompressed_size);
    Ok(output)
}

/// GZIP decompression
pub fn gzip_decompress(compressed: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(compressed);
    let mut output = Vec::new();
    
    decoder.read_to_end(&mut output)
        .map_err(|e| RedisReplicatorError::compression_error(
            &format!("GZIP decompression failed: {}", e)
        ))?;
    
    Ok(output)
}

/// Detect compression type from magic bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression
    None,
    /// LZF compression
    Lzf,
    /// LZ4 compression
    Lz4,
    /// Gzip compression
    Gzip,
}

impl CompressionType {
    /// Detect compression type from the first few bytes
    pub fn detect(data: &[u8]) -> Self {
        if data.len() < 2 {
            return CompressionType::None;
        }
        
        // GZIP magic number
        if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
            return CompressionType::Gzip;
        }
        
        // LZ4 magic number (if present)
        if data.len() >= 4 && &data[0..4] == b"\x04\"M\x18" {
            return CompressionType::Lz4;
        }
        
        // For LZF, we need to make a heuristic guess
        // LZF typically starts with control bytes < 32 for literals
        // or >= 32 for back references
        if data[0] < 32 || data[0] >= 32 {
            // This is a weak heuristic, in practice Redis tells us the compression type
            return CompressionType::Lzf;
        }
        
        CompressionType::None
    }
}

/// Generic decompression function
pub fn decompress(data: &[u8], compression_type: CompressionType, expected_len: Option<usize>) -> Result<Vec<u8>> {
    match compression_type {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Lzf => {
            let len = expected_len.ok_or_else(|| {
                RedisReplicatorError::compression_error("LZF decompression requires expected length")
            })?;
            lzf_decompress(data, len)
        },
        CompressionType::Lz4 => {
            let len = expected_len.ok_or_else(|| {
                RedisReplicatorError::compression_error("LZ4 decompression requires expected length")
            })?;
            lz4_decompress(data, len)
        },
        CompressionType::Gzip => gzip_decompress(data),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lzf_decompress_literal() {
        // Simple literal run: control byte 0x03 (3+1=4 literals) followed by "test"
        let compressed = vec![0x03, b't', b'e', b's', b't'];
        let result = lzf_decompress(&compressed, 4).unwrap();
        assert_eq!(result, b"test");
    }

    #[test]
    fn test_lzf_decompress_empty() {
        let result = lzf_decompress(&[], 0).unwrap();
        assert_eq!(result, Vec::<u8>::new());
    }

    #[test]
    fn test_compression_type_detection() {
        // GZIP magic
        let gzip_data = vec![0x1f, 0x8b, 0x08, 0x00];
        assert_eq!(CompressionType::detect(&gzip_data), CompressionType::Gzip);
        
        // LZ4 magic
        let lz4_data = vec![0x04, 0x22, 0x4d, 0x18];
        assert_eq!(CompressionType::detect(&lz4_data), CompressionType::Lz4);
        
        // Too short
        let short_data = vec![0x1f];
        assert_eq!(CompressionType::detect(&short_data), CompressionType::None);
    }

    #[test]
    fn test_decompress_none() {
        let data = b"hello world";
        let result = decompress(data, CompressionType::None, None).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_lzf_error_cases() {
        // Not enough input for literal run
        let compressed = vec![0x03, b't', b'e']; // Says 4 bytes but only has 2
        assert!(lzf_decompress(&compressed, 4).is_err());
        
        // Output exceeds expected length
        let compressed = vec![0x03, b't', b'e', b's', b't'];
        assert!(lzf_decompress(&compressed, 2).is_err());
    }

    #[test]
    fn test_gzip_decompress() {
        // Create a simple gzip compressed data
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;
        
        let original = b"Hello, World!";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = encoder.finish().unwrap();
        
        let decompressed = gzip_decompress(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }
}