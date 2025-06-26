//! I/O utilities for reading Redis data formats

use crate::error::{RedisReplicatorError, Result};
use bytes::BytesMut;
use std::io::Read;
use tokio::io::AsyncRead;

/// Read exact number of bytes from a reader
pub fn read_exact<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<()> {
    reader.read_exact(buf)
        .map_err(|e| RedisReplicatorError::Io(e))
}

/// Read a single byte
pub fn read_u8<R: Read>(reader: &mut R) -> Result<u8> {
    let mut buf = [0u8; 1];
    read_exact(reader, &mut buf)?;
    Ok(buf[0])
}

/// Read a 16-bit little-endian integer
pub fn read_u16_le<R: Read>(reader: &mut R) -> Result<u16> {
    let mut buf = [0u8; 2];
    read_exact(reader, &mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

/// Read a 32-bit little-endian integer
pub fn read_u32_le<R: Read>(reader: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    read_exact(reader, &mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

/// Read a 64-bit little-endian integer
pub fn read_u64_le<R: Read>(reader: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    read_exact(reader, &mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

/// Read a 16-bit big-endian integer
pub fn read_u16_be<R: Read>(reader: &mut R) -> Result<u16> {
    let mut buf = [0u8; 2];
    read_exact(reader, &mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

/// Read a 32-bit big-endian integer
pub fn read_u32_be<R: Read>(reader: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    read_exact(reader, &mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

/// Read a 64-bit big-endian integer
pub fn read_u64_be<R: Read>(reader: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    read_exact(reader, &mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

/// Read a variable-length integer (Redis length encoding)
pub fn read_length<R: Read>(reader: &mut R) -> Result<(u64, bool)> {
    let first_byte = read_u8(reader)?;
    
    match first_byte >> 6 {
        0 => {
            // 6-bit length
            Ok(((first_byte & 0x3F) as u64, false))
        },
        1 => {
            // 14-bit length
            let second_byte = read_u8(reader)?;
            let length = (((first_byte & 0x3F) as u64) << 8) | (second_byte as u64);
            Ok((length, false))
        },
        2 => {
            // 32-bit length
            let length = read_u32_be(reader)? as u64;
            Ok((length, false))
        },
        3 => {
            // Special encoding
            let encoding = first_byte & 0x3F;
            Ok((encoding as u64, true))
        },
        _ => unreachable!()
    }
}

/// Read a Redis string with length encoding
pub fn read_string<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let (length, is_encoded) = read_length(reader)?;
    
    if is_encoded {
        // Handle special encodings
        match length {
            0 => {
                // 8-bit integer
                let val = read_u8(reader)? as i8;
                Ok(val.to_string().into_bytes())
            },
            1 => {
                // 16-bit integer
                let val = read_u16_le(reader)? as i16;
                Ok(val.to_string().into_bytes())
            },
            2 => {
                // 32-bit integer
                let val = read_u32_le(reader)? as i32;
                Ok(val.to_string().into_bytes())
            },
            3 => {
                // LZF compressed string
                let compressed_len = read_length(reader)?.0 as usize;
                let uncompressed_len = read_length(reader)?.0 as usize;
                
                let mut compressed = vec![0u8; compressed_len];
                read_exact(reader, &mut compressed)?;
                
                crate::utils::compression::lzf_decompress(&compressed, uncompressed_len)
            },
            _ => Err(RedisReplicatorError::parse_error(
                0, format!("Unknown string encoding: {}", length)
            ))
        }
    } else {
        // Regular string
        let mut buf = vec![0u8; length as usize];
        read_exact(reader, &mut buf)?;
        Ok(buf)
    }
}

/// Read a Redis double (8 bytes)
pub fn read_double<R: Read>(reader: &mut R) -> Result<f64> {
    let bits = read_u64_le(reader)?;
    Ok(f64::from_bits(bits))
}

/// Read a Redis float (4 bytes)
pub fn read_float<R: Read>(reader: &mut R) -> Result<f32> {
    let bits = read_u32_le(reader)?;
    Ok(f32::from_bits(bits))
}

/// Async versions of the read functions
pub mod async_io {
    use super::*;
    use tokio::io::AsyncReadExt;
    
    /// Async read exact number of bytes
    pub async fn read_exact<R: AsyncRead + Unpin>(reader: &mut R, buf: &mut [u8]) -> Result<()> {
        reader.read_exact(buf).await
            .map_err(|e| RedisReplicatorError::Io(e))?;
        Ok(())
    }
    
    /// Async read a single byte
    pub async fn read_u8<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u8> {
        let mut buf = [0u8; 1];
        read_exact(reader, &mut buf).await?;
        Ok(buf[0])
    }
    
    /// Async read a 16-bit little-endian integer
    pub async fn read_u16_le<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u16> {
        let mut buf = [0u8; 2];
        read_exact(reader, &mut buf).await?;
        Ok(u16::from_le_bytes(buf))
    }
    
    /// Async read a 32-bit little-endian integer
    pub async fn read_u32_le<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u32> {
        let mut buf = [0u8; 4];
        read_exact(reader, &mut buf).await?;
        Ok(u32::from_le_bytes(buf))
    }
    
    /// Async read a 64-bit little-endian integer
    pub async fn read_u64_le<R: AsyncRead + Unpin>(reader: &mut R) -> Result<u64> {
        let mut buf = [0u8; 8];
        read_exact(reader, &mut buf).await?;
        Ok(u64::from_le_bytes(buf))
    }
    
    /// Async read a variable-length integer
    pub async fn read_length<R: AsyncRead + Unpin>(reader: &mut R) -> Result<(u64, bool)> {
        let first_byte = read_u8(reader).await?;
        
        match first_byte >> 6 {
            0 => {
                Ok(((first_byte & 0x3F) as u64, false))
            },
            1 => {
                let second_byte = read_u8(reader).await?;
                let length = (((first_byte & 0x3F) as u64) << 8) | (second_byte as u64);
                Ok((length, false))
            },
            2 => {
                let mut buf = [0u8; 4];
                read_exact(reader, &mut buf).await?;
                let length = u32::from_be_bytes(buf) as u64;
                Ok((length, false))
            },
            3 => {
                let encoding = first_byte & 0x3F;
                Ok((encoding as u64, true))
            },
            _ => unreachable!()
        }
    }
    
    /// Async read a Redis string
    pub async fn read_string<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
        let (length, is_encoded) = read_length(reader).await?;
        
        if is_encoded {
            match length {
                0 => {
                    let val = read_u8(reader).await? as i8;
                    Ok(val.to_string().into_bytes())
                },
                1 => {
                    let val = read_u16_le(reader).await? as i16;
                    Ok(val.to_string().into_bytes())
                },
                2 => {
                    let val = read_u32_le(reader).await? as i32;
                    Ok(val.to_string().into_bytes())
                },
                3 => {
                    let compressed_len = read_length(reader).await?.0 as usize;
                    let uncompressed_len = read_length(reader).await?.0 as usize;
                    
                    let mut compressed = vec![0u8; compressed_len];
                    read_exact(reader, &mut compressed).await?;
                    
                    crate::utils::compression::lzf_decompress(&compressed, uncompressed_len)
                },
                _ => Err(RedisReplicatorError::parse_error(
                    0, format!("Unknown string encoding: {}", length)
                ))
            }
        } else {
            let mut buf = vec![0u8; length as usize];
            read_exact(reader, &mut buf).await?;
            Ok(buf)
        }
    }
}

/// Buffer utilities for working with bytes
pub struct BufferReader {
    buffer: BytesMut,
    position: usize,
}

impl BufferReader {
    /// Create a new buffer reader
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            buffer: BytesMut::from(&data[..]),
            position: 0,
        }
    }
    
    /// Get remaining bytes
    pub fn remaining(&self) -> usize {
        self.buffer.len() - self.position
    }
    
    /// Check if there are more bytes to read
    pub fn has_remaining(&self) -> bool {
        self.position < self.buffer.len()
    }
    
    /// Read a single byte
    pub fn read_u8(&mut self) -> Result<u8> {
        if self.position >= self.buffer.len() {
            return Err(RedisReplicatorError::parse_error(
                self.position as u64, "Unexpected end of buffer".to_string()
            ));
        }
        
        let byte = self.buffer[self.position];
        self.position += 1;
        Ok(byte)
    }
    
    /// Read multiple bytes
    pub fn read_bytes(&mut self, len: usize) -> Result<&[u8]> {
        if self.position + len > self.buffer.len() {
            return Err(RedisReplicatorError::parse_error(
                self.position as u64, format!("Not enough bytes: need {}, have {}", len, self.remaining())
            ));
        }
        
        let start = self.position;
        self.position += len;
        Ok(&self.buffer[start..self.position])
    }
    
    /// Skip bytes
    pub fn skip(&mut self, len: usize) -> Result<()> {
        if self.position + len > self.buffer.len() {
            return Err(RedisReplicatorError::parse_error(
                self.position as u64, format!("Cannot skip {} bytes", len)
            ));
        }
        
        self.position += len;
        Ok(())
    }
    
    /// Get current position
    pub fn position(&self) -> usize {
        self.position
    }
    
    /// Seek to position
    pub fn seek(&mut self, pos: usize) -> Result<()> {
        if pos > self.buffer.len() {
            return Err(RedisReplicatorError::parse_error(
                pos as u64, "Seek position out of bounds".to_string()
            ));
        }
        
        self.position = pos;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_integers() {
        let data = vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0];
        let mut cursor = Cursor::new(&data);
        
        assert_eq!(read_u8(&mut cursor).unwrap(), 0x12);
        cursor.set_position(0);
        
        assert_eq!(read_u16_le(&mut cursor).unwrap(), 0x3412);
        cursor.set_position(0);
        
        assert_eq!(read_u32_le(&mut cursor).unwrap(), 0x78563412);
        cursor.set_position(0);
        
        assert_eq!(read_u64_le(&mut cursor).unwrap(), 0xF0DEBC9A78563412);
    }

    #[test]
    fn test_read_length_encoding() {
        // 6-bit length: 0b00111111 = 63
        let data = vec![0x3F];
        let mut cursor = Cursor::new(&data);
        let (length, is_encoded) = read_length(&mut cursor).unwrap();
        assert_eq!(length, 63);
        assert!(!is_encoded);
        
        // 14-bit length: 0b01000000 0x12 0x34 = (0 << 8) | 0x34 = 52
        let data = vec![0x40, 0x34];
        let mut cursor = Cursor::new(&data);
        let (length, is_encoded) = read_length(&mut cursor).unwrap();
        assert_eq!(length, 52);
        assert!(!is_encoded);
        
        // Special encoding: 0b11000001 = encoding 1
        let data = vec![0xC1];
        let mut cursor = Cursor::new(&data);
        let (length, is_encoded) = read_length(&mut cursor).unwrap();
        assert_eq!(length, 1);
        assert!(is_encoded);
    }

    #[test]
    fn test_buffer_reader() {
        let data = vec![1, 2, 3, 4, 5];
        let mut reader = BufferReader::new(data);
        
        assert_eq!(reader.remaining(), 5);
        assert!(reader.has_remaining());
        
        assert_eq!(reader.read_u8().unwrap(), 1);
        assert_eq!(reader.position(), 1);
        
        let bytes = reader.read_bytes(2).unwrap();
        assert_eq!(bytes, &[2, 3]);
        assert_eq!(reader.position(), 3);
        
        reader.skip(1).unwrap();
        assert_eq!(reader.position(), 4);
        assert_eq!(reader.read_u8().unwrap(), 5);
        
        assert!(!reader.has_remaining());
    }

    #[tokio::test]
    async fn test_async_read() {
        let data = vec![0x12, 0x34, 0x56, 0x78];
        let mut cursor = Cursor::new(&data);
        
        let byte = async_io::read_u8(&mut cursor).await.unwrap();
        assert_eq!(byte, 0x12);
        
        let word = async_io::read_u16_le(&mut cursor).await.unwrap();
        assert_eq!(word, 0x5634);
    }
}