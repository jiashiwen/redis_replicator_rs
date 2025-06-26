//! RDB parser implementation

use crate::error::{RedisReplicatorError, Result};
use crate::types::{Entry, RdbEntry, RedisValue, RdbType, RdbOpcode};
use crate::utils::io::BufferReader;
use crate::utils::current_timestamp_ms;
use super::{RdbInfo, RdbStats};
use std::collections::HashMap;

use tokio::io::{AsyncRead, AsyncReadExt};

/// RDB parsing options
#[derive(Debug, Clone)]
pub struct RdbParseOptions {
    /// Skip expired keys
    pub skip_expired: bool,
    /// Skip auxiliary fields
    pub skip_aux_fields: bool,
    /// Maximum memory usage during parsing (in bytes)
    pub max_memory: usize,
    /// Verify checksums
    pub verify_checksums: bool,
    /// Database filter (None means all databases)
    pub database_filter: Option<Vec<u32>>,
    /// Key pattern filter (regex)
    pub key_pattern: Option<String>,
    /// Maximum key size to process
    pub max_key_size: usize,
    /// Maximum value size to process
    pub max_value_size: usize,
}

impl Default for RdbParseOptions {
    fn default() -> Self {
        Self {
            skip_expired: true,
            skip_aux_fields: false,
            max_memory: 1024 * 1024 * 1024, // 1GB
            verify_checksums: true,
            database_filter: None,
            key_pattern: None,
            max_key_size: 1024 * 1024, // 1MB
            max_value_size: 16 * 1024 * 1024, // 16MB
        }
    }
}

/// RDB parser state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParserState {
    Header,
    Database,
    KeyValue,
    Checksum,
    Complete,
}

/// RDB parser
pub struct RdbParser {
    options: RdbParseOptions,
    state: ParserState,
    current_database: u32,
    stats: RdbStats,
    start_time: u64,
    #[allow(dead_code)]
    checksum_enabled: bool,
    crc: u64,
}

impl RdbParser {
    /// Create a new RDB parser with default options
    pub fn new() -> Self {
        Self::with_options(RdbParseOptions::default())
    }

    /// Create a new RDB parser with custom options
    pub fn with_options(options: RdbParseOptions) -> Self {
        Self {
            options,
            state: ParserState::Header,
            current_database: 0,
            stats: RdbStats::new(),
            start_time: current_timestamp_ms(),
            checksum_enabled: false,
            crc: 0,
        }
    }

    /// Parse RDB data from a buffer
    pub fn parse_buffer(&mut self, data: &[u8]) -> Result<Vec<Entry>> {
        let mut reader = BufferReader::new(data.to_vec());
        let mut entries = Vec::new();
        
        self.parse_with_reader(&mut reader, &mut |entry| {
            entries.push(entry);
            Ok(())
        })?;
        
        Ok(entries)
    }

    /// Parse RDB data from an async stream
    pub async fn parse_stream<R: AsyncRead + Unpin>(&mut self, mut reader: R) -> Result<Vec<Entry>> {
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await
            .map_err(|e| RedisReplicatorError::Io(e))?;
        
        self.parse_buffer(&buffer)
    }

    /// Validate RDB stream without full parsing
    pub async fn validate_stream<R: AsyncRead + Unpin>(&self, mut reader: R) -> Result<RdbInfo> {
        // Read header
        let mut header = [0u8; 9];
        reader.read_exact(&mut header).await
            .map_err(|e| RedisReplicatorError::Io(e))?;
        
        if &header[0..5] != b"REDIS" {
            return Err(RedisReplicatorError::parse_error(
                0, "Invalid RDB magic number".to_string()
            ));
        }
        
        let version_str = std::str::from_utf8(&header[5..9])
            .map_err(|_| RedisReplicatorError::parse_error(
                5, "Invalid RDB version format".to_string()
            ))?;
        
        let version = version_str.parse::<u32>()
            .map_err(|_| RedisReplicatorError::parse_error(
                5, format!("Invalid RDB version: {}", version_str)
            ))?;
        
        if version < 1 || version > 11 {
            return Err(RedisReplicatorError::parse_error(
                5, format!("Unsupported RDB version: {}", version)
            ));
        }
        
        // Read remaining data to get file size
        let mut remaining = Vec::new();
        reader.read_to_end(&mut remaining).await
            .map_err(|e| RedisReplicatorError::Io(e))?;
        
        Ok(RdbInfo {
            version,
            redis_version: None,
            creation_time: None,
            used_memory: None,
            database_count: 0,
            key_count: 0,
            expires_count: 0,
            file_size: (header.len() + remaining.len()) as u64,
            checksum: None,
        })
    }

    /// Parse with a custom entry handler
    fn parse_with_reader<F>(&mut self, reader: &mut BufferReader, mut handler: F) -> Result<()>
    where
        F: FnMut(Entry) -> Result<()>,
    {
        self.start_time = current_timestamp_ms();
        self.state = ParserState::Header;
        
        // Parse header
        self.parse_header(reader)?;
        
        // Parse content
        while reader.has_remaining() && self.state != ParserState::Complete {
            match self.state {
                ParserState::Database => {
                    self.parse_database_section(reader, &mut handler)?;
                },
                ParserState::KeyValue => {
                    self.parse_key_value(reader, &mut handler)?;
                },
                ParserState::Checksum => {
                    self.parse_checksum(reader)?;
                    self.state = ParserState::Complete;
                },
                _ => break,
            }
        }
        
        self.stats.processing_time_ms = current_timestamp_ms() - self.start_time;
        Ok(())
    }

    /// Parse RDB header
    fn parse_header(&mut self, reader: &mut BufferReader) -> Result<()> {
        // Read magic number "REDIS"
        let magic = reader.read_bytes(5)?;
        if magic != b"REDIS" {
            return Err(RedisReplicatorError::parse_error(
                (reader.position() - 5) as u64,
                "Invalid RDB magic number".to_string(),
            ));
        }
        
        // Read version (4 ASCII digits)
        let position = reader.position();
        let version_bytes = reader.read_bytes(4)?;
        let version_str = std::str::from_utf8(version_bytes)
            .map_err(|_| RedisReplicatorError::parse_error(
                (position - 4) as u64,
                "Invalid RDB version format".to_string(),
            ))?;
        
        let version = version_str.parse::<u32>()
            .map_err(|_| RedisReplicatorError::parse_error(
                (position - 4) as u64,
                format!("Invalid RDB version: {}", version_str),
            ))?;
        
        if version < 1 || version > 11 {
            return Err(RedisReplicatorError::parse_error(
                (reader.position() - 4) as u64,
                format!("Unsupported RDB version: {}", version),
            ));
        }
        
        tracing::debug!("RDB version: {}", version);
        self.state = ParserState::Database;
        Ok(())
    }

    /// Parse database section
    fn parse_database_section<F>(&mut self, reader: &mut BufferReader, handler: &mut F) -> Result<()>
    where
        F: FnMut(Entry) -> Result<()>,
    {
        let opcode = reader.read_u8()?;
        
        match RdbOpcode::try_from(opcode)? {
            RdbOpcode::Eof => {
                self.state = if self.options.verify_checksums {
                    ParserState::Checksum
                } else {
                    ParserState::Complete
                };
            },
            RdbOpcode::SelectDb => {
                let (db_id, _) = self.read_length(reader)?;
                self.current_database = db_id as u32;
            tracing::debug!("Selected database: {}", self.current_database);
                
                if let Some(ref filter) = self.options.database_filter {
                    if !filter.contains(&self.current_database) {
                        // Skip this database
                        return Ok(());
                    }
                }
                
                self.stats.databases_processed += 1;
            },
            RdbOpcode::ResizeDb => {
                let (hash_table_size, _) = self.read_length(reader)?;
                let (expire_hash_table_size, _) = self.read_length(reader)?;
            tracing::debug!("Resize DB: hash_table_size={}, expire_hash_table_size={}",
                   hash_table_size, expire_hash_table_size);
            },
            RdbOpcode::Aux => {
                if !self.options.skip_aux_fields {
                    let key = self.read_string(reader)?;
                    let value = self.read_string(reader)?;
                tracing::debug!("AUX field: {}={}",
                       String::from_utf8_lossy(&key),
                       String::from_utf8_lossy(&value));
                }
            },
            _ => {
                // This is a key-value pair
                reader.seek(reader.position() - 1)?; // Go back one byte
                self.state = ParserState::KeyValue;
                return self.parse_key_value(reader, handler);
            }
        }
        
        Ok(())
    }

    /// Parse key-value pair
    fn parse_key_value<F>(&mut self, reader: &mut BufferReader, handler: &mut F) -> Result<()>
    where
        F: FnMut(Entry) -> Result<()>,
    {
        let mut expiry: Option<u64> = None;
        let start_pos = reader.position();
        
        // Check for expiry information
        let opcode = reader.read_u8()?;
        let value_type = match RdbOpcode::try_from(opcode)? {
            RdbOpcode::Expire => {
                let expire_time = self.read_u32_le(reader)? as u64 * 1000; // Convert to milliseconds
                expiry = Some(expire_time);
                let type_byte = reader.read_u8()?;
                RdbType::try_from(type_byte)?
            },
            RdbOpcode::ExpireMs => {
                expiry = Some(self.read_u64_le(reader)?);
                let type_byte = reader.read_u8()?;
                RdbType::try_from(type_byte)?
            },
            _ => {
                // This is a value type, convert to RdbType
                RdbType::try_from(opcode)?
            },
        };
        
        // Check if key is expired
        if let Some(expire_time) = expiry {
            if self.options.skip_expired && expire_time < current_timestamp_ms() {
                // Skip expired key, but we still need to read it to advance the parser
                self.skip_key_value(reader, value_type)?;
                self.stats.expired_keys_skipped += 1;
                self.state = ParserState::Database;
                return Ok(());
            }
        }
        
        // Read key
        let key = self.read_string(reader)?;
        
        if key.len() > self.options.max_key_size {
                tracing::warn!("Key too large: {} bytes, skipping", key.len());
            self.skip_value(reader, value_type)?;
            self.state = ParserState::Database;
            return Ok(());
        }
        
        // Read value based on type
        let value = self.read_value(reader, value_type)?;
        
        // Create entry
        let entry = RdbEntry::KeyValue {
            key: String::from_utf8_lossy(&key).to_string(),
            value: value.clone(),
            expire_ms: expiry,
            idle: None,
            freq: None,
        };
        
        // Apply filters
        if let Some(ref pattern) = self.options.key_pattern {
            let key_str = String::from_utf8_lossy(&key);
            // Simple pattern matching (could be enhanced with regex)
            if !key_str.contains(pattern) {
                self.state = ParserState::Database;
                return Ok(());
            }
        }
        
        // Update statistics
        self.stats.keys_processed += 1;
        self.stats.bytes_processed += (reader.position() - start_pos) as u64;
        
        // Call handler
        handler(Entry::Rdb(entry))?;
        
        self.state = ParserState::Database;
        Ok(())
    }

    /// Skip a key-value pair without parsing the value
    fn skip_key_value(&mut self, reader: &mut BufferReader, value_type: RdbType) -> Result<()> {
        // Skip key
        let (key_len, _) = self.read_length(reader)?;
        reader.skip(key_len as usize)?;
        
        // Skip value
        self.skip_value(reader, value_type)?;
        
        Ok(())
    }

    /// Skip a value based on its type
    fn skip_value(&mut self, reader: &mut BufferReader, value_type: RdbType) -> Result<()> {
        match value_type {
            RdbType::String => {
                let (len, _) = self.read_length(reader)?;
                reader.skip(len as usize)?;
            },
            RdbType::List => {
                let (count, _) = self.read_length(reader)?;
                for _ in 0..count {
                    let (len, _) = self.read_length(reader)?;
                    reader.skip(len as usize)?;
                }
            },
            RdbType::Set => {
                let (count, _) = self.read_length(reader)?;
                for _ in 0..count {
                    let (len, _) = self.read_length(reader)?;
                    reader.skip(len as usize)?;
                }
            },
            RdbType::ZSet => {
                let (count, _) = self.read_length(reader)?;
                for _ in 0..count {
                    let (len, _) = self.read_length(reader)?;
                    reader.skip(len as usize)?; // member
                    reader.skip(8)?; // score (double)
                }
            },
            RdbType::Hash => {
                let (count, _) = self.read_length(reader)?;
                for _ in 0..count {
                    let (key_len, _) = self.read_length(reader)?;
                    reader.skip(key_len as usize)?;
                    let (val_len, _) = self.read_length(reader)?;
                    reader.skip(val_len as usize)?;
                }
            },
            _ => {
                return Err(RedisReplicatorError::parse_error(
                    (reader.position()) as u64,
                    format!("Cannot skip unknown value type: {:?}", value_type),
                ));
            }
        }
        Ok(())
    }

    /// Read a Redis value based on its type
    fn read_value(&mut self, reader: &mut BufferReader, value_type: RdbType) -> Result<RedisValue> {
        match value_type {
            RdbType::String => {
                let data = self.read_string(reader)?;
                let string_data = String::from_utf8_lossy(&data).to_string();
                Ok(RedisValue::String(string_data))
            },
            RdbType::List => {
                let (count, _) = self.read_length(reader)?;
                let mut list = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let item = self.read_string(reader)?;
                    let string_item = String::from_utf8_lossy(&item).to_string();
                    list.push(string_item);
                }
                Ok(RedisValue::List(list))
            },
            RdbType::Set => {
                let (count, _) = self.read_length(reader)?;
                let mut set = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let member = self.read_string(reader)?;
                    let string_member = String::from_utf8_lossy(&member).to_string();
                    set.push(string_member);
                }
                Ok(RedisValue::Set(set))
            },
            RdbType::ZSet => {
                let (count, _) = self.read_length(reader)?;
                let mut zset = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let member = self.read_string(reader)?;
                    let string_member = String::from_utf8_lossy(&member).to_string();
                    let score = self.read_double(reader)?;
                    zset.push(crate::types::ZSetEntry { member: string_member, score });
                }
                Ok(RedisValue::ZSet(zset))
            },
            RdbType::Hash => {
                let (count, _) = self.read_length(reader)?;
                let mut hash = HashMap::with_capacity(count as usize);
                for _ in 0..count {
                    let key = self.read_string(reader)?;
                    let string_key = String::from_utf8_lossy(&key).to_string();
                    let value = self.read_string(reader)?;
                    let string_value = String::from_utf8_lossy(&value).to_string();
                    hash.insert(string_key, string_value);
                }
                Ok(RedisValue::Hash(hash))
            },
            _ => {
                Err(RedisReplicatorError::parse_error(
                    (reader.position()) as u64,
                    format!("Unsupported value type: {:?}", value_type),
                ))
            }
        }
    }

    /// Parse checksum
    fn parse_checksum(&mut self, reader: &mut BufferReader) -> Result<()> {
        if reader.remaining() >= 8 {
            let checksum = self.read_u64_le(reader)?;
            if self.options.verify_checksums {
                if checksum != self.crc {
                    return Err(RedisReplicatorError::parse_error(
                        (reader.position() - 8) as u64,
                        format!("Checksum mismatch: expected {}, got {}", self.crc, checksum),
                    ));
                }
            }
        tracing::debug!("Checksum verified: {}", checksum);
        }
        Ok(())
    }

    /// Read length encoding
    fn read_length(&mut self, reader: &mut BufferReader) -> Result<(u64, bool)> {
        crate::utils::io::read_length(&mut std::io::Cursor::new(reader.read_bytes(6)?))
            .map_err(|e| RedisReplicatorError::parse_error((reader.position()) as u64, e.to_string()))
    }

    /// Read string with length encoding
    fn read_string(&mut self, reader: &mut BufferReader) -> Result<Vec<u8>> {
        let (length, is_encoded) = self.read_length(reader)?;
        
        if is_encoded {
            // Handle special encodings
            match length {
                0 => {
                    let val = reader.read_u8()? as i8;
                    Ok(val.to_string().into_bytes())
                },
                1 => {
                    let bytes = reader.read_bytes(2)?;
                    let val = i16::from_le_bytes([bytes[0], bytes[1]]);
                    Ok(val.to_string().into_bytes())
                },
                2 => {
                    let bytes = reader.read_bytes(4)?;
                    let val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    Ok(val.to_string().into_bytes())
                },
                3 => {
                    // LZF compressed string
                    let (compressed_len, _) = self.read_length(reader)?;
                    let (uncompressed_len, _) = self.read_length(reader)?;
                    
                    let compressed = reader.read_bytes(compressed_len as usize)?;
                    crate::utils::compression::lzf_decompress(compressed, uncompressed_len as usize)
                },
                _ => Err(RedisReplicatorError::parse_error(
                    (reader.position()) as u64,
                    format!("Unknown string encoding: {}", length),
                ))
            }
        } else {
            if length > self.options.max_value_size as u64 {
                return Err(RedisReplicatorError::parse_error(
                    (reader.position()) as u64,
                    format!("String too large: {} bytes", length),
                ));
            }
            
            let data = reader.read_bytes(length as usize)?;
            Ok(data.to_vec())
        }
    }

    /// Read 32-bit little-endian integer
    fn read_u32_le(&mut self, reader: &mut BufferReader) -> Result<u32> {
        let bytes = reader.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Read 64-bit little-endian integer
    fn read_u64_le(&mut self, reader: &mut BufferReader) -> Result<u64> {
        let bytes = reader.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    /// Read double (8 bytes)
    fn read_double(&mut self, reader: &mut BufferReader) -> Result<f64> {
        let bits = self.read_u64_le(reader)?;
        Ok(f64::from_bits(bits))
    }
}

impl Default for RdbParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdb_parse_options() {
        let options = RdbParseOptions::default();
        assert!(options.skip_expired);
        assert!(!options.skip_aux_fields);
        assert_eq!(options.max_memory, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_parser_creation() {
        let parser = RdbParser::new();
        assert_eq!(parser.state, ParserState::Header);
        assert_eq!(parser.current_database, 0);
    }

    #[test]
    fn test_invalid_magic_number() {
        let mut parser = RdbParser::new();
        let data = b"WRONG1234";
        let result = parser.parse_buffer(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_version() {
        let mut parser = RdbParser::new();
        let data = b"REDISabcd";
        let result = parser.parse_buffer(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_unsupported_version() {
        let mut parser = RdbParser::new();
        let data = b"REDIS9999";
        let result = parser.parse_buffer(data);
        assert!(result.is_err());
    }
}