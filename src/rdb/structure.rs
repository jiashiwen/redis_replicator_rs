//! RDB structure parsing for complex Redis data types

use crate::error::{RedisReplicatorError, Result};
use crate::types::{
    StreamValue, StreamEntry, StreamId, StreamGroup, StreamConsumer,
    StreamPendingEntry, ModuleValue, ZSetEntry
};
use crate::utils::io::BufferReader;
use std::collections::{HashMap, HashSet};

/// Parse Redis Stream data structure
pub fn parse_stream(reader: &mut BufferReader) -> Result<StreamValue> {
    let (length, _) = read_length(reader)?;
        tracing::debug!("Parsing stream with {} entries", length);
    
    let mut entries = Vec::new();
    let mut last_id = StreamId { ms: 0, seq: 0 };
    
    for _ in 0..length {
        let entry = parse_stream_entry(reader, &mut last_id)?;
        entries.push(entry);
    }
    
    // Parse consumer groups (if present)
    let groups = if reader.has_remaining() {
        parse_stream_groups(reader)?
    } else {
        Vec::new()
    };
    
    Ok(StreamValue {
        entries,
        groups,
        length: length,
        last_id: last_id,
        first_id: StreamId { ms: 0, seq: 0 },
        max_deleted_id: StreamId { ms: 0, seq: 0 },
        entries_added: length,
    })
}

/// Parse a single stream entry
fn parse_stream_entry(reader: &mut BufferReader, last_id: &mut StreamId) -> Result<StreamEntry> {
    // Read master ID (delta encoded)
    let ms_delta = read_stream_id_part(reader)?;
    let seq_delta = read_stream_id_part(reader)?;
    
    last_id.ms += ms_delta;
    last_id.seq = if ms_delta > 0 { seq_delta } else { last_id.seq + seq_delta };
    
    let id = last_id.clone();
    
    // Read field count
    let (field_count, _) = read_length(reader)?;
    
    // Read fields
    let mut fields = HashMap::new();
    for _ in 0..field_count {
        let field_name = read_string(reader)?;
        let field_value = read_string(reader)?;
        let string_name = String::from_utf8_lossy(&field_name).to_string();
        let string_value = String::from_utf8_lossy(&field_value).to_string();
        fields.insert(string_name, string_value);
    }
    
    Ok(StreamEntry { id, fields })
}

/// Parse stream consumer groups
fn parse_stream_groups(reader: &mut BufferReader) -> Result<Vec<StreamGroup>> {
    let (group_count, _) = read_length(reader)?;
    let mut groups = Vec::new();
    
    for _ in 0..group_count {
        let group = parse_stream_group(reader)?;
        groups.push(group);
    }
    
    Ok(groups)
}

/// Parse a single stream consumer group
fn parse_stream_group(reader: &mut BufferReader) -> Result<StreamGroup> {
    let name = read_string(reader)?;
    
    // Read last delivered ID
    let last_id_ms = read_u64_le(reader)?;
    let last_id_seq = read_u64_le(reader)?;
    let last_delivered_id = StreamId {
        ms: last_id_ms,
        seq: last_id_seq,
    };
    
    // Read pending entries
    let (pending_count, _) = read_length(reader)?;
    let mut pending_entries = Vec::new();
    
    for _ in 0..pending_count {
        let entry = parse_pending_entry(reader)?;
        pending_entries.push(entry);
    }
    
    // Read consumers
    let (consumer_count, _) = read_length(reader)?;
    let mut consumers = Vec::new();
    
    for _ in 0..consumer_count {
        let consumer = parse_stream_consumer(reader)?;
        consumers.push(consumer);
    }
    
    Ok(StreamGroup {
        name: String::from_utf8_lossy(&name).to_string(),
        last_delivered_id,
        entries_read: 0,
        lag: 0,
        consumers,
        pending: pending_entries,
    })
}

/// Parse a pending stream entry
fn parse_pending_entry(reader: &mut BufferReader) -> Result<StreamPendingEntry> {
    let id_ms = read_u64_le(reader)?;
    let id_seq = read_u64_le(reader)?;
    let id = StreamId { ms: id_ms, seq: id_seq };
    
    let delivery_time = read_u64_le(reader)?;
    let delivery_count = read_u64_le(reader)?;
    let consumer_name = read_string(reader)?;
    
    Ok(StreamPendingEntry {
        id,
        consumer: String::from_utf8_lossy(&consumer_name).to_string(),
        delivery_time,
        delivery_count,
    })
}

/// Parse a stream consumer
fn parse_stream_consumer(reader: &mut BufferReader) -> Result<StreamConsumer> {
    let name = read_string(reader)?;
    let seen_time = read_u64_le(reader)?;
    
    // Read pending entry IDs for this consumer
    let (pending_count, _) = read_length(reader)?;
    let mut pending_ids = Vec::new();
    
    for _ in 0..pending_count {
        let id_ms = read_u64_le(reader)?;
        let id_seq = read_u64_le(reader)?;
        pending_ids.push(StreamId { ms: id_ms, seq: id_seq });
    }
    
    Ok(StreamConsumer {
        name: String::from_utf8_lossy(&name).to_string(),
        seen_time,
        active_time: 0,
        pending_count: pending_ids.len() as u64,
    })
}

/// Parse Redis Module data
pub fn parse_module(reader: &mut BufferReader, module_id: u64) -> Result<ModuleValue> {
        tracing::debug!("Parsing module data with ID: {}", module_id);
    
    // Read module version
    let version = read_u64_le(reader)?;
    
    // Read module data length
    let (data_length, _) = read_length(reader)?;
    
    // Read raw module data
    let data = reader.read_bytes(data_length as usize)?.to_vec();
    
    Ok(ModuleValue {
        module_id,
        module_name: String::new(), // TODO: Extract module name from module_id
        version: version as u32,
        data: data.into(),
    })
}

/// Parse Redis Hash with ziplist encoding
pub fn parse_hash_ziplist(reader: &mut BufferReader) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
    let ziplist_data = read_string(reader)?;
    parse_ziplist_as_hash(&ziplist_data)
}

/// Parse Redis List with ziplist encoding
pub fn parse_list_ziplist(reader: &mut BufferReader) -> Result<Vec<Vec<u8>>> {
    let ziplist_data = read_string(reader)?;
    parse_ziplist_as_list(&ziplist_data)
}

/// Parse Redis Set with intset encoding
pub fn parse_set_intset(reader: &mut BufferReader) -> Result<HashSet<Vec<u8>>> {
    let intset_data = read_string(reader)?;
    parse_intset(&intset_data)
}

/// Parse Redis ZSet with ziplist encoding
pub fn parse_zset_ziplist(reader: &mut BufferReader) -> Result<Vec<ZSetEntry>> {
    let ziplist_data = read_string(reader)?;
    parse_ziplist_as_zset(&ziplist_data)
}

/// Parse ziplist as hash
fn parse_ziplist_as_hash(data: &[u8]) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
    let mut reader = BufferReader::new(data.to_vec());
    let mut hash = HashMap::new();
    
    // Skip ziplist header
    reader.skip(10)?; // zlbytes(4) + zltail(4) + zllen(2)
    
    while reader.has_remaining() {
        // Check for end marker
        if reader.remaining() == 1 && reader.read_bytes(1)?[0] == 0xFF {
            break;
        }
        
        // Read key
        let key = parse_ziplist_entry(&mut reader)?;
        if key.is_empty() {
            break;
        }
        
        // Read value
        let value = parse_ziplist_entry(&mut reader)?;
        hash.insert(key, value);
    }
    
    Ok(hash)
}

/// Parse ziplist as list
fn parse_ziplist_as_list(data: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut reader = BufferReader::new(data.to_vec());
    let mut list = Vec::new();
    
    // Skip ziplist header
    reader.skip(10)?;
    
    while reader.has_remaining() {
        // Check for end marker
        if reader.remaining() == 1 && reader.read_bytes(1)?[0] == 0xFF {
            break;
        }
        
        let entry = parse_ziplist_entry(&mut reader)?;
        if entry.is_empty() {
            break;
        }
        list.push(entry);
    }
    
    Ok(list)
}

/// Parse ziplist as sorted set
fn parse_ziplist_as_zset(data: &[u8]) -> Result<Vec<ZSetEntry>> {
    let mut reader = BufferReader::new(data.to_vec());
    let mut zset = Vec::new();
    
    // Skip ziplist header
    reader.skip(10)?;
    
    while reader.has_remaining() {
        // Check for end marker
        if reader.remaining() == 1 && reader.read_bytes(1)?[0] == 0xFF {
            break;
        }
        
        // Read member
        let member = parse_ziplist_entry(&mut reader)?;
        if member.is_empty() {
            break;
        }
        
        // Read score
        let score_bytes = parse_ziplist_entry(&mut reader)?;
        let score_str = String::from_utf8_lossy(&score_bytes);
        let score = score_str.parse::<f64>()
            .map_err(|e| RedisReplicatorError::parse_error(
                reader.position() as u64,
                format!("Invalid score in ziplist zset: {}", e),
            ))?;
        
        zset.push(ZSetEntry { member: String::from_utf8_lossy(&member).to_string(), score });
    }
    
    Ok(zset)
}

/// Parse a single ziplist entry
fn parse_ziplist_entry(reader: &mut BufferReader) -> Result<Vec<u8>> {
    if !reader.has_remaining() {
        return Ok(Vec::new());
    }
    
    // Read previous entry length
    let prev_len_byte = reader.read_u8()?;
    if prev_len_byte == 0xFE {
        reader.skip(4)?; // 4-byte previous length
    } else if prev_len_byte >= 0xF0 {
        reader.skip(1)?; // 2-byte previous length
    }
    // else: 1-byte previous length (already read)
    
    // Read encoding and length
    let encoding_byte = reader.read_u8()?;
    
    match encoding_byte >> 6 {
        0 => {
            // String with 6-bit length
            let len = (encoding_byte & 0x3F) as usize;
            Ok(reader.read_bytes(len)?.to_vec())
        },
        1 => {
            // String with 14-bit length
            let next_byte = reader.read_u8()?;
            let len = (((encoding_byte & 0x3F) as usize) << 8) | (next_byte as usize);
            Ok(reader.read_bytes(len)?.to_vec())
        },
        2 => {
            // String with 32-bit length
            let len_bytes = reader.read_bytes(4)?;
            let len = u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
            Ok(reader.read_bytes(len)?.to_vec())
        },
        3 => {
            // Integer encoding
            match encoding_byte & 0x3F {
                0 => {
                    // 16-bit integer
                    let bytes = reader.read_bytes(2)?;
                    let val = i16::from_le_bytes([bytes[0], bytes[1]]);
                    Ok(val.to_string().into_bytes())
                },
                1 => {
                    // 32-bit integer
                    let bytes = reader.read_bytes(4)?;
                    let val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    Ok(val.to_string().into_bytes())
                },
                2 => {
                    // 64-bit integer
                    let bytes = reader.read_bytes(8)?;
                    let val = i64::from_le_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3],
                        bytes[4], bytes[5], bytes[6], bytes[7],
                    ]);
                    Ok(val.to_string().into_bytes())
                },
                3 => {
                    // 24-bit integer
                    let bytes = reader.read_bytes(3)?;
                    let val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]) >> 8;
                    Ok(val.to_string().into_bytes())
                },
                4 => {
                    // 8-bit integer
                    let val = reader.read_u8()? as i8;
                    Ok(val.to_string().into_bytes())
                },
                _ => {
                    Err(RedisReplicatorError::parse_error(
                        reader.position() as u64,
                        format!("Unknown ziplist integer encoding: {}", encoding_byte & 0x3F),
                    ))
                }
            }
        },
        _ => unreachable!()
    }
}

/// Parse intset
fn parse_intset(data: &[u8]) -> Result<HashSet<Vec<u8>>> {
    let mut reader = BufferReader::new(data.to_vec());
    let mut set = HashSet::new();
    
    // Read encoding (4 bytes)
    let encoding_bytes = reader.read_bytes(4)?;
    let encoding = u32::from_le_bytes([encoding_bytes[0], encoding_bytes[1], encoding_bytes[2], encoding_bytes[3]]);
    
    // Read length (4 bytes)
    let length_bytes = reader.read_bytes(4)?;
    let length = u32::from_le_bytes([length_bytes[0], length_bytes[1], length_bytes[2], length_bytes[3]]);
    
    // Read integers based on encoding
    for _ in 0..length {
        let value = match encoding {
            2 => {
                // 16-bit integers
                let bytes = reader.read_bytes(2)?;
                let val = i16::from_le_bytes([bytes[0], bytes[1]]);
                val.to_string().into_bytes()
            },
            4 => {
                // 32-bit integers
                let bytes = reader.read_bytes(4)?;
                let val = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                val.to_string().into_bytes()
            },
            8 => {
                // 64-bit integers
                let bytes = reader.read_bytes(8)?;
                let val = i64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3],
                    bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                val.to_string().into_bytes()
            },
            _ => {
                return Err(RedisReplicatorError::parse_error(
                    reader.position() as u64,
                    format!("Unknown intset encoding: {}", encoding),
                ));
            }
        };
        
        set.insert(value);
    }
    
    Ok(set)
}

/// Read stream ID part (variable length integer)
fn read_stream_id_part(reader: &mut BufferReader) -> Result<u64> {
    let (value, _) = read_length(reader)?;
    Ok(value)
}

/// Helper function to read length encoding
fn read_length(reader: &mut BufferReader) -> Result<(u64, bool)> {
    let first_byte = reader.read_u8()?;
    
    match first_byte >> 6 {
        0 => {
            // 6-bit length
            Ok(((first_byte & 0x3F) as u64, false))
        },
        1 => {
            // 14-bit length
            let second_byte = reader.read_u8()?;
            let length = (((first_byte & 0x3F) as u64) << 8) | (second_byte as u64);
            Ok((length, false))
        },
        2 => {
            // 32-bit length
            let bytes = reader.read_bytes(4)?;
            let length = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64;
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

/// Helper function to read string
fn read_string(reader: &mut BufferReader) -> Result<Vec<u8>> {
    let (length, is_encoded) = read_length(reader)?;
    
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
                let (compressed_len, _) = read_length(reader)?;
                let (uncompressed_len, _) = read_length(reader)?;
                
                let compressed = reader.read_bytes(compressed_len as usize)?;
                crate::utils::compression::lzf_decompress(compressed, uncompressed_len as usize)
            },
            _ => Err(RedisReplicatorError::parse_error(
                reader.position() as u64,
                format!("Unknown string encoding: {}", length),
            ))
        }
    } else {
        let data = reader.read_bytes(length as usize)?;
        Ok(data.to_vec())
    }
}

/// Helper function to read 64-bit little-endian integer
fn read_u64_le(reader: &mut BufferReader) -> Result<u64> {
    let bytes = reader.read_bytes(8)?;
    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ziplist_entry_string() {
        // Simple string entry: prev_len=0, encoding=0x05 (6-bit length=5), data="hello"
        let data = vec![0x00, 0x05, b'h', b'e', b'l', b'l', b'o'];
        let mut reader = BufferReader::new(data);
        
        let result = parse_ziplist_entry(&mut reader).unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn test_parse_ziplist_entry_integer() {
        // Integer entry: prev_len=0, encoding=0xC0 (16-bit int), value=1234
        let data = vec![0x00, 0xC0, 0xD2, 0x04]; // 1234 in little-endian
        let mut reader = BufferReader::new(data);
        
        let result = parse_ziplist_entry(&mut reader).unwrap();
        assert_eq!(result, b"1234");
    }

    #[test]
    fn test_parse_intset() {
        // Intset with encoding=4 (32-bit), length=2, values=[100, 200]
        let data = vec![
            0x04, 0x00, 0x00, 0x00, // encoding = 4
            0x02, 0x00, 0x00, 0x00, // length = 2
            0x64, 0x00, 0x00, 0x00, // 100 in little-endian
            0xC8, 0x00, 0x00, 0x00, // 200 in little-endian
        ];
        
        let result = parse_intset(&data).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(b"100".as_slice()));
        assert!(result.contains(b"200".as_slice()));
    }

    #[test]
    fn test_read_length_encoding() {
        // Test 6-bit length
        let data = vec![0x05]; // length = 5
        let mut reader = BufferReader::new(data);
        let (length, is_encoded) = read_length(&mut reader).unwrap();
        assert_eq!(length, 5);
        assert!(!is_encoded);
        
        // Test 14-bit length
        let data = vec![0x40, 0x64]; // length = 100
        let mut reader = BufferReader::new(data);
        let (length, is_encoded) = read_length(&mut reader).unwrap();
        assert_eq!(length, 100);
        assert!(!is_encoded);
        
        // Test special encoding
        let data = vec![0xC1]; // encoding = 1
        let mut reader = BufferReader::new(data);
        let (encoding, is_encoded) = read_length(&mut reader).unwrap();
        assert_eq!(encoding, 1);
        assert!(is_encoded);
    }
}