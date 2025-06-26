//! CRC checksum implementations

use crc::{Crc, CRC_16_IBM_SDLC, CRC_64_REDIS};

/// CRC-16 implementation for Redis
static CRC16: Crc<u16> = Crc::<u16>::new(&CRC_16_IBM_SDLC);

/// CRC-64 implementation for Redis
static CRC64: Crc<u64> = Crc::<u64>::new(&CRC_64_REDIS);

/// Calculate CRC-16 checksum
pub fn crc16(data: &[u8]) -> u16 {
    CRC16.checksum(data)
}

/// Calculate CRC-64 checksum
pub fn crc64(data: &[u8]) -> u64 {
    CRC64.checksum(data)
}

/// CRC-16 digest for incremental calculation
pub struct Crc16Digest {
    digest: crc::Digest<'static, u16>,
}

impl Crc16Digest {
    /// Create a new CRC-16 digest
    pub fn new() -> Self {
        Self {
            digest: CRC16.digest(),
        }
    }

    /// Update the digest with new data
    pub fn update(&mut self, data: &[u8]) {
        self.digest.update(data);
    }

    /// Finalize and get the checksum
    pub fn finalize(self) -> u16 {
        self.digest.finalize()
    }

    /// Reset the digest
    pub fn reset(&mut self) {
        self.digest = CRC16.digest();
    }
}

impl Default for Crc16Digest {
    fn default() -> Self {
        Self::new()
    }
}

/// CRC-64 digest for incremental calculation
pub struct Crc64Digest {
    digest: crc::Digest<'static, u64>,
}

impl Crc64Digest {
    /// Create a new CRC-64 digest
    pub fn new() -> Self {
        Self {
            digest: CRC64.digest(),
        }
    }

    /// Update the digest with new data
    pub fn update(&mut self, data: &[u8]) {
        self.digest.update(data);
    }

    /// Finalize and get the checksum
    pub fn finalize(self) -> u64 {
        self.digest.finalize()
    }

    /// Reset the digest
    pub fn reset(&mut self) {
        self.digest = CRC64.digest();
    }
}

impl Default for Crc64Digest {
    fn default() -> Self {
        Self::new()
    }
}

/// Verify CRC-16 checksum
pub fn verify_crc16(data: &[u8], expected: u16) -> bool {
    crc16(data) == expected
}

/// Verify CRC-64 checksum
pub fn verify_crc64(data: &[u8], expected: u64) -> bool {
    crc64(data) == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16() {
        let data = b"Hello, World!";
        let checksum = crc16(data);
        assert!(checksum != 0);
        assert!(verify_crc16(data, checksum));
    }

    #[test]
    fn test_crc64() {
        let data = b"Hello, World!";
        let checksum = crc64(data);
        assert!(checksum != 0);
        assert!(verify_crc64(data, checksum));
    }

    #[test]
    fn test_crc16_digest() {
        let data = b"Hello, World!";
        let expected = crc16(data);
        
        let mut digest = Crc16Digest::new();
        digest.update(&data[..5]);
        digest.update(&data[5..]);
        let result = digest.finalize();
        
        assert_eq!(result, expected);
    }

    #[test]
    fn test_crc64_digest() {
        let data = b"Hello, World!";
        let expected = crc64(data);
        
        let mut digest = Crc64Digest::new();
        digest.update(&data[..5]);
        digest.update(&data[5..]);
        let result = digest.finalize();
        
        assert_eq!(result, expected);
    }

    #[test]
    fn test_empty_data() {
        assert_eq!(crc16(b""), 0);
        assert_eq!(crc64(b""), 0);
    }

    #[test]
    fn test_incremental_vs_single() {
        let data1 = b"Hello, ";
        let data2 = b"World!";
        let combined = b"Hello, World!";
        
        let single_crc16 = crc16(combined);
        let single_crc64 = crc64(combined);
        
        let mut digest16 = Crc16Digest::new();
        digest16.update(data1);
        digest16.update(data2);
        let incremental_crc16 = digest16.finalize();
        
        let mut digest64 = Crc64Digest::new();
        digest64.update(data1);
        digest64.update(data2);
        let incremental_crc64 = digest64.finalize();
        
        assert_eq!(single_crc16, incremental_crc16);
        assert_eq!(single_crc64, incremental_crc64);
    }
}