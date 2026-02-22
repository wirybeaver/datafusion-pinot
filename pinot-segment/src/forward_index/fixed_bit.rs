use crate::error::{Error, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const MAGIC_MARKER_SIZE: usize = 8;

/// Fixed-bit width forward index reader for dictionary-encoded columns
/// Based on PinotDataBitSet.java algorithm (big-endian byte order)
pub struct FixedBitWidthReader {
    buffer: Vec<u8>,
    bits_per_value: u8,
    num_values: u32,
}

impl FixedBitWidthReader {
    /// Read fixed-bit width forward index
    pub fn read(
        file_path: &Path,
        offset: usize,
        size: usize,
        bits_per_value: u8,
        num_values: u32,
    ) -> Result<Self> {
        let mut file = File::open(file_path)?;

        // Seek to offset
        file.seek(SeekFrom::Start(offset as u64))?;

        // Read all bytes including magic marker
        let mut buffer_with_magic = vec![0u8; size];
        file.read_exact(&mut buffer_with_magic)?;

        // Skip the 8-byte magic marker (0xDEADBEEFDEAFBEAD)
        // The actual bit-packed data starts after the magic marker
        let buffer = if size >= MAGIC_MARKER_SIZE {
            buffer_with_magic[MAGIC_MARKER_SIZE..].to_vec()
        } else {
            return Err(Error::InvalidFormat(
                "Forward index too small to contain magic marker".to_string(),
            ));
        };

        Ok(FixedBitWidthReader {
            buffer,
            bits_per_value,
            num_values,
        })
    }

    /// Read dictionary ID for a given document ID
    /// Based on PinotDataBitSet.java:80-101 (big-endian)
    pub fn get_dict_id(&self, doc_id: u32) -> Result<u32> {
        if doc_id >= self.num_values {
            return Err(Error::InvalidFormat(format!(
                "doc_id {} out of range (num_values={})",
                doc_id, self.num_values
            )));
        }

        let bit_offset = (doc_id as u64) * (self.bits_per_value as u64);
        let byte_offset = (bit_offset / 8) as usize;
        let bit_offset_in_first_byte = (bit_offset % 8) as usize;

        if byte_offset >= self.buffer.len() {
            return Err(Error::InvalidFormat(format!(
                "Buffer overflow: byte_offset={}, buffer_len={}",
                byte_offset,
                self.buffer.len()
            )));
        }

        let byte_mask = 0xFF >> bit_offset_in_first_byte;
        let mut current_value = (self.buffer[byte_offset] & byte_mask) as u32;

        let mut num_bits_left =
            self.bits_per_value as i32 - (8 - bit_offset_in_first_byte as i32);

        if num_bits_left <= 0 {
            // Value is fully contained within the first byte
            Ok(current_value >> (-num_bits_left))
        } else {
            // Value spans multiple bytes
            let mut byte_offset = byte_offset + 1;

            while num_bits_left > 8 {
                if byte_offset >= self.buffer.len() {
                    return Err(Error::InvalidFormat("Buffer overflow in multi-byte read".to_string()));
                }
                current_value = (current_value << 8) | (self.buffer[byte_offset] as u32);
                num_bits_left -= 8;
                byte_offset += 1;
            }

            if byte_offset >= self.buffer.len() {
                return Err(Error::InvalidFormat("Buffer overflow in final byte read".to_string()));
            }

            let final_value = (current_value << num_bits_left)
                | ((self.buffer[byte_offset] as u32) >> (8 - num_bits_left));
            Ok(final_value)
        }
    }

    /// Read all dictionary IDs as a batch
    pub fn read_all(&self) -> Result<Vec<u32>> {
        let mut dict_ids = Vec::with_capacity(self.num_values as usize);
        for doc_id in 0..self.num_values {
            dict_ids.push(self.get_dict_id(doc_id)?);
        }
        Ok(dict_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_packing_1bit() {
        // Test 1-bit values: [0, 1, 0, 1, 1, 0, 0, 1]
        // Packed as: 01011001 = 0x59
        let reader = FixedBitWidthReader {
            buffer: vec![0x59],
            bits_per_value: 1,
            num_values: 8,
        };

        assert_eq!(reader.get_dict_id(0).unwrap(), 0);
        assert_eq!(reader.get_dict_id(1).unwrap(), 1);
        assert_eq!(reader.get_dict_id(2).unwrap(), 0);
        assert_eq!(reader.get_dict_id(3).unwrap(), 1);
        assert_eq!(reader.get_dict_id(4).unwrap(), 1);
        assert_eq!(reader.get_dict_id(5).unwrap(), 0);
        assert_eq!(reader.get_dict_id(6).unwrap(), 0);
        assert_eq!(reader.get_dict_id(7).unwrap(), 1);
    }

    #[test]
    fn test_bit_packing_4bit() {
        // Test 4-bit values: [5, 10, 15, 3]
        // Packed as: 0101 1010 1111 0011 = 0x5A 0xF3
        let reader = FixedBitWidthReader {
            buffer: vec![0x5A, 0xF3],
            bits_per_value: 4,
            num_values: 4,
        };

        assert_eq!(reader.get_dict_id(0).unwrap(), 5);
        assert_eq!(reader.get_dict_id(1).unwrap(), 10);
        assert_eq!(reader.get_dict_id(2).unwrap(), 15);
        assert_eq!(reader.get_dict_id(3).unwrap(), 3);
    }

    #[test]
    fn test_bit_packing_cross_byte() {
        // Test 5-bit values: [10, 20, 5]
        // 10 = 01010, 20 = 10100, 5 = 00101
        // Packed: 01010 10100 00101 = 01010101 00001010 = 0x55 0x0A
        let reader = FixedBitWidthReader {
            buffer: vec![0x55, 0x0A],
            bits_per_value: 5,
            num_values: 3,
        };

        assert_eq!(reader.get_dict_id(0).unwrap(), 10);
        assert_eq!(reader.get_dict_id(1).unwrap(), 20);
        assert_eq!(reader.get_dict_id(2).unwrap(), 5);
    }
}
