use crate::error::{Error, Result};
use crate::metadata::DataType;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

const MAGIC_MARKER: u64 = 0xDEADBEEFDEAFBEAD;

#[derive(Debug)]
pub enum DictionaryValue {
    Int(Vec<i32>),
    Long(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    String(Vec<String>),
}

pub struct DictionaryReader {
    values: DictionaryValue,
}

impl DictionaryReader {
    /// Read dictionary from columns.psf file at given offset
    pub fn read(
        file_path: &Path,
        offset: usize,
        _size: usize,
        data_type: &DataType,
        cardinality: u32,
        length_of_each_entry: usize,
    ) -> Result<Self> {
        let mut file = File::open(file_path)?;

        // Seek to the dictionary offset
        file.seek(SeekFrom::Start(offset as u64))?;

        // Read and verify magic marker (8 bytes, big-endian)
        let mut magic_bytes = [0u8; 8];
        file.read_exact(&mut magic_bytes)?;
        let magic = u64::from_be_bytes(magic_bytes);

        if magic != MAGIC_MARKER {
            return Err(Error::InvalidFormat(format!(
                "Invalid magic marker: expected 0x{:X}, got 0x{:X}",
                MAGIC_MARKER, magic
            )));
        }

        // Read dictionary values based on data type
        let values = match data_type {
            DataType::Int => {
                let mut values = Vec::with_capacity(cardinality as usize);
                for _ in 0..cardinality {
                    let mut bytes = [0u8; 4];
                    file.read_exact(&mut bytes)?;
                    values.push(i32::from_be_bytes(bytes));
                }
                DictionaryValue::Int(values)
            }
            DataType::Long => {
                let mut values = Vec::with_capacity(cardinality as usize);
                for _ in 0..cardinality {
                    let mut bytes = [0u8; 8];
                    file.read_exact(&mut bytes)?;
                    values.push(i64::from_be_bytes(bytes));
                }
                DictionaryValue::Long(values)
            }
            DataType::Float => {
                let mut values = Vec::with_capacity(cardinality as usize);
                for _ in 0..cardinality {
                    let mut bytes = [0u8; 4];
                    file.read_exact(&mut bytes)?;
                    values.push(f32::from_be_bytes(bytes));
                }
                DictionaryValue::Float(values)
            }
            DataType::Double => {
                let mut values = Vec::with_capacity(cardinality as usize);
                for _ in 0..cardinality {
                    let mut bytes = [0u8; 8];
                    file.read_exact(&mut bytes)?;
                    values.push(f64::from_be_bytes(bytes));
                }
                DictionaryValue::Double(values)
            }
            DataType::String => {
                let mut values = Vec::with_capacity(cardinality as usize);

                if length_of_each_entry > 0 {
                    // Fixed-length strings (padded with null bytes)
                    for _ in 0..cardinality {
                        let mut str_bytes = vec![0u8; length_of_each_entry];
                        file.read_exact(&mut str_bytes)?;

                        // Trim trailing null bytes (padding)
                        let end = str_bytes.iter().position(|&b| b == 0).unwrap_or(str_bytes.len());
                        let trimmed = &str_bytes[..end];

                        let s = String::from_utf8(trimmed.to_vec()).map_err(|e| {
                            Error::Parse(format!("Invalid UTF-8 in dictionary: {}", e))
                        })?;
                        values.push(s);
                    }
                } else {
                    // Variable-length strings (with 4-byte length prefixes)
                    for _ in 0..cardinality {
                        // Read length (4 bytes, big-endian)
                        let mut len_bytes = [0u8; 4];
                        file.read_exact(&mut len_bytes)?;
                        let len = u32::from_be_bytes(len_bytes) as usize;

                        // Read string bytes
                        let mut str_bytes = vec![0u8; len];
                        file.read_exact(&mut str_bytes)?;
                        let s = String::from_utf8(str_bytes).map_err(|e| {
                            Error::Parse(format!("Invalid UTF-8 in dictionary: {}", e))
                        })?;
                        values.push(s);
                    }
                }

                DictionaryValue::String(values)
            }
            DataType::Bytes => {
                return Err(Error::UnsupportedFeature(
                    "BYTES dictionary not yet supported".to_string(),
                ))
            }
            DataType::Boolean => {
                return Err(Error::UnsupportedFeature(
                    "BOOLEAN dictionary not expected".to_string(),
                ))
            }
        };

        Ok(DictionaryReader { values })
    }

    pub fn get_int(&self, dict_id: u32) -> Option<i32> {
        match &self.values {
            DictionaryValue::Int(values) => values.get(dict_id as usize).copied(),
            _ => None,
        }
    }

    pub fn get_long(&self, dict_id: u32) -> Option<i64> {
        match &self.values {
            DictionaryValue::Long(values) => values.get(dict_id as usize).copied(),
            _ => None,
        }
    }

    pub fn get_float(&self, dict_id: u32) -> Option<f32> {
        match &self.values {
            DictionaryValue::Float(values) => values.get(dict_id as usize).copied(),
            _ => None,
        }
    }

    pub fn get_double(&self, dict_id: u32) -> Option<f64> {
        match &self.values {
            DictionaryValue::Double(values) => values.get(dict_id as usize).copied(),
            _ => None,
        }
    }

    pub fn get_string(&self, dict_id: u32) -> Option<&str> {
        match &self.values {
            DictionaryValue::String(values) => values.get(dict_id as usize).map(|s| s.as_str()),
            _ => None,
        }
    }
}
