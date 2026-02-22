use crate::error::{Error, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const METADATA_ENTRY_SIZE: usize = 8; // 4 bytes docId + 4 bytes offset

/// Variable-byte chunk forward index reader for RAW (non-dictionary) columns
/// Version 4 format (different from v2/v3)
pub struct VarByteChunkReader {
    file_path: PathBuf,
    _base_offset: usize,
    _target_decompressed_chunk_size: i32,
    compression_type: i32,
    metadata_offset: usize,
    metadata_size: usize,
    chunks_offset: usize,
}

impl VarByteChunkReader {
    /// Read variable-byte chunk forward index (V4 format)
    pub fn read(file_path: &Path, offset: usize, _size: usize, _total_docs: u32) -> Result<Self> {
        let mut file = File::open(file_path)?;
        file.seek(SeekFrom::Start(offset as u64))?;

        // V4 Header (16 bytes, big-endian for compatibility):
        // - Version (4 bytes)
        // - target_decompressed_chunk_size (4 bytes)
        // - compression_type (4 bytes)
        // - chunks_start_offset (4 bytes)

        // Skip/verify magic marker if present
        let mut first_bytes = [0u8; 4];
        file.read_exact(&mut first_bytes)?;

        // Check if this starts with DEADBEEF magic
        let has_magic = first_bytes == [0xDE, 0xAD, 0xBE, 0xEF];

        if has_magic {
            // Skip rest of magic marker (4 more bytes)
            let mut rest_magic = [0u8; 4];
            file.read_exact(&mut rest_magic)?;
        } else {
            // No magic marker, rewind
            file.seek(SeekFrom::Start(offset as u64))?;
        }

        // Read header (big-endian)
        let mut header = [0u8; 16];
        file.read_exact(&mut header)?;

        let version = i32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let target_decompressed_chunk_size =
            i32::from_be_bytes([header[4], header[5], header[6], header[7]]);
        let compression_type = i32::from_be_bytes([header[8], header[9], header[10], header[11]]);
        let chunks_start_offset =
            i32::from_be_bytes([header[12], header[13], header[14], header[15]]) as usize;

        if version != 4 {
            return Err(Error::UnsupportedFeature(format!(
                "Expected V4 format, got version {}",
                version
            )));
        }

        // Metadata starts at byte 16 (after header) and goes until chunks_start_offset
        let header_end = if has_magic { offset + 8 + 16 } else { offset + 16 };
        let metadata_offset = header_end;
        let metadata_size = chunks_start_offset - 16;
        let chunks_offset = offset + chunks_start_offset + if has_magic { 8 } else { 0 };

        Ok(VarByteChunkReader {
            file_path: file_path.to_path_buf(),
            _base_offset: offset,
            _target_decompressed_chunk_size: target_decompressed_chunk_size,
            compression_type,
            metadata_offset,
            metadata_size,
            chunks_offset,
        })
    }

    /// Binary search metadata to find chunk index for given doc_id
    fn find_chunk_metadata(&self, doc_id: u32) -> Result<(usize, usize)> {
        let mut file = File::open(&self.file_path)?;

        let num_entries = self.metadata_size / METADATA_ENTRY_SIZE;
        let mut low = 0i64;
        let mut high = (num_entries as i64) - 1;

        while low <= high {
            let mid = ((low + high) / 2) as usize;
            let entry_offset = self.metadata_offset + mid * METADATA_ENTRY_SIZE;

            file.seek(SeekFrom::Start(entry_offset as u64))?;
            let mut entry = [0u8; 8];
            file.read_exact(&mut entry)?;

            let entry_doc_id = u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]) & 0x7FFFFFFF;

            if entry_doc_id < doc_id {
                low = mid as i64 + 1;
            } else if entry_doc_id > doc_id {
                high = mid as i64 - 1;
            } else {
                return Ok((mid * METADATA_ENTRY_SIZE, mid));
            }
        }

        let result_idx = (low - 1).max(0) as usize;
        Ok((result_idx * METADATA_ENTRY_SIZE, result_idx))
    }

    /// Read raw bytes for a document
    pub fn get_bytes(&self, doc_id: u32) -> Result<Vec<u8>> {
        let mut file = File::open(&self.file_path)?;

        // Find the chunk containing this doc_id
        let (metadata_pos, entry_idx) = self.find_chunk_metadata(doc_id)?;

        // Read metadata entry (8 bytes, little-endian)
        file.seek(SeekFrom::Start((self.metadata_offset + metadata_pos) as u64))?;
        let mut entry = [0u8; 8];
        file.read_exact(&mut entry)?;

        let chunk_doc_id_offset = u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]) & 0x7FFFFFFF;
        let chunk_offset = u32::from_le_bytes([entry[4], entry[5], entry[6], entry[7]]) as usize;

        // Check if this is a "huge value" (single value spanning entire chunk)
        let is_regular_chunk = (u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]) & 0x80000000) == 0;

        // Determine chunk limit
        let chunk_limit = if (entry_idx + 1) * METADATA_ENTRY_SIZE < self.metadata_size {
            // Read next entry to get limit
            let mut next_entry = [0u8; 8];
            file.read_exact(&mut next_entry)?;
            u32::from_le_bytes([next_entry[4], next_entry[5], next_entry[6], next_entry[7]]) as usize
        } else {
            // Last chunk goes to end of file
            let file_size = file.metadata()?.len() as usize;
            file_size - self.chunks_offset
        };

        let chunk_size = chunk_limit - chunk_offset;

        // Read chunk data
        file.seek(SeekFrom::Start((self.chunks_offset + chunk_offset) as u64))?;
        let mut chunk_data = vec![0u8; chunk_size];
        file.read_exact(&mut chunk_data)?;

        // Handle compressed chunks
        if self.compression_type != 0 {
            return Err(Error::UnsupportedFeature(
                "Compressed chunks not yet supported".to_string(),
            ));
        }

        // For huge values, the entire chunk is the value
        if !is_regular_chunk {
            return Ok(chunk_data);
        }

        // Regular chunk: parse structure
        // First 4 bytes: number of docs in chunk (little-endian)
        let num_docs_in_chunk =
            u32::from_le_bytes([chunk_data[0], chunk_data[1], chunk_data[2], chunk_data[3]]) as usize;

        // Calculate index within chunk
        let doc_index_in_chunk = (doc_id - chunk_doc_id_offset) as usize;

        if doc_index_in_chunk >= num_docs_in_chunk {
            return Err(Error::InvalidFormat(format!(
                "doc_id {} not in chunk (chunk starts at {}, has {} docs)",
                doc_id, chunk_doc_id_offset, num_docs_in_chunk
            )));
        }

        // Read offset array (little-endian)
        let offset_array_start = 4; // After num_docs
        let value_offset_pos = offset_array_start + (doc_index_in_chunk + 1) * 4;
        let value_offset = u32::from_le_bytes([
            chunk_data[value_offset_pos],
            chunk_data[value_offset_pos + 1],
            chunk_data[value_offset_pos + 2],
            chunk_data[value_offset_pos + 3],
        ]) as usize;

        let next_offset = if doc_index_in_chunk == num_docs_in_chunk - 1 {
            chunk_data.len()
        } else {
            let next_offset_pos = value_offset_pos + 4;
            u32::from_le_bytes([
                chunk_data[next_offset_pos],
                chunk_data[next_offset_pos + 1],
                chunk_data[next_offset_pos + 2],
                chunk_data[next_offset_pos + 3],
            ]) as usize
        };

        let value_bytes = chunk_data[value_offset..next_offset].to_vec();
        Ok(value_bytes)
    }

    /// Read a single value as string
    pub fn get_string(&self, doc_id: u32) -> Result<String> {
        let bytes = self.get_bytes(doc_id)?;
        String::from_utf8(bytes)
            .map_err(|e| Error::Parse(format!("Invalid UTF-8 at doc_id {}: {}", doc_id, e)))
    }

    /// Read all values as strings
    pub fn read_all_strings(&self) -> Result<Vec<String>> {
        // Need to know total_docs - let's read from metadata
        let num_entries = self.metadata_size / METADATA_ENTRY_SIZE;
        if num_entries == 0 {
            return Ok(Vec::new());
        }

        // Read last metadata entry to get max doc_id
        let mut file = File::open(&self.file_path)?;
        let last_entry_offset = self.metadata_offset + (num_entries - 1) * METADATA_ENTRY_SIZE;
        file.seek(SeekFrom::Start(last_entry_offset as u64))?;
        let mut entry = [0u8; 8];
        file.read_exact(&mut entry)?;

        let last_doc_id = u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]) & 0x7FFFFFFF;

        // Need to read the chunk to find how many docs it contains
        let chunk_offset = u32::from_le_bytes([entry[4], entry[5], entry[6], entry[7]]) as usize;

        file.seek(SeekFrom::Start((self.chunks_offset + chunk_offset) as u64))?;
        let mut num_docs_bytes = [0u8; 4];
        file.read_exact(&mut num_docs_bytes)?;
        let num_docs_in_last_chunk = u32::from_le_bytes(num_docs_bytes);

        let total_docs = last_doc_id + num_docs_in_last_chunk;

        let mut values = Vec::with_capacity(total_docs as usize);
        for doc_id in 0..total_docs {
            values.push(self.get_string(doc_id)?);
        }
        Ok(values)
    }

    /// Read all values as raw bytes
    pub fn read_all_bytes(&self) -> Result<Vec<Vec<u8>>> {
        let num_entries = self.metadata_size / METADATA_ENTRY_SIZE;
        if num_entries == 0 {
            return Ok(Vec::new());
        }

        let mut file = File::open(&self.file_path)?;
        let last_entry_offset = self.metadata_offset + (num_entries - 1) * METADATA_ENTRY_SIZE;
        file.seek(SeekFrom::Start(last_entry_offset as u64))?;
        let mut entry = [0u8; 8];
        file.read_exact(&mut entry)?;

        let last_doc_id = u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]) & 0x7FFFFFFF;
        let chunk_offset = u32::from_le_bytes([entry[4], entry[5], entry[6], entry[7]]) as usize;

        file.seek(SeekFrom::Start((self.chunks_offset + chunk_offset) as u64))?;
        let mut num_docs_bytes = [0u8; 4];
        file.read_exact(&mut num_docs_bytes)?;
        let num_docs_in_last_chunk = u32::from_le_bytes(num_docs_bytes);

        let total_docs = last_doc_id + num_docs_in_last_chunk;

        let mut values = Vec::with_capacity(total_docs as usize);
        for doc_id in 0..total_docs {
            values.push(self.get_bytes(doc_id)?);
        }
        Ok(values)
    }
}
