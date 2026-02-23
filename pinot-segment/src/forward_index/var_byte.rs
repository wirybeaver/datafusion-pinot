use crate::error::{Error, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const METADATA_ENTRY_SIZE: usize = 8; // 4 bytes docId + 4 bytes offset

// Compression type constants (from Pinot ChunkCompressionType)
const PASS_THROUGH: i32 = 0;
const SNAPPY: i32 = 1;
const ZSTANDARD: i32 = 2;
const LZ4: i32 = 3;
const LZ4_LENGTH_PREFIXED: i32 = 4;

/// Variable-byte chunk forward index reader for RAW (non-dictionary) columns
/// Version 4 format (different from v2/v3)
pub struct VarByteChunkReader {
    file_path: PathBuf,
    base_offset: usize,
    forward_index_size: usize,
    target_decompressed_chunk_size: i32,
    compression_type: i32,
    metadata_offset: usize,
    metadata_size: usize,
    chunks_offset: usize,
    total_docs: u32,
}

impl VarByteChunkReader {
    /// Read variable-byte chunk forward index (V4 format)
    pub fn read(file_path: &Path, offset: usize, size: usize, total_docs: u32) -> Result<Self> {
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
            base_offset: offset,
            forward_index_size: size,
            target_decompressed_chunk_size,
            compression_type,
            metadata_offset,
            metadata_size,
            chunks_offset,
            total_docs,
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

        // Determine chunk limit and num_docs
        let (chunk_limit, num_docs_in_chunk) = if (entry_idx + 1) * METADATA_ENTRY_SIZE < self.metadata_size {
            // Read next entry to get limit and calculate num_docs
            let mut next_entry = [0u8; 8];
            file.read_exact(&mut next_entry)?;
            let next_doc_id = u32::from_le_bytes([next_entry[0], next_entry[1], next_entry[2], next_entry[3]]) & 0x7FFFFFFF;
            let next_chunk_offset = u32::from_le_bytes([next_entry[4], next_entry[5], next_entry[6], next_entry[7]]) as usize;

            // Check if next_chunk_offset is sentinel value (0xFFFFFFFF means end of chunks)
            if next_chunk_offset == 0xFFFFFFFF {
                // Last chunk - use forward index size to calculate limit
                let limit = self.forward_index_size - (self.chunks_offset - self.base_offset);
                (limit, 0) // num_docs will be read from decompressed chunk
            } else {
                let num_docs = (next_doc_id - chunk_doc_id_offset) as usize;
                (next_chunk_offset, num_docs)
            }
        } else {
            // Last chunk - use forward index size to calculate limit
            let limit = self.forward_index_size - (self.chunks_offset - self.base_offset);
            // For last chunk, we need to determine num_docs from the decompressed data
            // We'll handle this below
            (limit, 0)
        };

        let chunk_size = chunk_limit - chunk_offset;

        // Read chunk data
        file.seek(SeekFrom::Start((self.chunks_offset + chunk_offset) as u64))?;
        let mut chunk_data = vec![0u8; chunk_size];
        file.read_exact(&mut chunk_data)?;

        // Decompress if needed
        let decompressed_chunk = if self.compression_type == PASS_THROUGH {
            chunk_data
        } else {
            self.decompress_chunk(&chunk_data)?
        };

        // For huge values, the entire chunk is the value
        if !is_regular_chunk {
            return Ok(decompressed_chunk);
        }

        // Regular chunk structure for V4:
        // - num_docs: 4 bytes (LE) at position 0
        // - Offset array: (num_docs + 1) * 4 bytes (LE) starting at position 4
        // - String data: variable length

        // If we don't know num_docs (last chunk), read it from the decompressed chunk
        let num_docs_in_chunk = if num_docs_in_chunk == 0 {
            if decompressed_chunk.len() < 8 {
                return Err(Error::InvalidFormat("Decompressed chunk too small".to_string()));
            }
            // Read num_docs from first 4 bytes
            u32::from_le_bytes([
                decompressed_chunk[0],
                decompressed_chunk[1],
                decompressed_chunk[2],
                decompressed_chunk[3],
            ]) as usize
        } else {
            num_docs_in_chunk
        };

        // Calculate index within chunk
        let doc_index_in_chunk = (doc_id - chunk_doc_id_offset) as usize;

        if doc_index_in_chunk >= num_docs_in_chunk {
            return Err(Error::InvalidFormat(format!(
                "doc_id {} not in chunk (chunk starts at {}, has {} docs)",
                doc_id, chunk_doc_id_offset, num_docs_in_chunk
            )));
        }

        // Read offset for this document (offset array starts at position 4, after num_docs field)
        let offset_pos = 4 + doc_index_in_chunk * 4;
        if offset_pos + 4 > decompressed_chunk.len() {
            return Err(Error::InvalidFormat(format!(
                "Offset position {} out of range",
                offset_pos
            )));
        }

        let value_offset = u32::from_le_bytes([
            decompressed_chunk[offset_pos],
            decompressed_chunk[offset_pos + 1],
            decompressed_chunk[offset_pos + 2],
            decompressed_chunk[offset_pos + 3],
        ]) as usize;

        // For the last document in chunk, use chunk size as next offset
        // Otherwise read next offset from array
        let next_offset = if doc_index_in_chunk == num_docs_in_chunk - 1 {
            decompressed_chunk.len()
        } else {
            let next_offset_pos = offset_pos + 4;
            if next_offset_pos + 4 > decompressed_chunk.len() {
                return Err(Error::InvalidFormat(format!(
                    "Next offset position {} out of range",
                    next_offset_pos
                )));
            }
            u32::from_le_bytes([
                decompressed_chunk[next_offset_pos],
                decompressed_chunk[next_offset_pos + 1],
                decompressed_chunk[next_offset_pos + 2],
                decompressed_chunk[next_offset_pos + 3],
            ]) as usize
        };

        if value_offset > decompressed_chunk.len() || next_offset > decompressed_chunk.len() {
            return Err(Error::InvalidFormat(format!(
                "Value offsets out of range: {} to {} (chunk size: {})",
                value_offset, next_offset, decompressed_chunk.len()
            )));
        }

        let value_bytes = decompressed_chunk[value_offset..next_offset].to_vec();
        Ok(value_bytes)
    }

    /// Decompress chunk data based on compression type
    fn decompress_chunk(&self, compressed_data: &[u8]) -> Result<Vec<u8>> {
        match self.compression_type {
            PASS_THROUGH => Ok(compressed_data.to_vec()),
            LZ4 | LZ4_LENGTH_PREFIXED => {
                #[cfg(feature = "lz4")]
                {
                    // For LZ4_LENGTH_PREFIXED, first 4 bytes contain the decompressed size
                    let (decompressed_size, compressed_bytes) = if self.compression_type == LZ4_LENGTH_PREFIXED {
                        if compressed_data.len() < 4 {
                            return Err(Error::InvalidFormat(
                                "LZ4_LENGTH_PREFIXED data too short for length prefix".to_string(),
                            ));
                        }
                        let size = u32::from_le_bytes([
                            compressed_data[0],
                            compressed_data[1],
                            compressed_data[2],
                            compressed_data[3],
                        ]) as usize;
                        (size, &compressed_data[4..])
                    } else {
                        (self.target_decompressed_chunk_size as usize, compressed_data)
                    };

                    // Decompress using lz4 block decompression
                    let decompressed = lz4::block::decompress(compressed_bytes, Some(decompressed_size as i32))
                        .map_err(|e| {
                            Error::InvalidFormat(format!("LZ4 decompression failed: {}", e))
                        })?;

                    Ok(decompressed)
                }
                #[cfg(not(feature = "lz4"))]
                {
                    Err(Error::UnsupportedFeature(
                        "LZ4 compression support not enabled. Enable 'lz4' feature.".to_string(),
                    ))
                }
            }
            SNAPPY => Err(Error::UnsupportedFeature(
                "Snappy compression not yet supported".to_string(),
            )),
            ZSTANDARD => Err(Error::UnsupportedFeature(
                "Zstandard compression not yet supported".to_string(),
            )),
            _ => Err(Error::UnsupportedFeature(format!(
                "Unknown compression type: {}",
                self.compression_type
            ))),
        }
    }

    /// Read a single value as string
    pub fn get_string(&self, doc_id: u32) -> Result<String> {
        let bytes = self.get_bytes(doc_id)?;
        String::from_utf8(bytes)
            .map_err(|e| Error::Parse(format!("Invalid UTF-8 at doc_id {}: {}", doc_id, e)))
    }

    /// Read all values as strings
    pub fn read_all_strings(&self) -> Result<Vec<String>> {
        // Use optimized chunk-by-chunk reading instead of doc-by-doc
        self.read_all_strings_chunked()
    }

    /// Optimized: Read all strings by processing chunks sequentially
    /// instead of calling get_string() for each doc (which re-decompresses chunks)
    fn read_all_strings_chunked(&self) -> Result<Vec<String>> {
        let mut values = Vec::with_capacity(self.total_docs as usize);

        // Read metadata to find all chunks
        let num_entries = self.metadata_size / METADATA_ENTRY_SIZE;
        let mut file = File::open(&self.file_path)?;

        // Process each chunk
        for entry_idx in 0..num_entries {
            // Read metadata entry
            file.seek(SeekFrom::Start((self.metadata_offset + entry_idx * METADATA_ENTRY_SIZE) as u64))?;
            let mut entry = [0u8; 8];
            file.read_exact(&mut entry)?;

            let _chunk_doc_id_offset = u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]) & 0x7FFFFFFF;
            let chunk_offset = u32::from_le_bytes([entry[4], entry[5], entry[6], entry[7]]) as usize;

            // Check if this is a "huge value"
            let is_regular_chunk = (u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]) & 0x80000000) == 0;

            // Determine chunk limit
            let chunk_limit = if (entry_idx + 1) * METADATA_ENTRY_SIZE < self.metadata_size {
                let mut next_entry = [0u8; 8];
                file.read_exact(&mut next_entry)?;
                let next_chunk_offset = u32::from_le_bytes([next_entry[4], next_entry[5], next_entry[6], next_entry[7]]) as usize;
                if next_chunk_offset == 0xFFFFFFFF {
                    self.forward_index_size - (self.chunks_offset - self.base_offset)
                } else {
                    next_chunk_offset
                }
            } else {
                self.forward_index_size - (self.chunks_offset - self.base_offset)
            };

            let chunk_size = chunk_limit - chunk_offset;

            // Read and decompress chunk ONCE
            file.seek(SeekFrom::Start((self.chunks_offset + chunk_offset) as u64))?;
            let mut chunk_data = vec![0u8; chunk_size];
            file.read_exact(&mut chunk_data)?;

            let decompressed_chunk = if self.compression_type == PASS_THROUGH {
                chunk_data
            } else {
                self.decompress_chunk(&chunk_data)?
            };

            // Handle huge values (single value per chunk)
            if !is_regular_chunk {
                values.push(String::from_utf8_lossy(&decompressed_chunk).to_string());
                continue;
            }

            // Extract all values from this chunk
            if decompressed_chunk.len() < 8 {
                return Err(Error::InvalidFormat("Decompressed chunk too small".to_string()));
            }

            let num_docs_in_chunk = u32::from_le_bytes([
                decompressed_chunk[0],
                decompressed_chunk[1],
                decompressed_chunk[2],
                decompressed_chunk[3],
            ]) as usize;

            // Extract all strings from this chunk
            for doc_idx in 0..num_docs_in_chunk {
                let offset_pos = 4 + doc_idx * 4;
                let value_offset = u32::from_le_bytes([
                    decompressed_chunk[offset_pos],
                    decompressed_chunk[offset_pos + 1],
                    decompressed_chunk[offset_pos + 2],
                    decompressed_chunk[offset_pos + 3],
                ]) as usize;

                // For last document in chunk, use chunk size as next offset
                let next_offset = if doc_idx == num_docs_in_chunk - 1 {
                    decompressed_chunk.len()
                } else {
                    let next_offset_pos = offset_pos + 4;
                    u32::from_le_bytes([
                        decompressed_chunk[next_offset_pos],
                        decompressed_chunk[next_offset_pos + 1],
                        decompressed_chunk[next_offset_pos + 2],
                        decompressed_chunk[next_offset_pos + 3],
                    ]) as usize
                };

                if value_offset > decompressed_chunk.len() || next_offset > decompressed_chunk.len() {
                    return Err(Error::InvalidFormat(format!(
                        "Value offsets out of range: {} to {} (chunk size: {})",
                        value_offset, next_offset, decompressed_chunk.len()
                    )));
                }

                let value_bytes = &decompressed_chunk[value_offset..next_offset];
                values.push(String::from_utf8_lossy(value_bytes).to_string());
            }
        }

        Ok(values)
    }

    /// Read all values as raw bytes
    pub fn read_all_bytes(&self) -> Result<Vec<Vec<u8>>> {
        let mut values = Vec::with_capacity(self.total_docs as usize);
        for doc_id in 0..self.total_docs {
            values.push(self.get_bytes(doc_id)?);
        }
        Ok(values)
    }
}
