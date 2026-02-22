use crate::error::{Error, Result};
use crate::forward_index::{DictionaryReader, FixedBitWidthReader};
use crate::index_map::IndexMap;
use crate::metadata::{DataType, SegmentMetadata};
use std::path::{Path, PathBuf};

pub struct SegmentReader {
    segment_dir: PathBuf,
    metadata: SegmentMetadata,
    index_map: IndexMap,
}

impl SegmentReader {
    /// Open a Pinot segment directory
    pub fn open<P: AsRef<Path>>(segment_dir: P) -> Result<Self> {
        let segment_dir = segment_dir.as_ref().to_path_buf();

        // Read metadata.properties
        let metadata_path = segment_dir.join("metadata.properties");
        let metadata = SegmentMetadata::from_file(&metadata_path)?;

        // Read index_map
        let index_map_path = segment_dir.join("index_map");
        let index_map = IndexMap::from_file(&index_map_path)?;

        Ok(SegmentReader {
            segment_dir,
            metadata,
            index_map,
        })
    }

    pub fn metadata(&self) -> &SegmentMetadata {
        &self.metadata
    }

    pub fn total_docs(&self) -> u32 {
        self.metadata.total_docs
    }

    /// Read a dictionary-encoded INT column
    pub fn read_int_column(&self, column_name: &str) -> Result<Vec<i32>> {
        let col_meta = self.metadata.get_column(column_name)?;

        if col_meta.data_type != DataType::Int {
            return Err(Error::InvalidFormat(format!(
                "Column {} is not INT type",
                column_name
            )));
        }

        if !col_meta.has_dictionary {
            return Err(Error::UnsupportedFeature(
                "RAW INT columns not yet supported".to_string(),
            ));
        }

        // Read dictionary
        let dict_loc = self
            .index_map
            .get_dictionary(column_name)
            .ok_or_else(|| Error::InvalidFormat(format!("No dictionary for {}", column_name)))?;

        let columns_psf = self.segment_dir.join("columns.psf");
        let dictionary = DictionaryReader::read(
            &columns_psf,
            dict_loc.start_offset,
            dict_loc.size,
            &col_meta.data_type,
            col_meta.cardinality,
            col_meta.length_of_each_entry,
        )?;

        // Read forward index (dictionary IDs)
        let fwd_loc = self.index_map.get_forward_index(column_name).ok_or_else(|| {
            Error::InvalidFormat(format!("No forward index for {}", column_name))
        })?;

        let fixed_bit_reader = FixedBitWidthReader::read(
            &columns_psf,
            fwd_loc.start_offset,
            fwd_loc.size,
            col_meta.bits_per_element,
            col_meta.total_docs,
        )?;

        // Read all dict IDs and lookup values
        let dict_ids = fixed_bit_reader.read_all()?;
        let mut values = Vec::with_capacity(dict_ids.len());

        for dict_id in dict_ids {
            let value = dictionary.get_int(dict_id).ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "Invalid dict_id {} for column {}",
                    dict_id, column_name
                ))
            })?;
            values.push(value);
        }

        Ok(values)
    }

    /// Read a dictionary-encoded LONG column
    pub fn read_long_column(&self, column_name: &str) -> Result<Vec<i64>> {
        let col_meta = self.metadata.get_column(column_name)?;

        if col_meta.data_type != DataType::Long {
            return Err(Error::InvalidFormat(format!(
                "Column {} is not LONG type",
                column_name
            )));
        }

        if !col_meta.has_dictionary {
            return Err(Error::UnsupportedFeature(
                "RAW LONG columns not yet supported".to_string(),
            ));
        }

        let dict_loc = self
            .index_map
            .get_dictionary(column_name)
            .ok_or_else(|| Error::InvalidFormat(format!("No dictionary for {}", column_name)))?;

        let columns_psf = self.segment_dir.join("columns.psf");
        let dictionary = DictionaryReader::read(
            &columns_psf,
            dict_loc.start_offset,
            dict_loc.size,
            &col_meta.data_type,
            col_meta.cardinality,
            col_meta.length_of_each_entry,
        )?;

        let fwd_loc = self.index_map.get_forward_index(column_name).ok_or_else(|| {
            Error::InvalidFormat(format!("No forward index for {}", column_name))
        })?;

        let fixed_bit_reader = FixedBitWidthReader::read(
            &columns_psf,
            fwd_loc.start_offset,
            fwd_loc.size,
            col_meta.bits_per_element,
            col_meta.total_docs,
        )?;

        let dict_ids = fixed_bit_reader.read_all()?;
        let mut values = Vec::with_capacity(dict_ids.len());

        for dict_id in dict_ids {
            let value = dictionary.get_long(dict_id).ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "Invalid dict_id {} for column {}",
                    dict_id, column_name
                ))
            })?;
            values.push(value);
        }

        Ok(values)
    }

    /// Read a STRING column (supports both dictionary-encoded and RAW)
    pub fn read_string_column(&self, column_name: &str) -> Result<Vec<String>> {
        let col_meta = self.metadata.get_column(column_name)?;

        if col_meta.data_type != DataType::String {
            return Err(Error::InvalidFormat(format!(
                "Column {} is not STRING type",
                column_name
            )));
        }

        if col_meta.has_dictionary {
            // Dictionary-encoded STRING
            self.read_dict_encoded_string(column_name, col_meta)
        } else {
            // RAW STRING (variable-byte chunk format)
            self.read_raw_string(column_name, col_meta)
        }
    }

    /// Read dictionary-encoded STRING column
    fn read_dict_encoded_string(
        &self,
        column_name: &str,
        col_meta: &crate::metadata::ColumnMetadata,
    ) -> Result<Vec<String>> {
        let dict_loc = self
            .index_map
            .get_dictionary(column_name)
            .ok_or_else(|| Error::InvalidFormat(format!("No dictionary for {}", column_name)))?;

        let columns_psf = self.segment_dir.join("columns.psf");
        let dictionary = DictionaryReader::read(
            &columns_psf,
            dict_loc.start_offset,
            dict_loc.size,
            &col_meta.data_type,
            col_meta.cardinality,
            col_meta.length_of_each_entry,
        )?;

        let fwd_loc = self.index_map.get_forward_index(column_name).ok_or_else(|| {
            Error::InvalidFormat(format!("No forward index for {}", column_name))
        })?;

        let fixed_bit_reader = FixedBitWidthReader::read(
            &columns_psf,
            fwd_loc.start_offset,
            fwd_loc.size,
            col_meta.bits_per_element,
            col_meta.total_docs,
        )?;

        let dict_ids = fixed_bit_reader.read_all()?;
        let mut values = Vec::with_capacity(dict_ids.len());

        for dict_id in dict_ids {
            let value = dictionary.get_string(dict_id).ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "Invalid dict_id {} for column {}",
                    dict_id, column_name
                ))
            })?;
            values.push(value.to_string());
        }

        Ok(values)
    }

    /// Read RAW (non-dictionary) STRING column
    fn read_raw_string(
        &self,
        column_name: &str,
        col_meta: &crate::metadata::ColumnMetadata,
    ) -> Result<Vec<String>> {
        use crate::forward_index::VarByteChunkReader;

        let fwd_loc = self.index_map.get_forward_index(column_name).ok_or_else(|| {
            Error::InvalidFormat(format!("No forward index for {}", column_name))
        })?;

        let columns_psf = self.segment_dir.join("columns.psf");
        let var_byte_reader = VarByteChunkReader::read(
            &columns_psf,
            fwd_loc.start_offset,
            fwd_loc.size,
            col_meta.total_docs,
        )?;

        var_byte_reader.read_all_strings()
    }

    /// Read a dictionary-encoded FLOAT column
    pub fn read_float_column(&self, column_name: &str) -> Result<Vec<f32>> {
        let col_meta = self.metadata.get_column(column_name)?;

        if col_meta.data_type != DataType::Float {
            return Err(Error::InvalidFormat(format!(
                "Column {} is not FLOAT type",
                column_name
            )));
        }

        if !col_meta.has_dictionary {
            return Err(Error::UnsupportedFeature(
                "RAW FLOAT columns not yet supported".to_string(),
            ));
        }

        let dict_loc = self
            .index_map
            .get_dictionary(column_name)
            .ok_or_else(|| Error::InvalidFormat(format!("No dictionary for {}", column_name)))?;

        let columns_psf = self.segment_dir.join("columns.psf");
        let dictionary = DictionaryReader::read(
            &columns_psf,
            dict_loc.start_offset,
            dict_loc.size,
            &col_meta.data_type,
            col_meta.cardinality,
            col_meta.length_of_each_entry,
        )?;

        let fwd_loc = self.index_map.get_forward_index(column_name).ok_or_else(|| {
            Error::InvalidFormat(format!("No forward index for {}", column_name))
        })?;

        let fixed_bit_reader = FixedBitWidthReader::read(
            &columns_psf,
            fwd_loc.start_offset,
            fwd_loc.size,
            col_meta.bits_per_element,
            col_meta.total_docs,
        )?;

        let dict_ids = fixed_bit_reader.read_all()?;
        let mut values = Vec::with_capacity(dict_ids.len());

        for dict_id in dict_ids {
            let value = dictionary.get_float(dict_id).ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "Invalid dict_id {} for column {}",
                    dict_id, column_name
                ))
            })?;
            values.push(value);
        }

        Ok(values)
    }

    /// Read a dictionary-encoded DOUBLE column
    pub fn read_double_column(&self, column_name: &str) -> Result<Vec<f64>> {
        let col_meta = self.metadata.get_column(column_name)?;

        if col_meta.data_type != DataType::Double {
            return Err(Error::InvalidFormat(format!(
                "Column {} is not DOUBLE type",
                column_name
            )));
        }

        if !col_meta.has_dictionary {
            return Err(Error::UnsupportedFeature(
                "RAW DOUBLE columns not yet supported".to_string(),
            ));
        }

        let dict_loc = self
            .index_map
            .get_dictionary(column_name)
            .ok_or_else(|| Error::InvalidFormat(format!("No dictionary for {}", column_name)))?;

        let columns_psf = self.segment_dir.join("columns.psf");
        let dictionary = DictionaryReader::read(
            &columns_psf,
            dict_loc.start_offset,
            dict_loc.size,
            &col_meta.data_type,
            col_meta.cardinality,
            col_meta.length_of_each_entry,
        )?;

        let fwd_loc = self.index_map.get_forward_index(column_name).ok_or_else(|| {
            Error::InvalidFormat(format!("No forward index for {}", column_name))
        })?;

        let fixed_bit_reader = FixedBitWidthReader::read(
            &columns_psf,
            fwd_loc.start_offset,
            fwd_loc.size,
            col_meta.bits_per_element,
            col_meta.total_docs,
        )?;

        let dict_ids = fixed_bit_reader.read_all()?;
        let mut values = Vec::with_capacity(dict_ids.len());

        for dict_id in dict_ids {
            let value = dictionary.get_double(dict_id).ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "Invalid dict_id {} for column {}",
                    dict_id, column_name
                ))
            })?;
            values.push(value);
        }

        Ok(values)
    }
}
