use crate::error::{Error, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Int,
    Long,
    Float,
    Double,
    String,
    Bytes,
    Boolean,
}

impl DataType {
    pub fn from_string(s: &str) -> Result<Self> {
        match s {
            "INT" => Ok(DataType::Int),
            "LONG" => Ok(DataType::Long),
            "FLOAT" => Ok(DataType::Float),
            "DOUBLE" => Ok(DataType::Double),
            "STRING" => Ok(DataType::String),
            "BYTES" => Ok(DataType::Bytes),
            "BOOLEAN" => Ok(DataType::Boolean),
            _ => Err(Error::Parse(format!("Unknown data type: {}", s))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: DataType,
    pub cardinality: u32,
    pub total_docs: u32,
    pub bits_per_element: u8,
    pub has_dictionary: bool,
    pub is_sorted: bool,
    pub length_of_each_entry: usize,
}

#[derive(Debug)]
pub struct SegmentMetadata {
    pub segment_name: String,
    pub table_name: String,
    pub total_docs: u32,
    pub columns: HashMap<String, ColumnMetadata>,
}

impl SegmentMetadata {
    /// Parse metadata.properties file
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Self::parse(&content)
    }

    fn parse(content: &str) -> Result<Self> {
        let mut properties: HashMap<String, String> = HashMap::new();

        // Parse Java properties format
        for line in content.lines() {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Find the first '=' separator
            if let Some(idx) = line.find('=') {
                let key = line[..idx].trim().to_string();
                let value = line[idx + 1..].trim().to_string();

                // Handle Unicode escapes and special characters
                let decoded_value = Self::decode_java_string(&value);
                properties.insert(key, decoded_value);
            }
        }

        // Extract segment-level metadata
        let segment_name = properties
            .get("segment.name")
            .ok_or_else(|| Error::Parse("Missing segment.name".to_string()))?
            .clone();

        let table_name = properties
            .get("segment.table.name")
            .ok_or_else(|| Error::Parse("Missing segment.table.name".to_string()))?
            .clone();

        let total_docs = properties
            .get("segment.total.docs")
            .ok_or_else(|| Error::Parse("Missing segment.total.docs".to_string()))?
            .parse::<u32>()
            .map_err(|e| Error::Parse(format!("Invalid total.docs: {}", e)))?;

        // Parse column metadata
        let mut columns = HashMap::new();

        // Get list of columns from various sources
        let mut column_names = Vec::new();

        // Try "columns" property first (older format)
        if let Some(column_list) = properties.get("columns") {
            column_names.extend(column_list.split(',').map(|s| s.trim()));
        }

        // Also check dimension and metric columns (v3 format)
        if let Some(dim_cols) = properties.get("segment.dimension.column.names") {
            column_names.extend(dim_cols.split(',').map(|s| s.trim()));
        }
        if let Some(metric_cols) = properties.get("segment.metric.column.names") {
            column_names.extend(metric_cols.split(',').map(|s| s.trim()));
        }
        if let Some(datetime_cols) = properties.get("segment.datetime.column.names") {
            column_names.extend(datetime_cols.split(',').map(|s| s.trim()));
        }

        // Parse each column
        for column_name in column_names {
            if column_name.is_empty() {
                continue;
            }

            let col_meta = Self::parse_column_metadata(column_name, &properties, total_docs)?;
            columns.insert(column_name.to_string(), col_meta);
        }

        Ok(SegmentMetadata {
            segment_name,
            table_name,
            total_docs,
            columns,
        })
    }

    fn parse_column_metadata(
        name: &str,
        properties: &HashMap<String, String>,
        total_docs: u32,
    ) -> Result<ColumnMetadata> {
        let get_prop = |suffix: &str| {
            properties
                .get(&format!("column.{}.{}", name, suffix))
                .cloned()
        };

        let data_type_str = get_prop("dataType")
            .ok_or_else(|| Error::Parse(format!("Missing dataType for column {}", name)))?;
        let data_type = DataType::from_string(&data_type_str)?;

        let cardinality = get_prop("cardinality")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);

        let bits_per_element = get_prop("bitsPerElement")
            .and_then(|s| s.parse::<u8>().ok())
            .unwrap_or(0);

        let has_dictionary = get_prop("hasDictionary")
            .map(|s| s == "true")
            .unwrap_or(false);

        let is_sorted = get_prop("isSorted")
            .map(|s| s == "true")
            .unwrap_or(false);

        let length_of_each_entry = get_prop("lengthOfEachEntry")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);

        Ok(ColumnMetadata {
            name: name.to_string(),
            data_type,
            cardinality,
            total_docs,
            bits_per_element,
            has_dictionary,
            is_sorted,
            length_of_each_entry,
        })
    }

    fn decode_java_string(s: &str) -> String {
        // Handle basic Unicode escapes like \u0020
        let mut result = String::new();
        let mut chars = s.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '\\' {
                if let Some(&next) = chars.peek() {
                    match next {
                        'u' => {
                            // Unicode escape: \uXXXX
                            chars.next(); // consume 'u'
                            let hex: String = chars.by_ref().take(4).collect();
                            if let Ok(code) = u32::from_str_radix(&hex, 16) {
                                if let Some(unicode_char) = char::from_u32(code) {
                                    result.push(unicode_char);
                                    continue;
                                }
                            }
                            // If parsing fails, just add the original text
                            result.push('\\');
                            result.push('u');
                            result.push_str(&hex);
                        }
                        't' => {
                            chars.next();
                            result.push('\t');
                        }
                        'n' => {
                            chars.next();
                            result.push('\n');
                        }
                        'r' => {
                            chars.next();
                            result.push('\r');
                        }
                        '\\' => {
                            chars.next();
                            result.push('\\');
                        }
                        _ => {
                            result.push(ch);
                        }
                    }
                } else {
                    result.push(ch);
                }
            } else {
                result.push(ch);
            }
        }

        result
    }

    pub fn get_column(&self, name: &str) -> Result<&ColumnMetadata> {
        self.columns
            .get(name)
            .ok_or_else(|| Error::ColumnNotFound(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_properties() {
        let content = r#"
segment.name=test_segment
segment.table.name=testTable
segment.total.docs=100
columns=col1,col2
column.col1.dataType=INT
column.col1.cardinality=10
column.col1.bitsPerElement=4
column.col1.hasDictionary=true
column.col1.isSorted=false
column.col2.dataType=STRING
column.col2.cardinality=50
column.col2.bitsPerElement=6
column.col2.hasDictionary=true
column.col2.isSorted=true
"#;

        let metadata = SegmentMetadata::parse(content).unwrap();

        assert_eq!(metadata.segment_name, "test_segment");
        assert_eq!(metadata.table_name, "testTable");
        assert_eq!(metadata.total_docs, 100);
        assert_eq!(metadata.columns.len(), 2);

        let col1 = metadata.get_column("col1").unwrap();
        assert_eq!(col1.data_type, DataType::Int);
        assert_eq!(col1.cardinality, 10);
        assert_eq!(col1.bits_per_element, 4);
        assert!(col1.has_dictionary);
        assert!(!col1.is_sorted);
    }

    #[test]
    fn test_decode_unicode() {
        assert_eq!(SegmentMetadata::decode_java_string("hello"), "hello");
        assert_eq!(SegmentMetadata::decode_java_string("hello\\u0020world"), "hello world");
        assert_eq!(SegmentMetadata::decode_java_string("tab\\there"), "tab\there");
    }
}
