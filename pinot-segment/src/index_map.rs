use crate::error::{Error, Result};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct IndexLocation {
    pub start_offset: usize,
    pub size: usize,
}

#[derive(Debug)]
pub struct IndexMap {
    /// Maps (column_name, index_type) -> IndexLocation
    pub indexes: HashMap<(String, String), IndexLocation>,
}

impl IndexMap {
    /// Parse index_map file
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        Self::parse(&content)
    }

    fn parse(content: &str) -> Result<Self> {
        let mut indexes = HashMap::new();

        for line in content.lines() {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Format: {column}.{index_type}.{property}={value}
            // Example: hits.dictionary.startOffset=1024
            // Column names may contain dots, so we parse right-to-left
            if let Some(eq_idx) = line.find('=') {
                let key_part = line[..eq_idx].trim();
                let value = line[eq_idx + 1..].trim();

                // Split key into parts
                let parts: Vec<&str> = key_part.split('.').collect();

                if parts.len() < 3 {
                    continue; // Skip malformed entries
                }

                // Last part is the property (startOffset or size)
                let property = parts[parts.len() - 1];

                // Second-to-last part is the index type
                let index_type = parts[parts.len() - 2];

                // Everything before is the column name
                let column_name = parts[..parts.len() - 2].join(".");

                let value_num = value
                    .parse::<usize>()
                    .map_err(|e| Error::Parse(format!("Invalid number '{}': {}", value, e)))?;

                let key = (column_name.to_string(), index_type.to_string());

                let location = indexes.entry(key).or_insert(IndexLocation {
                    start_offset: 0,
                    size: 0,
                });

                match property {
                    "startOffset" => location.start_offset = value_num,
                    "size" => location.size = value_num,
                    _ => {} // Ignore unknown properties
                }
            }
        }

        Ok(IndexMap { indexes })
    }

    pub fn get_index(&self, column: &str, index_type: &str) -> Option<&IndexLocation> {
        self.indexes
            .get(&(column.to_string(), index_type.to_string()))
    }

    pub fn get_dictionary(&self, column: &str) -> Option<&IndexLocation> {
        self.get_index(column, "dictionary")
    }

    pub fn get_forward_index(&self, column: &str) -> Option<&IndexLocation> {
        self.get_index(column, "forward_index")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_index_map() {
        let content = r#"
# Index map for segment
hits.dictionary.startOffset=1024
hits.dictionary.size=40
hits.forward_index.startOffset=1064
hits.forward_index.size=48986
playerID.forward_index.startOffset=50050
playerID.forward_index.size=5000000
"#;

        let index_map = IndexMap::parse(content).unwrap();

        // Check dictionary index
        let dict = index_map.get_dictionary("hits").unwrap();
        assert_eq!(dict.start_offset, 1024);
        assert_eq!(dict.size, 40);

        // Check forward index
        let fwd = index_map.get_forward_index("hits").unwrap();
        assert_eq!(fwd.start_offset, 1064);
        assert_eq!(fwd.size, 48986);

        // Check RAW column (no dictionary)
        assert!(index_map.get_dictionary("playerID").is_none());
        let player_fwd = index_map.get_forward_index("playerID").unwrap();
        assert_eq!(player_fwd.start_offset, 50050);
    }

    #[test]
    fn test_column_name_with_dots() {
        let content = r#"
some.column.name.dictionary.startOffset=100
some.column.name.dictionary.size=200
"#;

        let index_map = IndexMap::parse(content).unwrap();
        let dict = index_map.get_dictionary("some.column.name").unwrap();
        assert_eq!(dict.start_offset, 100);
        assert_eq!(dict.size, 200);
    }
}
