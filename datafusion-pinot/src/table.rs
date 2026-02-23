use datafusion::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use pinot_segment::SegmentReader;
use std::any::Any;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::exec::PinotExec;
use crate::schema::create_arrow_schema;

/// TableProvider for Pinot table (one or more segments)
#[derive(Debug)]
pub struct PinotTable {
    segments: Vec<Arc<SegmentReader>>,
    schema: SchemaRef,
    _table_name: String,
}

impl PinotTable {
    /// Open a single Pinot segment and create a table
    pub fn open<P: AsRef<Path>>(segment_path: P) -> Result<Self> {
        let segment_reader = SegmentReader::open(segment_path.as_ref())
            .map_err(|e| Error::Internal(e.to_string()))?;

        let schema = create_arrow_schema(segment_reader.metadata())?;
        let table_name = segment_reader.metadata().table_name.clone();

        Ok(Self {
            segments: vec![Arc::new(segment_reader)],
            schema,
            _table_name: table_name,
        })
    }

    /// Open all segments for a Pinot table
    pub fn open_table<P: AsRef<Path>>(table_dir: P) -> Result<Self> {
        let table_dir = table_dir.as_ref();

        // Read all segment directories
        let entries = fs::read_dir(table_dir)
            .map_err(|e| Error::Internal(format!("Failed to read table directory: {}", e)))?;

        let mut segment_paths = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| Error::Internal(e.to_string()))?;
            let path = entry.path();

            // Skip non-directories and temporary directories
            if !path.is_dir() || path.file_name().unwrap().to_str().unwrap() == "tmp" {
                continue;
            }

            // Check if it's a valid segment (has v3 subdirectory)
            let v3_path = path.join("v3");
            if v3_path.exists() && v3_path.is_dir() {
                segment_paths.push(v3_path);
            }
        }

        if segment_paths.is_empty() {
            return Err(Error::Internal(format!(
                "No valid segments found in {}",
                table_dir.display()
            )));
        }

        // Sort for consistent ordering
        segment_paths.sort();

        // Use open_segments to load all segments
        let table_name = table_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        Self::open_segments(&segment_paths, table_name)
    }

    /// Open segments from a list of paths
    ///
    /// This method is used by the catalog to open tables when segment paths
    /// are provided by a MetadataProvider.
    ///
    /// # Arguments
    /// * `segment_paths` - Vector of paths to segment directories (typically v3 directories)
    /// * `table_name` - Name of the table (used for error messages if segments have no metadata)
    pub fn open_segments<P: AsRef<Path>>(segment_paths: &[P], table_name: &str) -> Result<Self> {
        if segment_paths.is_empty() {
            return Err(Error::Internal(format!(
                "No segments provided for table '{}'",
                table_name
            )));
        }

        // Load all segments
        let mut segments = Vec::new();
        let mut schema = None;
        let mut actual_table_name = table_name.to_string();

        for segment_path in segment_paths {
            let segment_reader = SegmentReader::open(segment_path.as_ref()).map_err(|e| {
                Error::Internal(format!(
                    "Failed to open segment {:?}: {}",
                    segment_path.as_ref(),
                    e
                ))
            })?;

            if schema.is_none() {
                schema = Some(create_arrow_schema(segment_reader.metadata())?);
                actual_table_name = segment_reader.metadata().table_name.clone();
            }

            segments.push(Arc::new(segment_reader));
        }

        Ok(Self {
            segments,
            schema: schema.unwrap(),
            _table_name: actual_table_name,
        })
    }

    /// Get the number of segments
    pub fn num_segments(&self) -> usize {
        self.segments.len()
    }

    /// Get total number of documents across all segments
    pub fn total_docs(&self) -> u64 {
        self.segments
            .iter()
            .map(|s| s.metadata().total_docs as u64)
            .sum()
    }
}

#[async_trait]
impl TableProvider for PinotTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PinotExec::new(
            self.segments.clone(),
            self.schema.clone(),
            projection.cloned(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEGMENT_DIR: &str = "/tmp/pinot/quickstart/PinotServerDataDir0/baseballStats_OFFLINE/baseballStats_OFFLINE_0_e40936cc-16f8-490e-a85f-bc61a9abee66/v3";
    const TABLE_DIR: &str = "/tmp/pinot/quickstart/PinotServerDataDir0/baseballStats_OFFLINE";

    #[test]
    fn test_pinot_table_open() {
        if !Path::new(SEGMENT_DIR).exists() {
            println!("Skipping test: segment directory not found");
            return;
        }

        let table = PinotTable::open(SEGMENT_DIR).expect("Failed to open table");

        // Verify schema
        let schema = table.schema();
        assert!(schema.fields().len() > 0, "Schema should have fields");

        // Check some expected columns
        assert!(schema.field_with_name("playerID").is_ok());
        assert!(schema.field_with_name("teamID").is_ok());
        assert!(schema.field_with_name("hits").is_ok());
    }

    #[test]
    fn test_open_table_multi_segment() {
        if !Path::new(TABLE_DIR).exists() {
            println!("Skipping test: table directory not found");
            return;
        }

        let table = PinotTable::open_table(TABLE_DIR).expect("Failed to open table");

        assert!(table.num_segments() >= 1, "Should have at least one segment");
        assert!(table.total_docs() > 0, "Should have documents");

        println!("Loaded {} segments with {} total docs",
                 table.num_segments(), table.total_docs());
    }
}
