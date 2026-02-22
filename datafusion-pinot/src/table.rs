use datafusion::arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use pinot_segment::SegmentReader;
use std::any::Any;
use std::path::Path;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::exec::PinotExec;
use crate::schema::{create_arrow_schema, create_projected_schema};

/// TableProvider for a single Pinot segment
#[derive(Debug)]
pub struct PinotTable {
    segment_reader: Arc<SegmentReader>,
    schema: SchemaRef,
}

impl PinotTable {
    /// Open a Pinot segment and create a table
    pub fn open<P: AsRef<Path>>(segment_path: P) -> Result<Self> {
        let segment_reader = SegmentReader::open(segment_path)
            .map_err(|e| Error::Internal(e.to_string()))?;

        let schema = create_arrow_schema(segment_reader.metadata())?;

        Ok(Self {
            segment_reader: Arc::new(segment_reader),
            schema,
        })
    }

    /// Create from an existing SegmentReader
    pub fn from_reader(segment_reader: SegmentReader) -> Result<Self> {
        let schema = create_arrow_schema(segment_reader.metadata())?;

        Ok(Self {
            segment_reader: Arc::new(segment_reader),
            schema,
        })
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
        let projected_schema = if let Some(proj) = projection {
            create_projected_schema(self.schema.as_ref(), proj)
                .map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            self.schema.clone()
        };

        Ok(Arc::new(PinotExec::new(
            self.segment_reader.clone(),
            projected_schema,
            projection.cloned(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEGMENT_DIR: &str = "/tmp/pinot/quickstart/PinotServerDataDir0/baseballStats_OFFLINE/baseballStats_OFFLINE_0_e40936cc-16f8-490e-a85f-bc61a9abee66/v3";

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
}
