use datafusion::arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatchOptions;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::stream::Stream;
use pinot_segment::{DataType as PinotDataType, SegmentReader};
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::{Error, Result};
use crate::schema::create_projected_schema;

const BATCH_SIZE: usize = 8192;

/// Execution plan for reading Pinot segments (supports multi-segment tables)
#[derive(Debug)]
pub struct PinotExec {
    segments: Vec<Arc<SegmentReader>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    plan_properties: PlanProperties,
}

impl PinotExec {
    pub fn new(
        segments: Vec<Arc<SegmentReader>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let num_partitions = segments.len();

        // Calculate projected schema
        let projected_schema = if let Some(ref proj) = projection {
            create_projected_schema(schema.as_ref(), proj).unwrap_or(schema.clone())
        } else {
            schema.clone()
        };

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            segments,
            schema: projected_schema,
            projection,
            plan_properties,
        }
    }

    /// Read all columns once (optimization to avoid re-reading for each batch)
    fn read_columns_once(
        segment_reader: &SegmentReader,
        _schema: &SchemaRef,
        projection: &Option<Vec<usize>>,
    ) -> Result<Vec<ArrayRef>> {
        let column_names: Vec<String> = if let Some(ref proj) = projection {
            proj.iter()
                .map(|&idx| {
                    segment_reader
                        .metadata()
                        .columns
                        .keys()
                        .nth(idx)
                        .unwrap()
                        .clone()
                })
                .collect()
        } else {
            segment_reader
                .metadata()
                .columns
                .keys()
                .cloned()
                .collect()
        };

        // Handle empty projection (e.g., COUNT(*) queries)
        if column_names.is_empty() {
            return Ok(vec![]);
        }

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_names.len());

        for column_name in column_names.iter() {
            let col_meta = segment_reader
                .metadata()
                .get_column(column_name)
                .map_err(|e| Error::Internal(e.to_string()))?;

            let array: ArrayRef = match col_meta.data_type {
                PinotDataType::Int => {
                    let values = segment_reader
                        .read_int_column(column_name)
                        .map_err(|e| Error::Internal(e.to_string()))?;
                    Arc::new(Int32Array::from(values))
                }
                PinotDataType::Long => {
                    let values = segment_reader
                        .read_long_column(column_name)
                        .map_err(|e| Error::Internal(e.to_string()))?;
                    Arc::new(Int64Array::from(values))
                }
                PinotDataType::Float => {
                    let values = segment_reader
                        .read_float_column(column_name)
                        .map_err(|e| Error::Internal(e.to_string()))?;
                    Arc::new(Float32Array::from(values))
                }
                PinotDataType::Double => {
                    let values = segment_reader
                        .read_double_column(column_name)
                        .map_err(|e| Error::Internal(e.to_string()))?;
                    Arc::new(Float64Array::from(values))
                }
                PinotDataType::String => {
                    let values = segment_reader
                        .read_string_column(column_name)
                        .map_err(|e| Error::Internal(e.to_string()))?;
                    Arc::new(StringArray::from(values))
                }
                _ => {
                    return Err(Error::UnsupportedFeature(format!(
                        "Data type {:?} not yet supported",
                        col_meta.data_type
                    )))
                }
            };

            arrays.push(array);
        }

        Ok(arrays)
    }

    /// Create a batch by slicing pre-read column arrays
    fn create_batch_from_arrays(
        column_arrays: &[ArrayRef],
        schema: &SchemaRef,
        offset: usize,
        limit: usize,
    ) -> Result<RecordBatch> {
        // Handle empty projection (e.g., COUNT(*) queries)
        if column_arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(limit));
            return RecordBatch::try_new_with_options(schema.clone(), vec![], &options)
                .map_err(|e| Error::Internal(format!("Failed to create RecordBatch: {}", e)));
        }

        // Slice each column array for this batch
        let sliced_arrays: Vec<ArrayRef> = column_arrays
            .iter()
            .map(|array| array.slice(offset, limit))
            .collect();

        RecordBatch::try_new(schema.clone(), sliced_arrays)
            .map_err(|e| Error::Internal(format!("Failed to create RecordBatch: {}", e)))
    }
}

impl DisplayAs for PinotExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PinotExec: segments={}, partitions={}",
            self.segments.len(),
            self.segments.len()
        )
    }
}

impl ExecutionPlan for PinotExec {
    fn name(&self) -> &str {
        "PinotExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Each partition reads from one segment
        let segment_reader = self
            .segments
            .get(partition)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Partition {} out of range (have {} segments)",
                    partition,
                    self.segments.len()
                ))
            })?
            .clone();

        let total_docs = segment_reader.metadata().total_docs as usize;
        let schema = self.schema.clone();
        let projection = self.projection.clone();

        // OPTIMIZATION: Read all columns ONCE instead of re-reading for each batch
        let column_arrays = Self::read_columns_once(&segment_reader, &schema, &projection)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Create batches by slicing the pre-read column data
        let batches = (0..total_docs)
            .step_by(BATCH_SIZE)
            .map(|offset| {
                let limit = BATCH_SIZE.min(total_docs - offset);
                Self::create_batch_from_arrays(&column_arrays, &schema, offset, limit)
            })
            .collect::<Result<Vec<_>>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Box::pin(PinotStream {
            schema,
            batches,
            index: 0,
        }))
    }
}

/// Stream of RecordBatches from Pinot segment
struct PinotStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    index: usize,
}

impl Stream for PinotStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index < self.batches.len() {
            let batch = self.batches[self.index].clone();
            self.index += 1;
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for PinotStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
