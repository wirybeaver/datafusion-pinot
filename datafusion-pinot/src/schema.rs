use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use pinot_segment::{DataType as PinotDataType, SegmentMetadata};
use std::sync::Arc;

use crate::error::Result;

/// Convert Pinot data type to Arrow data type
pub fn pinot_to_arrow_type(pinot_type: &PinotDataType) -> ArrowDataType {
    match pinot_type {
        PinotDataType::Int => ArrowDataType::Int32,
        PinotDataType::Long => ArrowDataType::Int64,
        PinotDataType::Float => ArrowDataType::Float32,
        PinotDataType::Double => ArrowDataType::Float64,
        PinotDataType::String => ArrowDataType::Utf8,
        PinotDataType::Bytes => ArrowDataType::Binary,
        PinotDataType::Boolean => ArrowDataType::Boolean,
    }
}

/// Create Arrow schema from Pinot segment metadata
pub fn create_arrow_schema(metadata: &SegmentMetadata) -> Result<SchemaRef> {
    let fields: Vec<Field> = metadata
        .columns
        .iter()
        .map(|(name, col_meta)| {
            Field::new(
                name.clone(),
                pinot_to_arrow_type(&col_meta.data_type),
                false, // nullable = false (Pinot columns are non-nullable)
            )
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

/// Create projected Arrow schema from column indices
pub fn create_projected_schema(
    schema: &Schema,
    projection: &[usize],
) -> Result<SchemaRef> {
    let fields: Vec<Field> = projection
        .iter()
        .map(|&idx| {
            schema
                .field(idx)
                .clone()
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pinot_to_arrow_type_conversion() {
        assert_eq!(
            pinot_to_arrow_type(&PinotDataType::Int),
            ArrowDataType::Int32
        );
        assert_eq!(
            pinot_to_arrow_type(&PinotDataType::Long),
            ArrowDataType::Int64
        );
        assert_eq!(
            pinot_to_arrow_type(&PinotDataType::Float),
            ArrowDataType::Float32
        );
        assert_eq!(
            pinot_to_arrow_type(&PinotDataType::Double),
            ArrowDataType::Float64
        );
        assert_eq!(
            pinot_to_arrow_type(&PinotDataType::String),
            ArrowDataType::Utf8
        );
        assert_eq!(
            pinot_to_arrow_type(&PinotDataType::Bytes),
            ArrowDataType::Binary
        );
        assert_eq!(
            pinot_to_arrow_type(&PinotDataType::Boolean),
            ArrowDataType::Boolean
        );
    }

    #[test]
    fn test_create_projected_schema() {
        let fields = vec![
            Field::new("col1", ArrowDataType::Int32, false),
            Field::new("col2", ArrowDataType::Utf8, false),
            Field::new("col3", ArrowDataType::Float64, false),
        ];
        let schema = Schema::new(fields);

        let projection = vec![0, 2]; // Select col1 and col3
        let projected = create_projected_schema(&schema, &projection).unwrap();

        assert_eq!(projected.fields().len(), 2);
        assert_eq!(projected.field(0).name(), "col1");
        assert_eq!(projected.field(1).name(), "col3");
    }
}
