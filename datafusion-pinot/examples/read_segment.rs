use datafusion::datasource::TableProvider;
use datafusion::prelude::*;
use datafusion_pinot::PinotTable;
use std::sync::Arc;

/// Example: Read a single Pinot segment directly
///
/// This demonstrates low-level segment reading without the catalog.
/// Useful when you know the exact segment path.
///
/// Usage:
///   cargo run --example read_segment
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Segment path
    let segment_path = std::env::var("PINOT_SEGMENT_PATH").unwrap_or_else(|_| {
        "/tmp/pinot/quickstart/PinotServerDataDir0/baseballStats_OFFLINE/baseballStats_OFFLINE_0_e40936cc-16f8-490e-a85f-bc61a9abee66/v3".to_string()
    });

    println!("📂 Reading Single Pinot Segment");
    println!("================================\n");
    println!("Segment path: {}\n", segment_path);

    // Open single segment as a table
    let table = PinotTable::open(&segment_path)?;

    // Display schema
    let schema = table.schema();
    println!("📋 Schema ({} columns):", schema.fields().len());
    for field in schema.fields() {
        println!("  - {}: {:?}", field.name(), field.data_type());
    }
    println!();

    // Create DataFusion context and register table
    let ctx = SessionContext::new();
    ctx.register_table("segment", Arc::new(table))?;

    // Query the segment
    println!("🔍 Querying segment...\n");

    // Simple count
    let df = ctx.sql("SELECT COUNT(*) FROM segment").await?;
    let results = df.collect().await?;
    println!("Total rows: {:?}\n", results[0]);

    // Sample data
    let df = ctx
        .sql("SELECT \"playerID\", \"teamID\", \"hits\", \"homeRuns\" FROM segment LIMIT 5")
        .await?;
    let results = df.collect().await?;
    println!("Sample data:");
    for batch in results {
        println!("{:?}\n", batch);
    }

    println!("✅ Segment reading completed!");
    Ok(())
}
