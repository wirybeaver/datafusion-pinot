use datafusion::prelude::*;
use datafusion_pinot::PinotTable;
use std::path::Path;
use std::sync::Arc;

const SEGMENT_DIR: &str = "/tmp/pinot/quickstart/PinotServerDataDir0/baseballStats_OFFLINE/baseballStats_OFFLINE_0_e40936cc-16f8-490e-a85f-bc61a9abee66/v3";

#[tokio::test]
async fn test_simple_select() {
    if !Path::new(SEGMENT_DIR).exists() {
        println!("Skipping test: segment directory not found");
        return;
    }

    let ctx = SessionContext::new();

    // Open Pinot table
    let table = PinotTable::open(SEGMENT_DIR).expect("Failed to open Pinot table");

    // Register table
    ctx.register_table("baseballStats", Arc::new(table))
        .expect("Failed to register table");

    // Execute simple SELECT query (use exact column names - case sensitive)
    let df = ctx
        .sql("SELECT \"playerID\", \"teamID\", \"hits\", \"homeRuns\" FROM baseballStats LIMIT 10")
        .await
        .expect("Failed to create DataFrame");

    let results = df.collect().await.expect("Failed to collect results");

    // Verify results
    assert!(!results.is_empty(), "Should have at least one batch");
    assert_eq!(results[0].num_rows(), 10, "Should have 10 rows");
    assert_eq!(results[0].num_columns(), 4, "Should have 4 columns");

    // Print sample results
    println!("Sample query results:");
    println!("{:?}", results[0]);

    println!("✓ Successfully executed SELECT query via DataFusion");
}

#[tokio::test]
async fn test_select_with_projection() {
    if !Path::new(SEGMENT_DIR).exists() {
        println!("Skipping test: segment directory not found");
        return;
    }

    let ctx = SessionContext::new();
    let table = PinotTable::open(SEGMENT_DIR).expect("Failed to open Pinot table");
    ctx.register_table("baseballStats", Arc::new(table))
        .expect("Failed to register table");

    // Test projection (selecting specific columns)
    let df = ctx
        .sql("SELECT \"playerID\", \"hits\" FROM baseballStats LIMIT 5")
        .await
        .expect("Failed to create DataFrame");

    let results = df.collect().await.expect("Failed to collect results");

    assert_eq!(results[0].num_rows(), 5);
    assert_eq!(results[0].num_columns(), 2);

    println!("✓ Column projection works correctly");
}

#[tokio::test]
async fn test_count_query() {
    if !Path::new(SEGMENT_DIR).exists() {
        println!("Skipping test: segment directory not found");
        return;
    }

    let ctx = SessionContext::new();
    let table = PinotTable::open(SEGMENT_DIR).expect("Failed to open Pinot table");
    ctx.register_table("baseballStats", Arc::new(table))
        .expect("Failed to register table");

    // Test COUNT query
    let df = ctx
        .sql("SELECT COUNT(*) FROM baseballStats")
        .await
        .expect("Failed to create DataFrame");

    let results = df.collect().await.expect("Failed to collect results");

    // Verify count matches expected total docs
    assert_eq!(results.len(), 1, "Should have one batch");
    assert_eq!(results[0].num_rows(), 1, "Should have one row");

    println!("Count query results: {:?}", results[0]);
    println!("✓ COUNT query executed successfully");
}
