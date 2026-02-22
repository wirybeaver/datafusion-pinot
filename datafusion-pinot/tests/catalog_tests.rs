use datafusion::prelude::*;
use datafusion_pinot::PinotCatalog;
use std::path::Path;
use std::sync::Arc;

const DATA_DIR: &str = "/tmp/pinot/quickstart/PinotServerDataDir0";

#[tokio::test]
async fn test_catalog_table_discovery() {
    if !Path::new(DATA_DIR).exists() {
        println!("Skipping test: data directory not found");
        return;
    }

    let ctx = SessionContext::new();

    // Register Pinot catalog
    let catalog = PinotCatalog::new(DATA_DIR).expect("Failed to create catalog");
    ctx.register_catalog("pinot", Arc::new(catalog));

    // Query using fully qualified table name (use quotes for case-sensitive names)
    let df = ctx
        .sql("SELECT COUNT(*) FROM pinot.default.\"baseballStats\"")
        .await
        .expect("Failed to create DataFrame");

    let results = df.collect().await.expect("Failed to collect results");

    println!("Catalog query results: {:?}", results[0]);

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 1);

    println!("✓ Catalog-based table discovery successful");
}

#[tokio::test]
async fn test_catalog_multiple_tables() {
    if !Path::new(DATA_DIR).exists() {
        println!("Skipping test: data directory not found");
        return;
    }

    let ctx = SessionContext::new();
    let catalog = PinotCatalog::new(DATA_DIR).expect("Failed to create catalog");

    ctx.register_catalog("pinot", Arc::new(catalog));

    // Query first table
    let df1 = ctx
        .sql("SELECT COUNT(*) FROM pinot.default.\"baseballStats\"")
        .await
        .expect("Failed to query baseballStats");

    let results1 = df1.collect().await.expect("Failed to collect");
    println!("baseballStats count: {:?}", results1[0]);

    // Query second table if it exists
    let result = ctx
        .sql("SELECT COUNT(*) FROM pinot.default.\"dimBaseballTeams\"")
        .await;

    if let Ok(df2) = result {
        let results2 = df2.collect().await.expect("Failed to collect");
        println!("dimBaseballTeams count: {:?}", results2[0]);
        println!("✓ Successfully queried multiple tables via catalog");
    } else {
        println!("✓ Successfully queried baseballStats via catalog");
    }
}

#[tokio::test]
async fn test_catalog_select_query() {
    if !Path::new(DATA_DIR).exists() {
        println!("Skipping test: data directory not found");
        return;
    }

    let ctx = SessionContext::new();
    let catalog = PinotCatalog::new(DATA_DIR).expect("Failed to create catalog");

    ctx.register_catalog("pinot", Arc::new(catalog));

    // Execute SELECT with projection
    let df = ctx
        .sql("SELECT \"playerID\", \"hits\" FROM pinot.default.\"baseballStats\" LIMIT 10")
        .await
        .expect("Failed to create DataFrame");

    let results = df.collect().await.expect("Failed to collect results");

    assert!(!results.is_empty());
    assert_eq!(results[0].num_columns(), 2);

    println!("✓ Catalog SELECT query successful");
}
