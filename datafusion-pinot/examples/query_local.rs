use datafusion::prelude::*;
use datafusion_pinot::PinotCatalog;
use std::sync::Arc;

/// Example: Query local Pinot segments using DataFusion
///
/// This demonstrates the **filesystem mode** where tables are discovered
/// by scanning local directories.
///
/// ## When to use filesystem mode:
/// - Static table discovery from local directories
/// - No need for controller HTTP API
/// - Simpler setup for local testing
///
/// ## When to use controller mode:
/// - Dynamic table discovery from running Pinot cluster
/// - Tables may change over time
/// - See query_controller.rs example (requires 'controller' feature)
///
/// This demonstrates how to:
/// 1. Create a Pinot catalog from a local data directory
/// 2. Register it with DataFusion
/// 3. Execute SQL queries against Pinot tables
///
/// Usage:
///   PINOT_DATA_DIR=/path/to/data cargo run --example query_local
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Point to your Pinot data directory
    let data_dir = std::env::var("PINOT_DATA_DIR")
        .unwrap_or_else(|_| "/tmp/pinot/quickstart/PinotServerDataDir0".to_string());

    println!("📊 DataFusion + Pinot Integration Example");
    println!("=========================================\n");

    // Create DataFusion session context
    let ctx = SessionContext::new();

    // Create and register Pinot catalog (filesystem mode)
    println!("🔌 Connecting to Pinot data directory: {}", data_dir);

    // Option 1: Using the simple constructor (recommended for filesystem mode)
    let catalog = PinotCatalog::new(&data_dir)?;

    // Option 2: Using the builder pattern (for more control)
    // let catalog = PinotCatalog::builder()
    //     .filesystem(&data_dir)
    //     .build()?;

    ctx.register_catalog("pinot", Arc::new(catalog));

    // Discover available tables
    println!("📋 Discovering tables...\n");
    let catalog = ctx.catalog("pinot").unwrap();
    let schema = catalog.schema("default").unwrap();
    let tables = schema.table_names();
    println!("Found {} tables:", tables.len());
    for table in &tables {
        println!("  - {}", table);
    }
    println!();

    // Example 1: Simple COUNT query
    println!("📊 Example 1: Count total rows");
    println!("SQL: SELECT COUNT(*) FROM pinot.default.\"baseballStats\"");
    let df = ctx
        .sql("SELECT COUNT(*) FROM pinot.default.\"baseballStats\"")
        .await?;
    let results = df.collect().await?;
    println!("Result:");
    for batch in results {
        println!("{:?}\n", batch);
    }

    // Example 2: SELECT with projection
    println!("📊 Example 2: Select specific columns");
    println!("SQL: SELECT \"playerID\", \"hits\", \"homeRuns\" FROM ... LIMIT 10");
    let df = ctx
        .sql("SELECT \"playerID\", \"hits\", \"homeRuns\" FROM pinot.default.\"baseballStats\" LIMIT 10")
        .await?;
    let results = df.collect().await?;
    println!("Result:");
    for batch in results {
        println!("{:?}\n", batch);
    }

    // Example 3: Aggregation
    println!("📊 Example 3: Aggregate statistics");
    println!("SQL: SELECT SUM(\"hits\"), AVG(\"homeRuns\"), MAX(\"strikeouts\") FROM ...");
    let df = ctx
        .sql("SELECT SUM(\"hits\") as total_hits, AVG(\"homeRuns\") as avg_homeruns, MAX(\"strikeouts\") as max_strikeouts FROM pinot.default.\"baseballStats\"")
        .await?;
    let results = df.collect().await?;
    println!("Result:");
    for batch in results {
        println!("{:?}\n", batch);
    }

    // Example 4: GROUP BY
    println!("📊 Example 4: Group by and aggregate");
    println!("SQL: SELECT \"teamID\", COUNT(*), SUM(\"hits\") FROM ... GROUP BY \"teamID\" LIMIT 10");
    let df = ctx
        .sql("SELECT \"teamID\", COUNT(*) as games, SUM(\"hits\") as total_hits FROM pinot.default.\"baseballStats\" GROUP BY \"teamID\" ORDER BY total_hits DESC LIMIT 10")
        .await?;
    let results = df.collect().await?;
    println!("Result:");
    for batch in results {
        println!("{:?}\n", batch);
    }

    // Example 5: Multiple tables
    if tables.contains(&"dimBaseballTeams".to_string()) {
        println!("📊 Example 5: Query multiple tables");
        println!("SQL: SELECT COUNT(*) FROM dimBaseballTeams");
        let df = ctx
            .sql("SELECT COUNT(*) FROM pinot.default.\"dimBaseballTeams\"")
            .await?;
        let results = df.collect().await?;
        println!("Result:");
        for batch in results {
            println!("{:?}\n", batch);
        }
    }

    println!("✅ All examples completed successfully!");
    Ok(())
}
