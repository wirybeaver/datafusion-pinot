//! Example: Query Pinot using controller-based discovery
//!
//! This example demonstrates how to use the controller mode to dynamically
//! discover tables from a running Pinot cluster while reading segment data
//! from the local filesystem.
//!
//! ## Prerequisites
//!
//! 1. Start Pinot with volume-mounted data directory:
//!    ```bash
//!    docker run -d --name pinot-quickstart \
//!      -p 9000:9000 \
//!      -v /tmp/pinot:/tmp/data \
//!      apachepinot/pinot:latest QuickStart \
//!      -type batch \
//!      -dataDir /tmp/data
//!    ```
//!
//! 2. Build with controller feature:
//!    ```bash
//!    cargo build --features controller --example query_controller
//!    ```
//!
//! 3. Run the example:
//!    ```bash
//!    PINOT_CONTROLLER_URL=http://localhost:9000 \
//!    PINOT_SEGMENT_DIR=/tmp/pinot/quickstart/PinotServerDataDir0 \
//!    cargo run --features controller --example query_controller
//!    ```

use datafusion::prelude::*;
use datafusion_pinot::PinotCatalog;
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get configuration from environment variables
    let controller_url = env::var("PINOT_CONTROLLER_URL")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());

    let segment_dir = env::var("PINOT_SEGMENT_DIR")
        .unwrap_or_else(|_| "/tmp/pinot/quickstart/PinotServerDataDir0".to_string());

    println!("Pinot Controller: {}", controller_url);
    println!("Segment Directory: {}", segment_dir);
    println!();

    // Create DataFusion context
    let ctx = SessionContext::new();

    // Register Pinot catalog using controller mode
    println!("Registering Pinot catalog with controller-based discovery...");
    let catalog = PinotCatalog::builder()
        .controller(&controller_url)
        .with_segment_dir(&segment_dir)
        .build()?;

    ctx.register_catalog("pinot", Arc::new(catalog));

    // List available tables
    println!("Discovering tables from controller...");
    let catalog = ctx.catalog("pinot").unwrap();
    let schema = catalog.schema("default").unwrap();
    let tables = schema.table_names();
    println!("Available tables: {:?}", tables);
    println!();

    // Query the baseballStats table
    if tables.contains(&"baseballStats".to_string()) {
        println!("Querying baseballStats table...");
        println!("---");

        // Count query
        let count_query = "SELECT COUNT(*) as total_records FROM pinot.default.baseballStats";
        println!("Query: {}", count_query);
        let df = ctx.sql(count_query).await?;
        let results = df.collect().await?;
        println!("Results:");
        for batch in results {
            println!("{:?}", batch);
        }
        println!();

        // Sample query with aggregation
        let sample_query =
            "SELECT playerName, AVG(homeRuns) as avg_home_runs \
             FROM pinot.default.baseballStats \
             GROUP BY playerName \
             ORDER BY avg_home_runs DESC \
             LIMIT 5";
        println!("Query: {}", sample_query);
        let df = ctx.sql(sample_query).await?;
        let results = df.collect().await?;
        println!("Results:");
        for batch in results {
            println!("{:?}", batch);
        }
    } else {
        println!("baseballStats table not found. Available tables: {:?}", tables);
    }

    Ok(())
}
