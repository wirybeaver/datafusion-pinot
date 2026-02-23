//! Profile query execution to find bottleneck

use datafusion::prelude::*;
use datafusion_pinot::PinotCatalog;
use std::env;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let controller_url = env::var("PINOT_CONTROLLER_URL")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());

    let segment_dir = env::var("PINOT_SEGMENT_DIR")
        .unwrap_or_else(|_| "/tmp/pinot/quickstart/PinotServerDataDir0".to_string());

    println!("=== Profiling Query Execution ===\n");

    // Step 1: Create context
    let start = Instant::now();
    let ctx = SessionContext::new();
    println!("1. Create context: {:?}", start.elapsed());

    // Step 2: Build catalog
    let start = Instant::now();
    let catalog = PinotCatalog::builder()
        .controller(&controller_url)
        .with_segment_dir(&segment_dir)
        .build()?;
    println!("2. Build catalog: {:?}", start.elapsed());

    // Step 3: Register catalog
    let start = Instant::now();
    ctx.register_catalog("pinot", Arc::new(catalog));
    println!("3. Register catalog: {:?}", start.elapsed());

    // Step 4: Parse SQL
    let query = r#"SELECT "playerName", AVG("homeRuns") as avg_home_runs
                   FROM pinot.default.baseballStats
                   GROUP BY "playerName"
                   ORDER BY avg_home_runs DESC
                   LIMIT 5"#;
    let start = Instant::now();
    let df = ctx.sql(query).await?;
    println!("4. Parse SQL: {:?}", start.elapsed());

    // Step 5: Execute query (this includes table discovery, schema inference, and data reading)
    let start = Instant::now();
    let results = df.collect().await?;
    let exec_time = start.elapsed();
    println!("5. Execute query: {:?}", exec_time);

    println!("\nResults:");
    for batch in results {
        println!("{:?}", batch);
    }

    Ok(())
}
