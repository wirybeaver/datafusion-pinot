//! Detailed profiling with multiple runs to separate compilation from execution

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

    println!("=== Detailed Query Profiling ===\n");

    // Setup
    let ctx = SessionContext::new();
    let catalog = PinotCatalog::builder()
        .controller(&controller_url)
        .with_segment_dir(&segment_dir)
        .build()?;
    ctx.register_catalog("pinot", Arc::new(catalog));

    let query = r#"SELECT "playerName", AVG("homeRuns") as avg_home_runs
                   FROM pinot.default.baseballStats
                   GROUP BY "playerName"
                   ORDER BY avg_home_runs DESC
                   LIMIT 5"#;

    // Run 1: First execution (may include schema inference)
    println!("Run 1 (cold - includes table discovery & schema inference):");
    let start = Instant::now();
    let df = ctx.sql(query).await?;
    let parse_time = start.elapsed();

    let start = Instant::now();
    let results = df.collect().await?;
    let exec_time = start.elapsed();

    println!("  Parse SQL: {:?}", parse_time);
    println!("  Execute: {:?}", exec_time);
    println!("  Total: {:?}\n", parse_time + exec_time);

    // Run 2: Second execution (schema cached?)
    println!("Run 2 (warm - schema may be cached):");
    let start = Instant::now();
    let df = ctx.sql(query).await?;
    let parse_time = start.elapsed();

    let start = Instant::now();
    let _results = df.collect().await?;
    let exec_time = start.elapsed();

    println!("  Parse SQL: {:?}", parse_time);
    println!("  Execute: {:?}", exec_time);
    println!("  Total: {:?}\n", parse_time + exec_time);

    // Run 3: Third execution
    println!("Run 3 (warm):");
    let start = Instant::now();
    let df = ctx.sql(query).await?;
    let parse_time = start.elapsed();

    let start = Instant::now();
    let _results = df.collect().await?;
    let exec_time = start.elapsed();

    println!("  Parse SQL: {:?}", parse_time);
    println!("  Execute: {:?}", exec_time);
    println!("  Total: {:?}\n", parse_time + exec_time);

    // Test simple COUNT(*) for comparison
    println!("COUNT(*) query for comparison:");
    let start = Instant::now();
    let df = ctx.sql("SELECT COUNT(*) FROM pinot.default.baseballStats").await?;
    let parse_time = start.elapsed();

    let start = Instant::now();
    let _results = df.collect().await?;
    let exec_time = start.elapsed();

    println!("  Parse SQL: {:?}", parse_time);
    println!("  Execute: {:?}", exec_time);
    println!("  Total: {:?}\n", parse_time + exec_time);

    println!("Results:");
    for batch in results {
        println!("{:?}", batch);
    }

    Ok(())
}
