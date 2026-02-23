//! Compare performance of RAW-encoded vs dictionary-encoded columns

use datafusion::prelude::*;
use datafusion_pinot::PinotCatalog;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = "/tmp/pinot/quickstart/PinotServerDataDir0";

    let ctx = SessionContext::new();
    let catalog = PinotCatalog::new(data_dir)?;
    ctx.register_catalog("pinot", Arc::new(catalog));

    println!("=== RAW vs Dictionary Encoding Performance ===\n");

    // Test 1: RAW-encoded column (playerID)
    println!("Test 1: RAW-encoded column (playerID)");
    let start = Instant::now();
    let df = ctx
        .sql(r#"SELECT "playerID", "hits", "homeRuns" FROM pinot.default."baseballStats" LIMIT 10"#)
        .await?;
    let parse_time = start.elapsed();

    let start = Instant::now();
    let results = df.collect().await?;
    let exec_time = start.elapsed();

    println!("  Parse: {:?}", parse_time);
    println!("  Execute: {:?}", exec_time);
    println!("  Total: {:?}\n", parse_time + exec_time);

    // Test 2: Dictionary-encoded columns only (playerName)
    println!("Test 2: Dictionary-encoded columns (playerName)");
    let start = Instant::now();
    let df = ctx
        .sql(r#"SELECT "playerName", "hits", "homeRuns" FROM pinot.default."baseballStats" LIMIT 10"#)
        .await?;
    let parse_time = start.elapsed();

    let start = Instant::now();
    let _results = df.collect().await?;
    let exec_time = start.elapsed();

    println!("  Parse: {:?}", parse_time);
    println!("  Execute: {:?}", exec_time);
    println!("  Total: {:?}\n", parse_time + exec_time);

    // Test 3: Only dictionary columns
    println!("Test 3: Only dictionary-encoded columns");
    let start = Instant::now();
    let df = ctx
        .sql(r#"SELECT "hits", "homeRuns" FROM pinot.default."baseballStats" LIMIT 10"#)
        .await?;
    let parse_time = start.elapsed();

    let start = Instant::now();
    let _results = df.collect().await?;
    let exec_time = start.elapsed();

    println!("  Parse: {:?}", parse_time);
    println!("  Execute: {:?}", exec_time);
    println!("  Total: {:?}\n", parse_time + exec_time);

    // Test 4: Full scan with RAW column
    println!("Test 4: Full scan with RAW column (no LIMIT)");
    let start = Instant::now();
    let df = ctx
        .sql(r#"SELECT "playerID", "hits" FROM pinot.default."baseballStats""#)
        .await?;
    let parse_time = start.elapsed();

    let start = Instant::now();
    let _results = df.collect().await?;
    let exec_time = start.elapsed();

    println!("  Parse: {:?}", parse_time);
    println!("  Execute: {:?}", exec_time);
    println!("  Total: {:?}\n", parse_time + exec_time);

    // Test 5: Full scan with dictionary column only
    println!("Test 5: Full scan with dictionary column only (no LIMIT)");
    let start = Instant::now();
    let df = ctx
        .sql(r#"SELECT "playerName", "hits" FROM pinot.default."baseballStats""#)
        .await?;
    let parse_time = start.elapsed();

    let start = Instant::now();
    let _results = df.collect().await?;
    let exec_time = start.elapsed();

    println!("  Parse: {:?}", parse_time);
    println!("  Execute: {:?}", exec_time);
    println!("  Total: {:?}\n", parse_time + exec_time);

    println!("Results from Test 1:");
    for batch in results {
        println!("{:?}", batch);
    }

    Ok(())
}
