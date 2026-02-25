//! Benchmark queries to measure performance

use datafusion::prelude::*;
use datafusion_pinot::PinotCatalog;
use std::sync::Arc;
use std::time::Instant;

mod benchmark_cases;
use benchmark_cases::{BenchmarkCase, BENCHMARK_CASES};

struct Benchmark {
    case: &'static BenchmarkCase,
}

impl Benchmark {
    fn new(case: &'static BenchmarkCase) -> Self {
        Self { case }
    }

    async fn run(&self, ctx: &SessionContext) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
        println!("{}. {}:", self.case.name, self.case.description);
        println!("   SQL: {}", self.case.sql);

        let start = Instant::now();
        let df = ctx.sql(self.case.sql).await?;
        let results = df.collect().await?;
        let duration = start.elapsed();

        let row_count: usize = results.iter().map(|b| b.num_rows()).sum();
        if row_count > 0 {
            println!("   Rows: {}", row_count);
        }
        println!("   Time: {:?}\n", duration);

        Ok(duration)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = "/tmp/pinot/quickstart/PinotServerDataDir0";

    let ctx = SessionContext::new();
    let catalog = PinotCatalog::new(data_dir)?;
    ctx.register_catalog("pinot", Arc::new(catalog));

    println!("=== DataFusion-Pinot Benchmark ===");
    println!("Dataset: baseballStats (97,889 rows)\n");

    for case in BENCHMARK_CASES {
        let benchmark = Benchmark::new(case);
        benchmark.run(&ctx).await?;
    }

    println!("=== Benchmark Complete ===");

    Ok(())
}
