use pinot_segment::SegmentReader;
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    let segment_path = if args.len() > 1 {
        &args[1]
    } else {
        // Default test segment
        "/tmp/pinot/quickstart/PinotServerDataDir0/baseballStats_OFFLINE/baseballStats_OFFLINE_0_e40936cc-16f8-490e-a85f-bc61a9abee66/v3"
    };

    println!("Opening Pinot segment: {}", segment_path);

    let reader = SegmentReader::open(segment_path).expect("Failed to open segment");

    // Print metadata
    let metadata = reader.metadata();
    println!("\n=== Segment Metadata ===");
    println!("Table name: {}", metadata.table_name);
    println!("Segment name: {}", metadata.segment_name);
    println!("Total docs: {}", metadata.total_docs);
    println!("Number of columns: {}", metadata.columns.len());

    println!("\n=== Columns ===");
    let mut columns: Vec<_> = metadata.columns.values().collect();
    columns.sort_by_key(|c| &c.name);

    for col in columns.iter() {
        println!(
            "  {} ({:?}): cardinality={}, has_dict={}, bits_per_elem={}",
            col.name, col.data_type, col.cardinality, col.has_dictionary, col.bits_per_element
        );
    }

    // Read some sample data
    println!("\n=== Sample Data (first 10 rows) ===");

    // Read dictionary-encoded columns
    let team_ids = reader.read_string_column("teamID").expect("Failed to read teamID");
    let hits = reader.read_int_column("hits").expect("Failed to read hits");
    let home_runs = reader.read_int_column("homeRuns").expect("Failed to read homeRuns");

    println!("{:<10} {:<10} {:<10}", "teamID", "hits", "homeRuns");
    println!("{}", "-".repeat(30));

    for i in 0..10.min(team_ids.len()) {
        println!("{:<10} {:<10} {:<10}", team_ids[i], hits[i], home_runs[i]);
    }

    println!("\n=== Statistics ===");
    let total_hits: i32 = hits.iter().sum();
    let total_home_runs: i32 = home_runs.iter().sum();
    let avg_hits = total_hits as f64 / hits.len() as f64;
    let avg_home_runs = total_home_runs as f64 / home_runs.len() as f64;

    println!("Total hits: {}", total_hits);
    println!("Total home runs: {}", total_home_runs);
    println!("Average hits per game: {:.2}", avg_hits);
    println!("Average home runs per game: {:.2}", avg_home_runs);
}
