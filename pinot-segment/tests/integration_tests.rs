use pinot_segment::SegmentReader;
use std::path::Path;

const SEGMENT_DIR: &str = "/tmp/pinot/quickstart/PinotServerDataDir0/baseballStats_OFFLINE/baseballStats_OFFLINE_0_e40936cc-16f8-490e-a85f-bc61a9abee66/v3";

#[test]
fn test_read_baseball_stats_metadata() {
    if !Path::new(SEGMENT_DIR).exists() {
        println!("Skipping test: segment directory not found");
        return;
    }

    let reader = SegmentReader::open(SEGMENT_DIR).expect("Failed to open segment");
    let metadata = reader.metadata();

    assert_eq!(metadata.table_name, "baseballStats");
    assert_eq!(metadata.total_docs, 97889);

    // Check hits column
    let hits_col = metadata.get_column("hits").expect("hits column not found");
    assert_eq!(hits_col.cardinality, 250);
    assert_eq!(hits_col.bits_per_element, 8);
    assert!(hits_col.has_dictionary);

    println!("✓ Metadata loaded successfully");
}

#[test]
fn test_read_dict_encoded_int_column() {
    if !Path::new(SEGMENT_DIR).exists() {
        println!("Skipping test: segment directory not found");
        return;
    }

    let reader = SegmentReader::open(SEGMENT_DIR).expect("Failed to open segment");

    // Read hits column (dictionary-encoded INT)
    let hits = reader.read_int_column("hits").expect("Failed to read hits column");

    assert_eq!(hits.len(), 97889, "Should have 97889 rows");

    // Verify values are in valid range (0-262 based on metadata)
    for (idx, &value) in hits.iter().enumerate() {
        assert!(
            value >= 0 && value <= 262,
            "Value {} at index {} out of range",
            value,
            idx
        );
    }

    // Print some sample values
    println!("Sample hits values:");
    for i in 0..10 {
        println!("  Row {}: {}", i, hits[i]);
    }

    println!("✓ Successfully read {} hits values", hits.len());
}

#[test]
fn test_read_dict_encoded_string_column() {
    if !Path::new(SEGMENT_DIR).exists() {
        println!("Skipping test: segment directory not found");
        return;
    }

    let reader = SegmentReader::open(SEGMENT_DIR).expect("Failed to open segment");

    // Read teamID column (dictionary-encoded STRING)
    let team_ids = reader
        .read_string_column("teamID")
        .expect("Failed to read teamID column");

    assert_eq!(team_ids.len(), 97889, "Should have 97889 rows");

    // Verify all values are non-empty
    for (idx, value) in team_ids.iter().enumerate() {
        assert!(
            !value.is_empty(),
            "Empty teamID at index {}",
            idx
        );
    }

    // Print some sample values
    println!("Sample teamID values:");
    for i in 0..10 {
        println!("  Row {}: {}", i, team_ids[i]);
    }

    println!("✓ Successfully read {} teamID values", team_ids.len());
}

#[test]
fn test_read_multiple_columns() {
    if !Path::new(SEGMENT_DIR).exists() {
        println!("Skipping test: segment directory not found");
        return;
    }

    let reader = SegmentReader::open(SEGMENT_DIR).expect("Failed to open segment");

    // Read multiple columns
    let hits = reader.read_int_column("hits").expect("Failed to read hits");
    let home_runs = reader
        .read_int_column("homeRuns")
        .expect("Failed to read homeRuns");
    let team_ids = reader
        .read_string_column("teamID")
        .expect("Failed to read teamID");

    assert_eq!(hits.len(), 97889);
    assert_eq!(home_runs.len(), 97889);
    assert_eq!(team_ids.len(), 97889);

    // Print first few rows as a mini table
    println!("\nFirst 10 rows:");
    println!("{:<10} {:<10} {:<10}", "teamID", "hits", "homeRuns");
    println!("{}", "-".repeat(30));
    for i in 0..10 {
        println!("{:<10} {:<10} {:<10}", team_ids[i], hits[i], home_runs[i]);
    }

    println!("\n✓ Successfully read multiple columns");
}
