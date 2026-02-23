//! Check column encodings for baseballStats

use pinot_segment::SegmentReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let segment_path = "/tmp/pinot/quickstart/PinotServerDataDir0/baseballStats_OFFLINE/baseballStats_OFFLINE_0_e40936cc-16f8-490e-a85f-bc61a9abee66/v3";

    let reader = SegmentReader::open(segment_path)?;
    let metadata = reader.metadata();

    println!("Segment: {}", metadata.segment_name);
    println!("Total docs: {}\n", metadata.total_docs);

    // Check playerName encoding
    if let Ok(col) = metadata.get_column("playerName") {
        println!("playerName column:");
        println!("  Data type: {:?}", col.data_type);
        println!("  Cardinality: {}", col.cardinality);
        println!("  Has dictionary: {}", col.has_dictionary);
        println!("  Total docs: {}", col.total_docs);
        println!();
    }

    // Check homeRuns encoding
    if let Ok(col) = metadata.get_column("homeRuns") {
        println!("homeRuns column:");
        println!("  Data type: {:?}", col.data_type);
        println!("  Cardinality: {}", col.cardinality);
        println!("  Has dictionary: {}", col.has_dictionary);
        println!("  Total docs: {}", col.total_docs);
        println!();
    }

    // Check playerID too
    if let Ok(col) = metadata.get_column("playerID") {
        println!("playerID column:");
        println!("  Data type: {:?}", col.data_type);
        println!("  Cardinality: {}", col.cardinality);
        println!("  Has dictionary: {}", col.has_dictionary);
        println!("  Total docs: {}", col.total_docs);
        println!();
    }

    Ok(())
}
