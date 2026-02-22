# DataFusion-Pinot: Apache Pinot Integration for Apache DataFusion

This project integrates Apache Pinot's segment format with Apache DataFusion's query engine, enabling SQL queries over Pinot data using DataFusion.

## Overview

Apache Pinot stores data in columnar segment files optimized for analytical queries. This integration allows you to:

- Read Pinot segment files directly without network overhead
- Execute SQL queries using DataFusion's high-performance engine
- Leverage DataFusion's optimizations and Apache Arrow integration
- Join Pinot data with other DataFusion sources (Parquet, CSV, etc.)

## Project Status

### ✅ Milestone 1: Dictionary-Encoded Column Reading (COMPLETE)

**Implemented:**
- ✅ Metadata parser (`metadata.properties`)
- ✅ Index map parser for column locations
- ✅ Dictionary reader for all supported types (INT, LONG, FLOAT, DOUBLE, STRING)
- ✅ Fixed-bit forward index decoder for dictionary IDs
- ✅ High-level SegmentReader API
- ✅ Support for both fixed-length and variable-length string dictionaries
- ✅ Integration tests with real Pinot segments

**Supported Data Types:**
- INT (dictionary-encoded)
- LONG (dictionary-encoded)
- FLOAT (dictionary-encoded)
- DOUBLE (dictionary-encoded)
- STRING (dictionary-encoded, fixed-length)

### 🚧 Milestone 2: RAW Encoding Support (INFRASTRUCTURE COMPLETE)

**Implemented:**
- ✅ VarByteChunkReader for V4 format
- ✅ V4 header parsing (mixed BE/LE endianness)
- ✅ Metadata binary search for chunk lookup
- ✅ Chunk-based value extraction
- ✅ Detection of compression types
- ✅ Infrastructure for uncompressed RAW columns

**Current Limitation:**
- ⚠️ Compression support deferred (requires external dependencies like LZ4, Snappy, Zstandard)
- The baseballStats test data uses LZ4-compressed RAW columns
- All infrastructure is correct and ready for uncompressed data or future compression support

**Why defer compression:**
- Maintains zero-dependency principle for `pinot-segment` crate
- Core reading logic is complete and tested
- Compression can be added as optional feature with external crates

### 🚧 Upcoming Milestones

**Milestone 3: DataFusion Integration**
- TableProvider trait implementation
- ExecutionPlan for segment scanning
- Schema mapping (Pinot → Arrow)
- SQL query execution via DataFusion

**Milestone 4: Multi-Segment Support**
- Scan multiple segments in a table directory
- PinotCatalog for table discovery
- Segment-based partitioning

**Milestone 5: Validation & Documentation**
- Comprehensive test suite
- Result validation against Pinot HTTP API
- Usage examples and documentation

## Quick Start

### Prerequisites

You need a running Pinot instance with sample data. The quickest way is using Docker:

```bash
# Start Pinot with quickstart data
docker run \
    --name pinot-quickstart \
    -p 9000:9000 \
    -d apachepinot/pinot:latest QuickStart \
    -type batch
```

This will create segment files in the container. To access them:

```bash
# Copy segments from container to local filesystem
docker cp pinot-quickstart:/opt/pinot/data /tmp/pinot
```

### Reading a Pinot Segment

```rust
use pinot_segment::SegmentReader;

// Open a segment directory
let reader = SegmentReader::open(
    "/tmp/pinot/quickstart/PinotServerDataDir0/baseballStats_OFFLINE/baseballStats_OFFLINE_0.../v3"
)?;

// Read column data
let hits = reader.read_int_column("hits")?;
let team_ids = reader.read_string_column("teamID")?;

println!("Total docs: {}", reader.total_docs());
println!("First hit value: {}", hits[0]);
println!("First team: {}", team_ids[0]);
```

### Running the Example

```bash
# Build and run the example
cargo run --example read_segment

# Or specify a custom segment path
cargo run --example read_segment /path/to/segment/v3
```

## Architecture

```
datafusion-pinot/
├── pinot-segment/           # Core segment reading library (Milestone 1 ✅)
│   ├── metadata.rs          # Parse metadata.properties
│   ├── index_map.rs         # Parse index_map (column offsets)
│   ├── forward_index/       # Forward index readers
│   │   ├── dictionary.rs    # Dictionary value lookup
│   │   ├── fixed_bit.rs     # Bit-packed dict ID decoder
│   │   └── var_byte.rs      # Variable-byte RAW decoder (TODO)
│   └── segment_reader.rs    # High-level API
│
├── datafusion-pinot/        # DataFusion integration (TODO)
│   ├── catalog.rs           # Table discovery
│   ├── table.rs             # TableProvider implementation
│   ├── exec.rs              # ExecutionPlan for scanning
│   └── schema.rs            # Pinot → Arrow type mapping
│
└── examples/
    └── read_segment.rs      # Basic usage example
```

## Implementation Details

### Pinot Segment Format (v3)

Pinot segments store data in a columnar format with the following structure:

**Files in a segment directory:**
- `metadata.properties`: Column metadata (data type, cardinality, encoding)
- `index_map`: Offset locations for dictionaries and forward indexes
- `columns.psf`: Packed storage file containing all data

**Dictionary-Encoded Columns:**

1. **Dictionary** (in `columns.psf`):
   - 8-byte magic marker (0xDEADBEEFDEAFBEAD)
   - Fixed-size values (INT/LONG/FLOAT/DOUBLE: 4-8 bytes each)
   - Fixed-length strings (padded with null bytes)

2. **Forward Index** (in `columns.psf`):
   - 8-byte magic marker
   - Bit-packed dictionary IDs (uses minimal bits based on cardinality)
   - Big-endian byte order

**Bit-Packing Algorithm:**

For a column with cardinality N, dictionary IDs range from 0 to N-1. The forward index packs these IDs using `ceil(log2(N))` bits per value, based on PinotDataBitSet.java:

```rust
// Read dict_id for doc at position doc_id
let bit_offset = doc_id * bits_per_value;
let byte_offset = bit_offset / 8;
let bit_offset_in_first_byte = bit_offset % 8;

// Extract bits across byte boundaries (big-endian)
// ... (see fixed_bit.rs for full implementation)
```

## Supported Features

### ✅ Implemented
- Dictionary-encoded columns (INT, LONG, FLOAT, DOUBLE, STRING)
- Fixed-bit forward index decoding
- Metadata parsing (v3 format)
- Index map parsing

### ❌ Not Yet Supported
- RAW (non-dictionary) encoding
- Compression (SNAPPY, ZSTANDARD, LZ4)
- Multi-value columns (arrays)
- Inverted indexes
- Star-tree indexes
- Timestamp columns
- BYTES data type
- V1/V2 segment formats

## Testing

```bash
# Run all tests
cargo test

# Run only pinot-segment tests
cargo test -p pinot-segment

# Run integration tests with real segment
cargo test -p pinot-segment --test integration_tests -- --nocapture
```

**Integration tests require:**
- Pinot segment files at `/tmp/pinot/quickstart/PinotServerDataDir0/baseballStats_OFFLINE/`
- Use Docker setup above to generate test data

## Performance Considerations

**Current Implementation:**
- Reads entire columns into memory (`Vec<T>`)
- Suitable for segments that fit in memory
- Efficient bit-packing decoder
- No compression support yet

**Future Optimizations:**
- Streaming/chunked reading for large columns
- Compression support (requires external crates)
- Lazy dictionary loading
- Memory-mapped file access

## Contributing

This is a new project implementing the plan documented in the repository. Contributions are welcome!

### Development Guidelines

See [AGENTS.md](AGENTS.md) for detailed engineering practices.

**Key principles:**
- Incremental development with focused commits
- Test-driven development (unit + integration tests)
- Zero external dependencies in `pinot-segment` (except std)
- Clear error messages

## License

Apache License 2.0 (same as Apache Pinot and Apache DataFusion)

## References

- [Apache Pinot](https://pinot.apache.org/)
- [Apache DataFusion](https://datafusion.apache.org/)
- [Pinot Segment Format](https://docs.pinot.apache.org/basics/components/table#segment)
- Java Reference Implementation: `pinot-segment-local/src/main/java/org/apache/pinot/segment/local/`
