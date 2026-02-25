# DataFusion-Pinot: Apache Pinot Integration for Apache DataFusion

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A Rust library that integrates Apache Pinot's segment format with Apache DataFusion's query engine, enabling high-performance SQL queries over Pinot data without network overhead.

## Features

- **Basic SQL Support** - Execute SQL queries on Pinot segments using DataFusion
- **Dictionary & RAW Encoding** - Read both dictionary-encoded and RAW columns
- **LZ4 Compression** - Support for LZ4-compressed RAW columns
- **Automatic Discovery** - Catalog-based table discovery via filesystem or controller API

## Quick Start

### Prerequisites

You need Pinot segment files on your local filesystem. The quickest way is using Docker:

```bash
# Start Pinot with quickstart data
docker run \
    --name pinot-quickstart \
    -p 9000:9000 \
    -d apachepinot/pinot:latest QuickStart \
    -type batch

# Copy segments to local filesystem
docker cp pinot-quickstart:/opt/pinot/data /tmp/pinot
```

### Basic Usage

```rust
use datafusion::prelude::*;
use datafusion_pinot::PinotCatalog;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create DataFusion context
    let ctx = SessionContext::new();

    // Register Pinot catalog
    let catalog = PinotCatalog::new("/tmp/pinot/quickstart/PinotServerDataDir0")?;
    ctx.register_catalog("pinot", Arc::new(catalog));

    // Execute SQL queries!
    let df = ctx
        .sql("SELECT COUNT(*) FROM pinot.default.\"baseballStats\"")
        .await?;

    let results = df.collect().await?;
    println!("{:?}", results);

    Ok(())
}
```

## Discovery Modes

DataFusion-Pinot supports two modes for discovering tables:

### Filesystem Mode (Default)

Discovers tables by scanning local directories for `*_OFFLINE` and `*_REALTIME` patterns. Simple and requires no running Pinot instance.

```rust
use datafusion_pinot::PinotCatalog;

// Simple constructor
let catalog = PinotCatalog::new("/tmp/pinot/quickstart/PinotServerDataDir0")?;

// Or using builder
let catalog = PinotCatalog::builder()
    .filesystem("/tmp/pinot/quickstart/PinotServerDataDir0")
    .build()?;
```

**When to use:**
- Static table discovery from local directories
- Testing and development
- Tables don't change frequently
- Simpler setup without controller dependency

### Controller Mode (Requires `controller` feature)

Discovers tables dynamically via Pinot controller HTTP API, while reading segment data from local filesystem. Enables hybrid metadata discovery with local data access.

```toml
[dependencies]
datafusion-pinot = { version = "0.1", features = ["controller"] }
```

```rust
use datafusion_pinot::PinotCatalog;

let catalog = PinotCatalog::builder()
    .controller("http://localhost:9000")
    .with_segment_dir("/tmp/pinot/quickstart/PinotServerDataDir0")
    .build()?;
```

**When to use:**
- Dynamic table discovery from running Pinot cluster
- Tables may change over time
- Want to query the same tables the cluster serves
- Need centralized metadata management

**Docker setup for controller mode:**

```bash
# Start Pinot with volume-mounted data directory
docker run -d \
  --name pinot-quickstart \
  -p 9000:9000 \
  -v /tmp/pinot:/tmp/data \
  apachepinot/pinot:latest QuickStart \
  -type batch \
  -dataDir /tmp/data

# Controller API is available at http://localhost:9000 (HTTP only, no HTTPS)
# Segments are accessible at: /tmp/pinot/quickstart/PinotServerDataDir0
```

**How it works:**
1. Controller API (`GET /tables`) provides list of available tables
2. Controller API (`GET /segments/{table}?type=OFFLINE`) lists segment names
3. Segment data read from local filesystem (zero-copy, no download)
4. Best of both: dynamic discovery + local performance

**Note:** Controller mode uses HTTP only (not HTTPS). This is appropriate for:
- Localhost connections (`http://localhost:9000`)
- Private network deployments
- Development and testing environments

### Running Examples

```bash
# Filesystem mode - Query local segments with catalog
cargo run --package datafusion-pinot --example query_local

# Controller mode - Query using controller API (requires 'controller' feature)
PINOT_CONTROLLER_URL=http://localhost:9000 \
PINOT_SEGMENT_DIR=/tmp/pinot/quickstart/PinotServerDataDir0 \
cargo run --package datafusion-pinot --features controller --example query_controller

# Low-level - Read a single segment directly
cargo run --package datafusion-pinot --example read_segment
```

## Supported SQL Queries

The integration supports the full power of DataFusion's SQL engine:

```sql
-- Simple SELECT with projection
SELECT "playerID", "hits", "homeRuns"
FROM pinot.default."baseballStats"
LIMIT 10;

-- Aggregations
SELECT SUM("hits"), AVG("homeRuns"), MAX("strikeouts")
FROM pinot.default."baseballStats";

-- GROUP BY with ORDER BY
SELECT "teamID", COUNT(*) as games, SUM("hits") as total_hits
FROM pinot.default."baseballStats"
GROUP BY "teamID"
ORDER BY total_hits DESC
LIMIT 10;

-- Multiple tables
SELECT COUNT(*) FROM pinot.default."baseballStats";
SELECT COUNT(*) FROM pinot.default."dimBaseballTeams";
```

## Supported Data Types

| Pinot Type | Arrow Type | Dictionary | RAW | Compression |
|------------|------------|------------|-----|-------------|
| INT        | Int32      | ✅         | ⏳  | ✅ LZ4      |
| LONG       | Int64      | ✅         | ⏳  | ✅ LZ4      |
| FLOAT      | Float32    | ✅         | ⏳  | ✅ LZ4      |
| DOUBLE     | Float64    | ✅         | ⏳  | ✅ LZ4      |
| STRING     | Utf8       | ✅         | ✅  | ✅ LZ4      |
| BYTES      | Binary     | ⏳         | ⏳  | ⏳          |
| BOOLEAN    | Boolean    | ⏳         | ⏳  | ⏳          |
| TIMESTAMP  | Timestamp  | ❌         | ❌  | ❌          |

✅ Supported | ⏳ Planned | ❌ Not supported

## Architecture

### Project Structure

```
datafusion-pinot/
├── pinot-segment/              # Core segment reading library
│   ├── src/
│   │   ├── metadata.rs         # Metadata parser
│   │   ├── index_map.rs        # Index location parser
│   │   ├── segment_reader.rs   # High-level API
│   │   └── forward_index/
│   │       ├── dictionary.rs   # Dictionary reader
│   │       ├── fixed_bit.rs    # Bit-packed decoder
│   │       └── var_byte.rs     # RAW column reader (V4)
│   └── tests/
│       └── integration_tests.rs
│
├── datafusion-pinot/           # DataFusion integration
│   ├── src/
│   │   ├── catalog.rs          # Table discovery & builder
│   │   ├── controller.rs       # HTTP client (optional)
│   │   ├── metadata_provider.rs # Discovery abstraction
│   │   ├── table.rs            # TableProvider
│   │   ├── exec.rs             # ExecutionPlan
│   │   └── schema.rs           # Type mapping
│   ├── tests/
│   │   ├── query_tests.rs
│   │   ├── catalog_tests.rs
│   │   └── controller_client_tests.rs
│   └── examples/
│       ├── query_local.rs      # Filesystem mode
│       ├── query_controller.rs # Controller mode
│       └── read_segment.rs     # Low-level reading
```

### How It Works

1. **Segment Reading** (`pinot-segment` crate)
   - Parses Pinot v3 segment metadata
   - Reads dictionary and forward index data
   - Decodes bit-packed dictionary IDs
   - Handles LZ4-compressed RAW columns

2. **DataFusion Integration** (`datafusion-pinot` crate)
   - Implements `TableProvider` trait
   - Creates Arrow `RecordBatch` from segment data
   - Supports projection pushdown
   - Parallel execution (one partition per segment)

3. **Catalog Discovery**
   - **Filesystem mode**: Scans data directory for `*_OFFLINE` / `*_REALTIME` tables
   - **Controller mode**: HTTP calls to controller for table/segment lists
   - Auto-registers discovered tables
   - Supports fully qualified names: `pinot.default.tableName`

## Implementation Status

### ✅ Milestone 1: Dictionary-Encoded Columns (COMPLETE)
- Metadata and index map parsing
- Dictionary readers for all types
- Fixed-bit forward index decoder
- Bit-packing algorithm (big-endian)

### ✅ Milestone 2: RAW Encoding with LZ4 (COMPLETE)
- VarByteChunk V4 format support
- LZ4_LENGTH_PREFIXED decompression
- Metadata binary search for chunks
- Proper handling of last chunk boundaries

### ✅ Milestone 3: DataFusion Integration (COMPLETE)
- TableProvider and ExecutionPlan implementations
- Schema mapping (Pinot → Arrow)
- RecordBatch conversion
- SQL query execution

### ✅ Milestone 4: Multi-Segment & Catalog (COMPLETE)
- Multi-segment table support
- Automatic table discovery
- Segment-based partitioning
- Parallel segment reading

### ✅ Milestone 5: Validation & Documentation (COMPLETE)
- Comprehensive test suite (25 tests)
- Working examples
- Full documentation

## Performance

**Key Optimizations:**
- **Chunk-by-chunk decompression** for RAW columns - Process LZ4-compressed chunks sequentially, decompressing each chunk once and extracting all values, instead of decompressing per document
- **Column read caching** - Read each column into Arrow arrays once, then slice batches from pre-loaded data instead of re-reading for every batch
- **Batch processing** - Process data in 8,192-row chunks for efficient memory usage
- **Parallel execution** - One partition per segment enables concurrent segment processing
- **Efficient bit-packing** - Optimized decoder for dictionary-encoded columns

**Benchmark Results** (baseballStats: 97,889 rows, release build):

| Query Type | Time | SQL |
|------------|------|-----|
| COUNT(*) | 3.97ms | `SELECT COUNT(*) FROM baseballStats` |
| Full scan (dictionary) | 36.15ms | `SELECT "playerName", "hits", "homeRuns" FROM baseballStats` |
| Full scan (RAW) | 14.21ms | `SELECT "playerID", "hits", "homeRuns" FROM baseballStats` |
| Aggregation | 7.05ms | `SELECT SUM("hits"), AVG("homeRuns"), MAX("strikeouts") FROM baseballStats` |
| GROUP BY | 18.17ms | `SELECT "teamID", COUNT(*), SUM("hits") FROM baseballStats GROUP BY "teamID" ORDER BY SUM("hits") DESC LIMIT 10` |
| GROUP BY (RAW) | 22.97ms | `SELECT "playerID", AVG("homeRuns") FROM baseballStats GROUP BY "playerID" ORDER BY AVG("homeRuns") DESC LIMIT 10` |
| LIMIT 10 | 15.10ms | `SELECT "playerID", "hits", "homeRuns" FROM baseballStats LIMIT 10` |

Run benchmarks yourself:
```bash
cargo run --example benchmark --release
```

## Limitations

**Not Yet Supported:**
- Snappy / Zstandard compression (LZ4 only)
- Multi-value columns (arrays)
- Inverted indexes (filter pushdown uses full scan)
- Star-tree indexes
- V1/V2 segment formats (V3 only)
- Timestamp data type
- BYTES data type
- Filter pushdown to segment level

**Design Decisions:**
- Reads entire columns into memory (suitable for segments < 1GB)
- No lazy loading (loads all data for queried columns)
- No memory mapping (uses standard file I/O)

## Testing

```bash
# Run all tests
cargo test --workspace

# Run with output
cargo test --workspace -- --nocapture

# Test specific package
cargo test --package pinot-segment
cargo test --package datafusion-pinot

# Integration tests (requires Pinot data)
cargo test --test integration_tests -- --nocapture
```

**Test Coverage:**
- Unit tests for parsers, decoders, type mapping
- Integration tests with real Pinot segments
- End-to-end SQL query tests
- Multi-segment and catalog tests

## API Reference

### Reading a Single Segment

```rust
use pinot_segment::SegmentReader;

let reader = SegmentReader::open("/path/to/segment/v3")?;

// Read columns
let player_ids = reader.read_string_column("playerID")?;
let hits = reader.read_int_column("hits")?;
let home_runs = reader.read_int_column("homeRuns")?;

// Metadata
println!("Total docs: {}", reader.metadata().total_docs);
println!("Table: {}", reader.metadata().table_name);
```

### Using DataFusion with Catalog

```rust
use datafusion::prelude::*;
use datafusion_pinot::PinotCatalog;

let ctx = SessionContext::new();
let catalog = PinotCatalog::new("/data/pinot")?;
ctx.register_catalog("pinot", Arc::new(catalog));

// Query discovered tables
let df = ctx.sql("SELECT * FROM pinot.default.\"myTable\" LIMIT 100").await?;
let results = df.collect().await?;
```

### Using DataFusion with Single Table

```rust
use datafusion::prelude::*;
use datafusion_pinot::PinotTable;

let ctx = SessionContext::new();

// Open table (supports multi-segment)
let table = PinotTable::open_table("/data/pinot/myTable_OFFLINE")?;
ctx.register_table("myTable", Arc::new(table))?;

// Query
let df = ctx.sql("SELECT COUNT(*) FROM myTable").await?;
```

## Contributing

Contributions are welcome! This project follows these principles:

- **Incremental development** - Small, focused commits
- **Test-driven** - Tests before features
- **Zero unsafe code** - Rust safety guarantees
- **Clear errors** - Helpful error messages

## Roadmap

Future enhancements:

- [ ] Snappy/Zstandard compression support
- [ ] Filter pushdown using segment min/max stats
- [ ] Inverted index support for faster filtering
- [ ] Multi-value column support (arrays)
- [ ] Memory-mapped file I/O
- [ ] Streaming/chunked reading for large columns
- [ ] Star-tree index support
- [ ] Write support (create Pinot segments)
- [ ] REALTIME segment support

## References

- [Apache Pinot](https://pinot.apache.org/)
- [Apache DataFusion](https://datafusion.apache.org/)
- [Apache Arrow](https://arrow.apache.org/)
- [Pinot Segment Format](https://docs.pinot.apache.org/basics/components/table#segment)
- [Java Implementation](https://github.com/apache/pinot/tree/master/pinot-segment-local/src/main/java/org/apache/pinot/segment/local)

## License

Apache License 2.0 (same as Apache Pinot and Apache DataFusion)

## Citation

If you use this project in your research or products, please cite:

```
DataFusion-Pinot: A Rust implementation of Apache Pinot segment reading
with Apache DataFusion SQL engine integration.
```
