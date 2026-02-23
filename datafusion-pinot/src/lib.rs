//! DataFusion integration for Apache Pinot segments
//!
//! This library provides integration between Apache Pinot's segment format and
//! Apache DataFusion's query engine, enabling SQL queries over Pinot data with
//! zero network overhead.
//!
//! # Features
//!
//! - **Filesystem Mode**: Discover tables by scanning local directories
//! - **Controller Mode**: Discover tables via Pinot controller HTTP API (requires `controller` feature)
//! - **Zero-Copy**: Direct segment reading without Pinot server
//! - **Full SQL**: Execute DataFusion SQL queries on Pinot segments
//!
//! # Quick Start - Filesystem Mode
//!
//! ```rust,no_run
//! use datafusion::prelude::*;
//! use datafusion_pinot::PinotCatalog;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = SessionContext::new();
//!
//! // Discover tables from local directory
//! let catalog = PinotCatalog::new("/tmp/pinot/quickstart/PinotServerDataDir0")?;
//! ctx.register_catalog("pinot", Arc::new(catalog));
//!
//! // Execute SQL queries
//! let df = ctx.sql("SELECT COUNT(*) FROM pinot.default.baseballStats").await?;
//! let results = df.collect().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Controller Mode (requires `controller` feature)
//!
//! Enable the `controller` feature in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! datafusion-pinot = { version = "0.1", features = ["controller"] }
//! ```
//!
//! Then use the builder to configure controller mode:
//!
//! ```rust,no_run
//! # #[cfg(feature = "controller")]
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use datafusion::prelude::*;
//! use datafusion_pinot::PinotCatalog;
//! use std::sync::Arc;
//!
//! let ctx = SessionContext::new();
//!
//! // Discover tables from controller, read data from local filesystem
//! let catalog = PinotCatalog::builder()
//!     .controller("http://localhost:9000")
//!     .with_segment_dir("/tmp/pinot/quickstart/PinotServerDataDir0")
//!     .build()?;
//! ctx.register_catalog("pinot", Arc::new(catalog));
//!
//! let df = ctx.sql("SELECT COUNT(*) FROM pinot.default.baseballStats").await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Architecture
//!
//! The library consists of two main components:
//!
//! - **Metadata Providers**: Abstract table/segment discovery
//!   - `FileSystemMetadataProvider`: Scans local directories
//!   - `ControllerMetadataProvider`: Uses HTTP API (feature-gated)
//!
//! - **DataFusion Integration**: Implements TableProvider and ExecutionPlan
//!   - `PinotCatalog`: Catalog-level table discovery
//!   - `PinotTable`: TableProvider implementation
//!   - Schema mapping from Pinot to Arrow types

pub mod catalog;
pub mod error;
pub mod exec;
pub mod metadata_provider;
pub mod schema;
pub mod table;

#[cfg(feature = "controller")]
pub mod controller;

pub use catalog::{PinotCatalog, PinotCatalogBuilder, PinotCatalogSource};
pub use error::{Error, Result};
pub use metadata_provider::{FileSystemMetadataProvider, MetadataProvider};
pub use table::PinotTable;

#[cfg(feature = "controller")]
pub use controller::PinotControllerClient;

#[cfg(feature = "controller")]
pub use metadata_provider::ControllerMetadataProvider;
