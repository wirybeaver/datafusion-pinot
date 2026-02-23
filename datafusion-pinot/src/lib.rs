// DataFusion integration for Apache Pinot segments

pub mod catalog;
pub mod error;
pub mod exec;
pub mod metadata_provider;
pub mod schema;
pub mod table;

#[cfg(feature = "controller")]
pub mod controller;

pub use catalog::PinotCatalog;
pub use error::{Error, Result};
pub use metadata_provider::{FileSystemMetadataProvider, MetadataProvider};
pub use table::PinotTable;

#[cfg(feature = "controller")]
pub use controller::PinotControllerClient;

#[cfg(feature = "controller")]
pub use metadata_provider::ControllerMetadataProvider;
