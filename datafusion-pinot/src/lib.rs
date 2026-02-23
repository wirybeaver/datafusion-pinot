// DataFusion integration for Apache Pinot segments

pub mod catalog;
pub mod error;
pub mod exec;
pub mod schema;
pub mod table;

#[cfg(feature = "controller")]
pub mod controller;

pub use catalog::PinotCatalog;
pub use error::{Error, Result};
pub use table::PinotTable;

#[cfg(feature = "controller")]
pub use controller::PinotControllerClient;
