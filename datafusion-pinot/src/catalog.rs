use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::metadata_provider::{FileSystemMetadataProvider, MetadataProvider};
use crate::table::PinotTable;

#[cfg(feature = "controller")]
use crate::controller::PinotControllerClient;

#[cfg(feature = "controller")]
use crate::metadata_provider::ControllerMetadataProvider;

/// Catalog provider for Pinot tables
#[derive(Debug)]
pub struct PinotCatalog {
    schema_provider: Arc<PinotSchemaProvider>,
}

impl PinotCatalog {
    /// Create a new Pinot catalog from a data directory
    /// (e.g., /tmp/pinot/quickstart/PinotServerDataDir0)
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();

        if !data_dir.exists() {
            return Err(Error::Internal(format!(
                "Data directory does not exist: {}",
                data_dir.display()
            )));
        }

        let metadata_provider = Arc::new(FileSystemMetadataProvider::new(data_dir));
        let schema_provider = Arc::new(PinotSchemaProvider::new(metadata_provider));

        Ok(Self { schema_provider })
    }

    /// Create a builder for configuring a Pinot catalog
    ///
    /// The builder allows you to choose between filesystem and controller modes.
    ///
    /// # Example
    /// ```ignore
    /// // Filesystem mode
    /// let catalog = PinotCatalog::builder()
    ///     .filesystem("/tmp/pinot/quickstart/PinotServerDataDir0")
    ///     .build()?;
    ///
    /// // Controller mode (requires 'controller' feature)
    /// let catalog = PinotCatalog::builder()
    ///     .controller("http://localhost:9000")
    ///     .with_segment_dir("/tmp/pinot/quickstart/PinotServerDataDir0")
    ///     .build()?;
    /// ```
    pub fn builder() -> PinotCatalogBuilder {
        PinotCatalogBuilder::default()
    }

    /// Create a catalog from a metadata provider
    ///
    /// This is a low-level API for advanced use cases. Most users should use
    /// `new()` or `builder()` instead.
    pub fn from_provider(metadata_provider: Arc<dyn MetadataProvider>) -> Self {
        let schema_provider = Arc::new(PinotSchemaProvider::new(metadata_provider));
        Self { schema_provider }
    }
}

impl CatalogProvider for PinotCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["default".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "default" {
            Some(self.schema_provider.clone())
        } else {
            None
        }
    }
}

/// Builder for configuring a PinotCatalog
///
/// Supports two modes:
/// - **Filesystem mode**: Discovers tables by scanning local directories
/// - **Controller mode**: Discovers tables via HTTP API, reads data from local filesystem
///
/// # Example - Filesystem Mode
/// ```ignore
/// let catalog = PinotCatalog::builder()
///     .filesystem("/tmp/pinot/quickstart/PinotServerDataDir0")
///     .build()?;
/// ```
///
/// # Example - Controller Mode (requires 'controller' feature)
/// ```ignore
/// let catalog = PinotCatalog::builder()
///     .controller("http://localhost:9000")
///     .with_segment_dir("/tmp/pinot/quickstart/PinotServerDataDir0")
///     .build()?;
/// ```
#[derive(Default)]
pub struct PinotCatalogBuilder {
    source: Option<PinotCatalogSource>,
}

/// Configuration source for PinotCatalog
pub enum PinotCatalogSource {
    /// Filesystem-based discovery (scans local directories)
    FileSystem { data_dir: PathBuf },

    /// Controller-based discovery (HTTP API + local filesystem)
    #[cfg(feature = "controller")]
    Controller {
        base_url: String,
        segment_dir: PathBuf,
    },
}

impl PinotCatalogBuilder {
    /// Configure catalog to use filesystem-based discovery
    ///
    /// # Arguments
    /// * `data_dir` - Root directory containing table directories (e.g., `/tmp/pinot/quickstart/PinotServerDataDir0`)
    ///
    /// # Example
    /// ```ignore
    /// let catalog = PinotCatalog::builder()
    ///     .filesystem("/tmp/pinot/quickstart/PinotServerDataDir0")
    ///     .build()?;
    /// ```
    pub fn filesystem<P: Into<PathBuf>>(mut self, data_dir: P) -> Self {
        self.source = Some(PinotCatalogSource::FileSystem {
            data_dir: data_dir.into(),
        });
        self
    }

    /// Configure catalog to use controller-based discovery
    ///
    /// Requires the `controller` feature to be enabled.
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the Pinot controller (e.g., "http://localhost:9000")
    ///
    /// # Note
    /// You must also call `with_segment_dir()` to specify where segment data is located.
    ///
    /// # Example
    /// ```ignore
    /// let catalog = PinotCatalog::builder()
    ///     .controller("http://localhost:9000")
    ///     .with_segment_dir("/tmp/pinot/quickstart/PinotServerDataDir0")
    ///     .build()?;
    /// ```
    #[cfg(feature = "controller")]
    pub fn controller(mut self, base_url: impl Into<String>) -> Self {
        // If we already have a controller source, update the URL
        // Otherwise, create a new controller source with empty segment_dir
        match self.source {
            Some(PinotCatalogSource::Controller {
                ref segment_dir, ..
            }) => {
                self.source = Some(PinotCatalogSource::Controller {
                    base_url: base_url.into(),
                    segment_dir: segment_dir.clone(),
                });
            }
            _ => {
                self.source = Some(PinotCatalogSource::Controller {
                    base_url: base_url.into(),
                    segment_dir: PathBuf::new(),
                });
            }
        }
        self
    }

    /// Specify the local directory containing segment data
    ///
    /// This is required when using controller mode.
    ///
    /// # Arguments
    /// * `dir` - Directory containing segment data (e.g., `/tmp/pinot/quickstart/PinotServerDataDir0`)
    #[cfg(feature = "controller")]
    pub fn with_segment_dir<P: Into<PathBuf>>(mut self, dir: P) -> Self {
        // If we already have a controller source, update the segment_dir
        // Otherwise, create a new controller source with empty base_url
        match self.source {
            Some(PinotCatalogSource::Controller { ref base_url, .. }) => {
                self.source = Some(PinotCatalogSource::Controller {
                    base_url: base_url.clone(),
                    segment_dir: dir.into(),
                });
            }
            _ => {
                self.source = Some(PinotCatalogSource::Controller {
                    base_url: String::new(),
                    segment_dir: dir.into(),
                });
            }
        }
        self
    }

    /// Build the PinotCatalog
    ///
    /// # Errors
    /// Returns error if:
    /// - No source has been configured
    /// - Data directory doesn't exist (filesystem mode)
    /// - Controller URL or segment directory missing (controller mode)
    pub fn build(self) -> Result<PinotCatalog> {
        let source = self
            .source
            .ok_or_else(|| Error::Internal("No catalog source configured".to_string()))?;

        match source {
            PinotCatalogSource::FileSystem { data_dir } => {
                if !data_dir.exists() {
                    return Err(Error::Internal(format!(
                        "Data directory does not exist: {}",
                        data_dir.display()
                    )));
                }

                let metadata_provider = Arc::new(FileSystemMetadataProvider::new(data_dir));
                Ok(PinotCatalog::from_provider(metadata_provider))
            }

            #[cfg(feature = "controller")]
            PinotCatalogSource::Controller {
                base_url,
                segment_dir,
            } => {
                if base_url.is_empty() {
                    return Err(Error::Internal(
                        "Controller URL not specified".to_string(),
                    ));
                }

                if segment_dir.as_os_str().is_empty() {
                    return Err(Error::Internal(
                        "Segment directory not specified".to_string(),
                    ));
                }

                if !segment_dir.exists() {
                    return Err(Error::Internal(format!(
                        "Segment directory does not exist: {}",
                        segment_dir.display()
                    )));
                }

                let client = Arc::new(PinotControllerClient::new(base_url));
                let metadata_provider =
                    Arc::new(ControllerMetadataProvider::new(client, segment_dir));
                Ok(PinotCatalog::from_provider(metadata_provider))
            }
        }
    }
}

/// Schema provider for Pinot (discovers tables using MetadataProvider)
#[derive(Debug)]
pub struct PinotSchemaProvider {
    metadata_provider: Arc<dyn MetadataProvider>,
}

impl PinotSchemaProvider {
    pub fn new(metadata_provider: Arc<dyn MetadataProvider>) -> Self {
        Self { metadata_provider }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for PinotSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Convert async to sync - try to use existing runtime, or create one if needed
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle
                .block_on(self.metadata_provider.list_tables())
                .unwrap_or_default(),
            Err(_) => {
                // No runtime exists, create a temporary one
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(self.metadata_provider.list_tables())
                    .unwrap_or_default()
            }
        }
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // Get segment paths from metadata provider
        let segment_paths = match self.metadata_provider.get_segment_paths(name).await {
            Ok(paths) => paths,
            Err(_) => return Ok(None),
        };

        // Open table from segment paths
        match PinotTable::open_segments(&segment_paths, name) {
            Ok(table) => Ok(Some(Arc::new(table))),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        // Convert async to sync - try to use existing runtime, or create one if needed
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle.block_on(self.metadata_provider.table_exists(name)),
            Err(_) => {
                // No runtime exists, create a temporary one
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(self.metadata_provider.table_exists(name))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DATA_DIR: &str = "/tmp/pinot/quickstart/PinotServerDataDir0";

    #[test]
    fn test_catalog_creation() {
        if !Path::new(DATA_DIR).exists() {
            println!("Skipping test: data directory not found");
            return;
        }

        let catalog = PinotCatalog::new(DATA_DIR).expect("Failed to create catalog");

        let schema_names = catalog.schema_names();
        assert!(schema_names.contains(&"default".to_string()));
    }

    #[test]
    fn test_discover_tables() {
        if !Path::new(DATA_DIR).exists() {
            println!("Skipping test: data directory not found");
            return;
        }

        let catalog = PinotCatalog::new(DATA_DIR).expect("Failed to create catalog");
        let schema = catalog.schema("default").expect("Should have default schema");

        let table_names = schema.table_names();
        println!("Discovered tables: {:?}", table_names);

        assert!(!table_names.is_empty(), "Should discover at least one table");
        assert!(table_names.contains(&"baseballStats".to_string()));
    }

    #[tokio::test]
    async fn test_get_table() {
        if !Path::new(DATA_DIR).exists() {
            println!("Skipping test: data directory not found");
            return;
        }

        let catalog = PinotCatalog::new(DATA_DIR).expect("Failed to create catalog");
        let schema = catalog.schema("default").expect("Should have default schema");

        let table = schema
            .table("baseballStats")
            .await
            .expect("Failed to get table");

        assert!(table.is_some(), "Should find baseballStats table");

        let table = table.unwrap();
        let schema = table.schema();
        assert!(schema.fields().len() > 0, "Table should have fields");
    }

    #[test]
    fn test_builder_filesystem_mode() {
        if !Path::new(DATA_DIR).exists() {
            println!("Skipping test: data directory not found");
            return;
        }

        let catalog = PinotCatalog::builder()
            .filesystem(DATA_DIR)
            .build()
            .expect("Failed to build catalog");

        let schema_names = catalog.schema_names();
        assert!(schema_names.contains(&"default".to_string()));
    }

    #[test]
    fn test_builder_missing_source() {
        let result = PinotCatalog::builder().build();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("No catalog source configured"));
    }

    #[test]
    fn test_builder_nonexistent_directory() {
        let result = PinotCatalog::builder()
            .filesystem("/nonexistent/path/12345")
            .build();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not exist"));
    }

    #[cfg(feature = "controller")]
    #[test]
    fn test_builder_controller_mode() {
        if !Path::new(DATA_DIR).exists() {
            println!("Skipping test: data directory not found");
            return;
        }

        let result = PinotCatalog::builder()
            .controller("http://localhost:9000")
            .with_segment_dir(DATA_DIR)
            .build();

        // Should succeed in building (controller connection will be tested separately)
        assert!(result.is_ok());
    }

    #[cfg(feature = "controller")]
    #[test]
    fn test_builder_controller_missing_url() {
        let result = PinotCatalog::builder()
            .with_segment_dir(DATA_DIR)
            .build();

        assert!(result.is_err());
    }

    #[cfg(feature = "controller")]
    #[test]
    fn test_builder_controller_missing_segment_dir() {
        let result = PinotCatalog::builder()
            .controller("http://localhost:9000")
            .build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Segment directory not specified"));
    }
}
