use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use std::any::Any;
use std::path::Path;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::metadata_provider::{FileSystemMetadataProvider, MetadataProvider};
use crate::table::PinotTable;

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
}
