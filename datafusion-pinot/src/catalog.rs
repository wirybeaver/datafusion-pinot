use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use std::any::Any;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::table::PinotTable;

/// Catalog provider for Pinot tables
#[derive(Debug)]
pub struct PinotCatalog {
    data_dir: PathBuf,
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

        let schema_provider = Arc::new(PinotSchemaProvider::new(data_dir.clone()));

        Ok(Self {
            data_dir,
            schema_provider,
        })
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

/// Schema provider for Pinot (discovers tables in data directory)
#[derive(Debug)]
pub struct PinotSchemaProvider {
    data_dir: PathBuf,
}

impl PinotSchemaProvider {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// Scan data directory for Pinot tables
    fn discover_tables(&self) -> Result<Vec<String>> {
        let entries = fs::read_dir(&self.data_dir)
            .map_err(|e| Error::Internal(format!("Failed to read data directory: {}", e)))?;

        let mut table_names = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|e| Error::Internal(e.to_string()))?;
            let path = entry.path();

            // Look for directories ending with _OFFLINE or _REALTIME
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.ends_with("_OFFLINE") {
                    // Strip _OFFLINE suffix to get table name
                    let table_name = name.strip_suffix("_OFFLINE").unwrap().to_string();
                    table_names.push(table_name);
                } else if name.ends_with("_REALTIME") {
                    // Strip _REALTIME suffix
                    let table_name = name.strip_suffix("_REALTIME").unwrap().to_string();
                    if !table_names.contains(&table_name) {
                        table_names.push(table_name);
                    }
                }
            }
        }

        table_names.sort();
        Ok(table_names)
    }
}

#[async_trait::async_trait]
impl SchemaProvider for PinotSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.discover_tables().unwrap_or_default()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // Try to open OFFLINE table first, then REALTIME
        let offline_dir = self.data_dir.join(format!("{}_OFFLINE", name));
        let realtime_dir = self.data_dir.join(format!("{}_REALTIME", name));

        let table_dir = if offline_dir.exists() {
            offline_dir
        } else if realtime_dir.exists() {
            realtime_dir
        } else {
            return Ok(None);
        };

        match PinotTable::open_table(&table_dir) {
            Ok(table) => Ok(Some(Arc::new(table))),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        let offline_dir = self.data_dir.join(format!("{}_OFFLINE", name));
        let realtime_dir = self.data_dir.join(format!("{}_REALTIME", name));
        offline_dir.exists() || realtime_dir.exists()
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
