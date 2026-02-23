//! Metadata provider abstraction for Pinot table and segment discovery
//!
//! This module defines the `MetadataProvider` trait that abstracts the source
//! of metadata (table lists, segment locations) from the catalog implementation.
//!
//! Two implementations are provided:
//! - `FileSystemMetadataProvider`: Discovers tables by scanning local directories
//! - `ControllerMetadataProvider`: Discovers tables via HTTP calls to Pinot controller

use crate::error::{Error, Result};
use async_trait::async_trait;
use std::fs;
use std::path::{Path, PathBuf};

/// Trait for discovering Pinot table metadata and segment locations
///
/// This trait abstracts whether metadata comes from:
/// - Local filesystem scanning (filesystem-only mode)
/// - Pinot controller HTTP API (controller mode)
///
/// # Example
/// ```ignore
/// use datafusion_pinot::metadata_provider::MetadataProvider;
///
/// async fn discover_tables(provider: &dyn MetadataProvider) {
///     let tables = provider.list_tables().await.unwrap();
///     for table in tables {
///         println!("Table: {}", table);
///         let segments = provider.get_segment_paths(&table).await.unwrap();
///         println!("  Segments: {}", segments.len());
///     }
/// }
/// ```
#[async_trait]
pub trait MetadataProvider: Send + Sync {
    /// List all available table names
    ///
    /// # Returns
    /// Vector of table names without type suffixes (e.g., "baseballStats" not "baseballStats_OFFLINE")
    ///
    /// # Errors
    /// Returns error if metadata source is unavailable or cannot be read
    async fn list_tables(&self) -> Result<Vec<String>>;

    /// Check if a table exists
    ///
    /// # Arguments
    /// * `name` - Table name to check (without type suffix)
    ///
    /// # Returns
    /// `true` if table exists, `false` otherwise
    async fn table_exists(&self, name: &str) -> bool {
        self.list_tables()
            .await
            .map(|tables| tables.iter().any(|t| t == name))
            .unwrap_or(false)
    }

    /// Get filesystem paths to all segments for a table
    ///
    /// Returns paths to segment directories (typically pointing to the `v3/` subdirectory
    /// containing the segment metadata and data files).
    ///
    /// # Arguments
    /// * `table_name` - Name of the table (without type suffix)
    ///
    /// # Returns
    /// Vector of filesystem paths to segment directories
    ///
    /// # Errors
    /// Returns error if:
    /// - Table does not exist
    /// - Segments cannot be located
    /// - Filesystem is not accessible
    ///
    /// # Example
    /// ```ignore
    /// let paths = provider.get_segment_paths("baseballStats").await?;
    /// // Returns: ["/tmp/pinot/.../baseballStats_OFFLINE/seg1/v3", ...]
    /// ```
    async fn get_segment_paths(&self, table_name: &str) -> Result<Vec<PathBuf>>;
}

/// Filesystem-based metadata provider
///
/// Discovers tables by scanning a local directory for `*_OFFLINE` and `*_REALTIME`
/// subdirectories. This is the default implementation that works with Pinot's
/// standard directory layout.
///
/// # Example
/// ```ignore
/// use datafusion_pinot::metadata_provider::FileSystemMetadataProvider;
///
/// let provider = FileSystemMetadataProvider::new("/tmp/pinot/quickstart/PinotServerDataDir0");
/// let tables = provider.list_tables().await?;
/// ```
#[derive(Debug, Clone)]
pub struct FileSystemMetadataProvider {
    data_dir: PathBuf,
}

impl FileSystemMetadataProvider {
    /// Create a new filesystem metadata provider
    ///
    /// # Arguments
    /// * `data_dir` - Root directory containing table directories (e.g., `/tmp/pinot/quickstart/PinotServerDataDir0`)
    pub fn new<P: Into<PathBuf>>(data_dir: P) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    /// Get the data directory path
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

#[async_trait]
impl MetadataProvider for FileSystemMetadataProvider {
    async fn list_tables(&self) -> Result<Vec<String>> {
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

    async fn table_exists(&self, name: &str) -> bool {
        let offline_dir = self.data_dir.join(format!("{}_OFFLINE", name));
        let realtime_dir = self.data_dir.join(format!("{}_REALTIME", name));
        offline_dir.exists() || realtime_dir.exists()
    }

    async fn get_segment_paths(&self, table_name: &str) -> Result<Vec<PathBuf>> {
        // Try OFFLINE first, then REALTIME
        let offline_dir = self.data_dir.join(format!("{}_OFFLINE", table_name));
        let realtime_dir = self.data_dir.join(format!("{}_REALTIME", table_name));

        let table_dir = if offline_dir.exists() {
            offline_dir
        } else if realtime_dir.exists() {
            realtime_dir
        } else {
            return Err(Error::Internal(format!(
                "Table '{}' not found in {}",
                table_name,
                self.data_dir.display()
            )));
        };

        // Read all segment directories
        let entries = fs::read_dir(&table_dir)
            .map_err(|e| Error::Internal(format!("Failed to read table directory: {}", e)))?;

        let mut segment_paths = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| Error::Internal(e.to_string()))?;
            let path = entry.path();

            // Skip non-directories and temporary directories
            if !path.is_dir() || path.file_name().unwrap().to_str().unwrap() == "tmp" {
                continue;
            }

            // Check if it's a valid segment (has v3 subdirectory)
            let v3_path = path.join("v3");
            if v3_path.exists() && v3_path.is_dir() {
                segment_paths.push(v3_path);
            }
        }

        if segment_paths.is_empty() {
            return Err(Error::Internal(format!(
                "No valid segments found in {}",
                table_dir.display()
            )));
        }

        // Sort for consistent ordering
        segment_paths.sort();
        Ok(segment_paths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_filesystem_provider_list_tables() {
        // This test requires a running Pinot instance with data
        let data_dir = "/tmp/pinot/quickstart/PinotServerDataDir0";
        if !Path::new(data_dir).exists() {
            println!("Skipping test: data directory not found");
            return;
        }

        let provider = FileSystemMetadataProvider::new(data_dir);
        let tables = provider.list_tables().await.unwrap();

        assert!(!tables.is_empty(), "Should discover at least one table");
        // Table names should not contain _OFFLINE or _REALTIME suffixes
        for table in &tables {
            assert!(!table.ends_with("_OFFLINE"));
            assert!(!table.ends_with("_REALTIME"));
        }
    }

    #[tokio::test]
    async fn test_filesystem_provider_table_exists() {
        let data_dir = "/tmp/pinot/quickstart/PinotServerDataDir0";
        if !Path::new(data_dir).exists() {
            println!("Skipping test: data directory not found");
            return;
        }

        let provider = FileSystemMetadataProvider::new(data_dir);

        // Test with a known table
        assert!(provider.table_exists("baseballStats").await);

        // Test with non-existent table
        assert!(!provider.table_exists("nonexistent_table_12345").await);
    }

    #[tokio::test]
    async fn test_filesystem_provider_get_segment_paths() {
        let data_dir = "/tmp/pinot/quickstart/PinotServerDataDir0";
        if !Path::new(data_dir).exists() {
            println!("Skipping test: data directory not found");
            return;
        }

        let provider = FileSystemMetadataProvider::new(data_dir);
        let paths = provider.get_segment_paths("baseballStats").await.unwrap();

        assert!(!paths.is_empty(), "Should find at least one segment");

        // All paths should end with v3
        for path in &paths {
            assert!(path.ends_with("v3"), "Path should end with v3: {:?}", path);
            assert!(path.exists(), "Segment path should exist: {:?}", path);
        }
    }
}
