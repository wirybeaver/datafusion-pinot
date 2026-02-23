//! Metadata provider abstraction for Pinot table and segment discovery
//!
//! This module defines the `MetadataProvider` trait that abstracts the source
//! of metadata (table lists, segment locations) from the catalog implementation.
//!
//! Two implementations are provided:
//! - `FileSystemMetadataProvider`: Discovers tables by scanning local directories
//! - `ControllerMetadataProvider`: Discovers tables via HTTP calls to Pinot controller

use crate::error::Result;
use async_trait::async_trait;
use std::path::PathBuf;

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
