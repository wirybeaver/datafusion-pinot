//! Pinot Controller HTTP API client
//!
//! This module provides a client for interacting with Apache Pinot's controller HTTP API
//! to discover table metadata and segment information.

use crate::error::{Error, Result};
use serde::Deserialize;
use std::collections::HashMap;

/// HTTP client for Pinot Controller API
///
/// # Example
/// ```no_run
/// use datafusion_pinot::controller::PinotControllerClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = PinotControllerClient::new("http://localhost:9000");
/// let tables = client.list_tables().await?;
/// println!("Available tables: {:?}", tables);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PinotControllerClient {
    base_url: String,
    client: reqwest::Client,
}

/// Response from /tables endpoint
#[derive(Debug, Deserialize)]
pub struct TablesResponse {
    pub tables: Vec<String>,
}

/// Response from /segments/{tableName} endpoint
///
/// The Pinot controller returns segments grouped by type:
/// ```json
/// [
///   {"OFFLINE": ["segment1", "segment2"]},
///   {"REALTIME": ["segment3"]}
/// ]
/// ```
#[derive(Debug, Deserialize)]
pub struct SegmentListResponse(Vec<HashMap<String, Vec<String>>>);

impl PinotControllerClient {
    /// Create a new controller client
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the Pinot controller (e.g., "http://localhost:9000")
    ///
    /// # Example
    /// ```
    /// use datafusion_pinot::controller::PinotControllerClient;
    ///
    /// let client = PinotControllerClient::new("http://localhost:9000");
    /// ```
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            client: reqwest::Client::new(),
        }
    }

    /// List all tables from the controller
    ///
    /// Makes a GET request to `/tables` endpoint.
    ///
    /// # Errors
    /// Returns error if:
    /// - HTTP request fails
    /// - Response cannot be parsed as JSON
    /// - Controller returns non-200 status
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let url = format!("{}/tables", self.base_url);
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(Error::HttpClient(format!(
                "Controller returned status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let tables_response: TablesResponse = response.json().await?;
        Ok(tables_response.tables)
    }

    /// List segments for a specific table and type
    ///
    /// Makes a GET request to `/segments/{tableName}?type={tableType}` endpoint.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table (without type suffix)
    /// * `table_type` - Type of segments to retrieve ("OFFLINE" or "REALTIME")
    ///
    /// # Errors
    /// Returns error if:
    /// - HTTP request fails
    /// - Response cannot be parsed as JSON
    /// - Controller returns non-200 status
    /// - Requested table type not found in response
    ///
    /// # Example
    /// ```no_run
    /// # use datafusion_pinot::controller::PinotControllerClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = PinotControllerClient::new("http://localhost:9000");
    /// let segments = client.list_segments("baseballStats", "OFFLINE").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_segments(&self, table_name: &str, table_type: &str) -> Result<Vec<String>> {
        let url = format!(
            "{}/segments/{}?type={}",
            self.base_url, table_name, table_type
        );
        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(Error::HttpClient(format!(
                "Controller returned status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        let segment_list: SegmentListResponse = response.json().await?;

        // Extract segments for the requested type
        // Response format: [{"OFFLINE": [...]}, {"REALTIME": [...]}]
        for map in segment_list.0 {
            if let Some(segments) = map.get(table_type) {
                return Ok(segments.clone());
            }
        }

        // If table type not found, return empty list
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = PinotControllerClient::new("http://localhost:9000");
        assert_eq!(client.base_url, "http://localhost:9000");
    }

    #[test]
    fn test_deserialize_tables_response() {
        let json = r#"{"tables": ["table1", "table2"]}"#;
        let response: TablesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.tables, vec!["table1", "table2"]);
    }

    #[test]
    fn test_deserialize_segment_list_response() {
        let json = r#"[{"OFFLINE": ["seg1", "seg2"]}, {"REALTIME": ["seg3"]}]"#;
        let response: SegmentListResponse = serde_json::from_str(json).unwrap();

        // Verify we can extract OFFLINE segments
        let offline_segments = response.0.iter()
            .find_map(|map| map.get("OFFLINE"))
            .unwrap();
        assert_eq!(offline_segments, &vec!["seg1", "seg2"]);

        // Verify we can extract REALTIME segments
        let realtime_segments = response.0.iter()
            .find_map(|map| map.get("REALTIME"))
            .unwrap();
        assert_eq!(realtime_segments, &vec!["seg3"]);
    }
}
