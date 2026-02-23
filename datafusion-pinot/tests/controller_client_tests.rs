//! Unit tests for PinotControllerClient using HTTP mocks

#[cfg(feature = "controller")]
mod controller_tests {
    use datafusion_pinot::controller::PinotControllerClient;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_list_tables_success() {
        // Start a background HTTP server on a random local port
        let mock_server = MockServer::start().await;

        // Arrange
        let response_body = r#"{"tables": ["baseballStats", "airlineStats"]}"#;
        Mock::given(method("GET"))
            .and(path("/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_string(response_body))
            .mount(&mock_server)
            .await;

        // Act
        let client = PinotControllerClient::new(mock_server.uri());
        let tables = client.list_tables().await.unwrap();

        // Assert
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0], "baseballStats");
        assert_eq!(tables[1], "airlineStats");
    }

    #[tokio::test]
    async fn test_list_tables_empty() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"tables": []}"#))
            .mount(&mock_server)
            .await;

        let client = PinotControllerClient::new(mock_server.uri());
        let tables = client.list_tables().await.unwrap();

        assert_eq!(tables.len(), 0);
    }

    #[tokio::test]
    async fn test_list_tables_404() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/tables"))
            .respond_with(ResponseTemplate::new(404).set_body_string("Not Found"))
            .mount(&mock_server)
            .await;

        let client = PinotControllerClient::new(mock_server.uri());
        let result = client.list_tables().await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("404"));
    }

    #[tokio::test]
    async fn test_list_tables_500() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/tables"))
            .respond_with(
                ResponseTemplate::new(500).set_body_string("Internal Server Error"),
            )
            .mount(&mock_server)
            .await;

        let client = PinotControllerClient::new(mock_server.uri());
        let result = client.list_tables().await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("500"));
    }

    #[tokio::test]
    async fn test_list_tables_invalid_json() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/tables"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not valid json"))
            .mount(&mock_server)
            .await;

        let client = PinotControllerClient::new(mock_server.uri());
        let result = client.list_tables().await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        // The error comes from reqwest's json() method, which may be wrapped
        // Just verify it's an error - the specific message format may vary
        assert!(!err_msg.is_empty());
    }

    #[tokio::test]
    async fn test_list_segments_offline_success() {
        let mock_server = MockServer::start().await;

        let response_body = r#"[
            {"OFFLINE": ["baseballStats_OFFLINE_0", "baseballStats_OFFLINE_1"]},
            {"REALTIME": []}
        ]"#;

        Mock::given(method("GET"))
            .and(path("/segments/baseballStats"))
            .and(query_param("type", "OFFLINE"))
            .respond_with(ResponseTemplate::new(200).set_body_string(response_body))
            .mount(&mock_server)
            .await;

        let client = PinotControllerClient::new(mock_server.uri());
        let segments = client.list_segments("baseballStats", "OFFLINE").await.unwrap();

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0], "baseballStats_OFFLINE_0");
        assert_eq!(segments[1], "baseballStats_OFFLINE_1");
    }

    #[tokio::test]
    async fn test_list_segments_realtime_success() {
        let mock_server = MockServer::start().await;

        let response_body = r#"[
            {"OFFLINE": []},
            {"REALTIME": ["baseballStats_REALTIME_0"]}
        ]"#;

        Mock::given(method("GET"))
            .and(path("/segments/baseballStats"))
            .and(query_param("type", "REALTIME"))
            .respond_with(ResponseTemplate::new(200).set_body_string(response_body))
            .mount(&mock_server)
            .await;

        let client = PinotControllerClient::new(mock_server.uri());
        let segments = client
            .list_segments("baseballStats", "REALTIME")
            .await
            .unwrap();

        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0], "baseballStats_REALTIME_0");
    }

    #[tokio::test]
    async fn test_list_segments_not_found() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/segments/nonexistent"))
            .and(query_param("type", "OFFLINE"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let client = PinotControllerClient::new(mock_server.uri());
        let result = client.list_segments("nonexistent", "OFFLINE").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_segments_empty() {
        let mock_server = MockServer::start().await;

        let response_body = r#"[{"OFFLINE": []}, {"REALTIME": []}]"#;

        Mock::given(method("GET"))
            .and(path("/segments/emptyTable"))
            .and(query_param("type", "OFFLINE"))
            .respond_with(ResponseTemplate::new(200).set_body_string(response_body))
            .mount(&mock_server)
            .await;

        let client = PinotControllerClient::new(mock_server.uri());
        let segments = client.list_segments("emptyTable", "OFFLINE").await.unwrap();

        assert_eq!(segments.len(), 0);
    }

    #[tokio::test]
    async fn test_list_segments_type_not_in_response() {
        let mock_server = MockServer::start().await;

        // Response only contains OFFLINE, but we request REALTIME
        let response_body = r#"[{"OFFLINE": ["seg1"]}]"#;

        Mock::given(method("GET"))
            .and(path("/segments/table"))
            .and(query_param("type", "REALTIME"))
            .respond_with(ResponseTemplate::new(200).set_body_string(response_body))
            .mount(&mock_server)
            .await;

        let client = PinotControllerClient::new(mock_server.uri());
        let segments = client.list_segments("table", "REALTIME").await.unwrap();

        // Should return empty list when type not found
        assert_eq!(segments.len(), 0);
    }
}
