use std::fmt;

#[derive(Debug)]
pub enum Error {
    PinotSegment(pinot_segment::Error),
    DataFusion(String),
    Arrow(String),
    Internal(String),
    UnsupportedFeature(String),

    // Controller-specific errors (feature-gated)
    #[cfg(feature = "controller")]
    HttpClient(String),

    #[cfg(feature = "controller")]
    JsonParse(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::PinotSegment(e) => write!(f, "Pinot segment error: {}", e),
            Error::DataFusion(msg) => write!(f, "DataFusion error: {}", msg),
            Error::Arrow(msg) => write!(f, "Arrow error: {}", msg),
            Error::Internal(msg) => write!(f, "Internal error: {}", msg),
            Error::UnsupportedFeature(msg) => write!(f, "Unsupported feature: {}", msg),

            #[cfg(feature = "controller")]
            Error::HttpClient(msg) => write!(f, "HTTP client error: {}", msg),

            #[cfg(feature = "controller")]
            Error::JsonParse(msg) => write!(f, "JSON parse error: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

impl From<pinot_segment::Error> for Error {
    fn from(err: pinot_segment::Error) -> Self {
        Error::PinotSegment(err)
    }
}

#[cfg(feature = "controller")]
impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::HttpClient(err.to_string())
    }
}

#[cfg(feature = "controller")]
impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::JsonParse(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
