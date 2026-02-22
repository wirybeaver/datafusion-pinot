use std::fmt;

#[derive(Debug)]
pub enum Error {
    PinotSegment(pinot_segment::Error),
    DataFusion(String),
    Arrow(String),
    Internal(String),
    UnsupportedFeature(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::PinotSegment(e) => write!(f, "Pinot segment error: {}", e),
            Error::DataFusion(msg) => write!(f, "DataFusion error: {}", msg),
            Error::Arrow(msg) => write!(f, "Arrow error: {}", msg),
            Error::Internal(msg) => write!(f, "Internal error: {}", msg),
            Error::UnsupportedFeature(msg) => write!(f, "Unsupported feature: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

impl From<pinot_segment::Error> for Error {
    fn from(err: pinot_segment::Error) -> Self {
        Error::PinotSegment(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
