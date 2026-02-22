use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Parse(String),
    InvalidFormat(String),
    UnsupportedFeature(String),
    ColumnNotFound(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::Parse(msg) => write!(f, "Parse error: {}", msg),
            Error::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            Error::UnsupportedFeature(msg) => write!(f, "Unsupported feature: {}", msg),
            Error::ColumnNotFound(name) => write!(f, "Column not found: {}", name),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
