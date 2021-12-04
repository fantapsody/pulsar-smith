use std::fmt::{Display, Formatter};
use oauth2::url::ParseError;

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    SerDe(String),
    Custom(String),
}

impl From<ParseError> for Error {
    fn from(e: ParseError) -> Self {
        Error::SerDe(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::SerDe(e.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IO(e)
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::Custom(s)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IO(e) => write!(f, "{}", e),
            Error::SerDe(str) => write!(f, "{}", str),
            Error::Custom(str) => write!(f, "{}", str),
        }
    }
}

impl std::error::Error for Error {

}
