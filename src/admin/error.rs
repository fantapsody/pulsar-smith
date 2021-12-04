use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum Error {
    SerDe(String),
    Custom(String)
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Custom(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::SerDe(e.to_string())
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
            Error::SerDe(str) => write!(f, "serde {}", str),
            Error::Custom(str) => write!(f, "{}", str),
        }
    }
}

impl std::error::Error for Error {

}
