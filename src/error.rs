use std::fmt::{Display, Formatter};

use pulsar::error::ConsumerError;

#[derive(Debug)]
pub enum Error {
    Auth(crate::auth::error::Error),
    IO(std::io::Error),
    PulsarAdmin(crate::admin::error::Error),
    Pulsar(String),
    SerDe(String),
    Custom(String),
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Error::Custom(e.to_string())
    }
}

impl From<pulsar::Error> for Error {
    fn from(e: pulsar::Error) -> Self {
        Error::Pulsar(e.to_string())
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Custom(e.to_string())
    }
}

impl From<crate::auth::error::Error> for Error {
    fn from(e: crate::auth::error::Error) -> Self {
        Error::Auth(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IO(e)
    }
}

impl From<crate::admin::error::Error> for Error {
    fn from(e: crate::admin::error::Error) -> Self {
        Error::PulsarAdmin(e)
    }
}

impl From<&dyn std::error::Error> for Error {
    fn from(e: &dyn std::error::Error) -> Self {
        Error::Custom(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::SerDe(e.to_string())
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(e: serde_yaml::Error) -> Self {
        Error::SerDe(e.to_string())
    }
}

impl From<ConsumerError> for Error {
    fn from(e: ConsumerError) -> Self {
        Error::Pulsar(e.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IO(e) => write!(f, "io error [{}]", e),
            Error::Auth(e) => write!(f, "pulsar admin error [{}]", e),
            Error::PulsarAdmin(e) => write!(f, "pulsar admin error [{}]", e),
            Error::Pulsar(str) => write!(f, "pulsar error {}", str),
            Error::SerDe(str) => write!(f, "serde error {}", str),
            Error::Custom(str) => write!(f, "{}", str),
        }
    }
}

impl std::error::Error for Error {}
