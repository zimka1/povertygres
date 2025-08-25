use thiserror::Error;

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("invalid catalog: {0}")]
    Invalid(String),
    #[error("table exists: {0}")]
    TableExists(String),
    #[error("table not found: {0}")]
    TableNotFound(String),
}
