use crate::errors::catalog_error::CatalogError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("database error: {0}")]
    Database(String),

    #[error("parser error: {0}")]
    Parser(String),

    #[error("unknown error: {0}")]
    Other(String),
}

impl From<String> for EngineError {
    fn from(err: String) -> Self {
        EngineError::Other(err)
    }
}
