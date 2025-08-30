use serde::{Deserialize, Serialize};
use crate::types::storage_types::ForeignKeyConstraint;

use super::storage_types::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CatColumnType {
    Int32, // integer column
    Text,  // string column
    Bool,  // boolean column
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,            // column name
    #[serde(rename = "type")]
    pub ty: CatColumnType,       // column type
    pub not_null: bool,          // whether column is NOT NULL
    pub default: Option<Value>   // optional default value
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub oid: u32,                // unique table ID
    pub file: String,            // file path for table storage
    pub columns: Vec<ColumnMeta>,// schema definition
    pub next_rowid: u64,         // auto-increment row ID counter
    pub primary_key: Option<String>,        // optional primary key
    pub foreign_keys: Vec<ForeignKeyConstraint>, // list of foreign keys
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub version: u32,                  // catalog format version
    pub page_size: u32,                // page size used by DB
    pub next_table_oid: u32,           // counter for new table IDs
    pub tables: BTreeMap<String, TableMeta>, // map table name → metadata
}

impl Catalog {
    pub fn empty(page_size: u32) -> Self {
        // create empty catalog with no tables
        Self {
            version: 1,
            page_size,
            next_table_oid: 1,
            tables: BTreeMap::new(),
        }
    }

    pub fn has_table(&self, name: &str) -> bool {
        // check if a table with this name exists in the catalog
        self.tables.contains_key(name)
    }
}
