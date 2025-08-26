use serde::{Deserialize, Serialize};
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub oid: u32,                // unique table ID
    pub file: String,            // file path for table storage
    pub columns: Vec<ColumnMeta>,// schema definition
    pub next_rowid: u64,         // auto-increment row ID counter
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
        // create empty catalog
        Self {
            version: 1,
            page_size,
            next_table_oid: 1,
            tables: BTreeMap::new(),
        }
    }

    pub fn has_table(&self, name: &str) -> bool {
        // check if table exists
        self.tables.contains_key(name)
    }
}
