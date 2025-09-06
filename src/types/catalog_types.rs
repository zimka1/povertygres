use crate::types::{storage_types::ForeignKeyConstraint, transaction_types::TxStatus};
use serde::{Deserialize, Serialize};

use super::storage_types::Value;
use std::collections::{BTreeMap, HashMap};

/// Supported column types in catalog metadata
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CatColumnType {
    Int32, // integer column
    Text,  // string column
    Bool,  // boolean column
}

/// Metadata describing a single column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String, // column name
    #[serde(rename = "type")]
    pub ty: CatColumnType, // column type
    pub not_null: bool, // whether column is NOT NULL
    pub default: Option<Value>, // optional default value
}

/// Metadata describing a table definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub oid: u32,                                // unique table ID
    pub file: String,                            // file path for table storage
    pub columns: Vec<ColumnMeta>,                // schema definition
    pub next_rowid: u64,                         // auto-increment row ID counter
    pub primary_key: Option<String>,             // optional primary key
    pub foreign_keys: Vec<ForeignKeyConstraint>, // list of foreign keys
}

/// Global catalog structure, persisted on disk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub version: u32,                        // catalog format version
    pub page_size: u32,                      // page size used by DB
    pub next_table_oid: u32,                 // counter for new table IDs
    pub next_xid: u32,                       // counter for new transaction IDs
    pub transactions: HashMap<u32, TxStatus>,// transaction status map (xid -> status)
    pub indexes: HashMap<String, IndexMeta>, // defined indexes
    pub tables: BTreeMap<String, TableMeta>, // map table name → metadata
}

/// Metadata describing an index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMeta {
    pub name: String,           // index name
    pub table: String,          // table name this index belongs to
    pub columns: Vec<String>,   // indexed columns
}

impl Catalog {
    /// Create a new empty catalog with no tables or indexes
    pub fn empty(page_size: u32) -> Self {
        Self {
            version: 1,
            page_size,
            next_table_oid: 1,
            next_xid: 1,
            tables: BTreeMap::new(),
            indexes: HashMap::new(),
            transactions: HashMap::new(),
        }
    }

    /// Check if a table with the given name exists in the catalog
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
