use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CatColumnType {
    Int32,
    Text,
    Bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMeta {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: CatColumnType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub oid: u32,
    pub file: String,
    pub columns: Vec<ColumnMeta>,
    pub next_rowid: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub version: u32,
    pub page_size: u32,
    pub next_table_oid: u32,
    pub tables: BTreeMap<String, TableMeta>,
}

impl Catalog {
    pub fn empty(page_size: u32) -> Self {
        Self {
            version: 1,
            page_size,
            next_table_oid: 1,
            tables: BTreeMap::new(),
        }
    }
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
