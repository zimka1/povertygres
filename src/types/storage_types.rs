use super::catalog_types::CatColumnType;
use crate::storage::heap_file::HeapFile;
use crate::types::b_tree::BTreeIndex;
use crate::types::transaction_types::TransactionManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

pub struct Database {
    // Stores tables by their name
    pub tables: HashMap<String, Table>,
    pub indexes: HashMap<String, BTreeIndex>,
    pub transaction_manager: TransactionManager,
}

impl Database {
    // Creates a new empty database
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            transaction_manager: TransactionManager::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    // Table name
    pub name: String,
    // List of columns in the table
    pub columns: Vec<Column>,
    // Stored rows in the table
    // Low-level heap file storage for rows
    pub heap: HeapFile,
    // Optional primary key column name
    pub primary_key: Option<String>,
    // List of foreign key constraints defined on this table
    pub foreign_keys: Vec<ForeignKeyConstraint>,
}

#[derive(Debug, Clone)]
pub struct Column {
    // Column name
    pub name: String,
    // Data type of the column
    pub column_type: ColumnType,
    // Whether the column is NOT NULL
    pub not_null: bool,
    // Optional default value for this column
    pub default: Option<Value>,
}

// Supported data types for columns
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int,
    Text,
    Bool,
}

impl From<CatColumnType> for ColumnType {
    fn from(c: CatColumnType) -> Self {
        match c {
            CatColumnType::Int32 => ColumnType::Int,
            CatColumnType::Text => ColumnType::Text,
            CatColumnType::Bool => ColumnType::Bool,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    // Ordered list of values (same order as columns)
    pub values: Vec<Value>,
}

// Represents a single cell value
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Null, // Equivalent to SQL NULL
}

// Display implementation for pretty-printing values
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Int(i) => write!(f, "{}", i),
            Value::Text(s) => write!(f, "{}", s),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Null => write!(f, "NULL"),
        }
    }
}

/// Enumerates the possible data types for values in the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Int,
    Text,
    Bool,
    Null,
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ValueType::Int => "INT",
            ValueType::Text => "TEXT",
            ValueType::Bool => "BOOL",
            ValueType::Null => "NULL",
        })
    }
}

impl Value {
    /// Returns the `ValueType` corresponding to this `Value` variant.
    pub fn vtype(&self) -> ValueType {
        match self {
            Value::Int(_) => ValueType::Int,
            Value::Text(_) => ValueType::Text,
            Value::Bool(_) => ValueType::Bool,
            Value::Null => ValueType::Null,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    // Local columns in the current table that form the foreign key
    pub local_columns: Vec<String>,
    // Name of the referenced (parent) table
    pub referenced_table: String,
    // Columns in the referenced table that are targeted
    pub referenced_columns: Vec<String>,
}
