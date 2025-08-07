use crate::storage::Column;
use std::fmt;

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

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int,
    Text,
    Bool,
}

#[derive(Debug)]
pub enum Query {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table: String,
        column_names: Option<Vec<String>>,
        values: Vec<Value>,
    },
    Select {
        table: String,
        column_names: Vec<String>,
    },
}
