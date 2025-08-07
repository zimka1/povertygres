use crate::storage::Column;

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
