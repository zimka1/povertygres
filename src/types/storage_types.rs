use std::fmt;

#[derive(Debug, Clone)]
pub struct Table {
    // Table name
    pub name: String,
    // List of columns in the table
    pub columns: Vec<Column>,
    // Stored rows in the table
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone)]
pub struct Column {
    // Column name
    pub name: String,
    // Data type of the column
    pub column_type: ColumnType,
}

// Supported data types for columns
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int,
    Text,
    Bool,
}

#[derive(Debug, Clone)]
pub struct Row {
    // Ordered list of values (same order as columns)
    pub values: Vec<Value>,
}

// Represents a single cell value
#[derive(Debug, Clone, PartialEq)]
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