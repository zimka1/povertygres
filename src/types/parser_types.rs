use super::storage_types::{Column, Value, Table};

/// Abstract Syntax Tree (AST) for parsed SQL-like queries
#[derive(Debug)]
pub enum Query {
    /// CREATE TABLE table_name (col1 type1, col2 type2, ...)
    CreateTable {
        table_name: String,
        columns: Vec<Column>,
    },
    /// INSERT INTO table (col1, col2) VALUES (...)
    Insert {
        table_name: String,
        column_names: Option<Vec<String>>, // Optional list of target columns; None means "all columns"
        values: Vec<Value>,                // Values to insert (order matches columns)
    },
    /// SELECT col1, col2 FROM table
    Select {
        from_table: FromItem,
        column_names: Vec<String>, // "*" is represented as ["*"]
        filter: Option<Condition>,
    },
    /// DELETE FROM table
    Delete {
        table_name: String,
        filter: Option<Condition>,
    },
    Update {
        table_name: String,
        column_names: Vec<String>,
        values: Vec<Value>,
        filter: Option<Condition>,
    },
}

/// Boolean condition tree used for WHERE clauses
#[derive(Debug, Clone)]
pub enum Condition {
    Eq(String, Value),                   // column = value
    Neq(String, Value),                  // column != value
    Gt(String, Value),                   // column > value
    Lt(String, Value),                   // column < value
    Gte(String, Value),                  // column >= value
    Lte(String, Value),                  // column <= value
    And(Box<Condition>, Box<Condition>), // logical AND
    Or(Box<Condition>, Box<Condition>),  // logical OR
    Not(Box<Condition>),                 // logical NOT
}

/// Token types produced by the WHERE clause tokenizer
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String), // column name or identifier
    Int(i64),      // integer literal
    Str(String),   // string literal
    Bool(bool),    // boolean literal

    // Comparison operators
    Eq,  // =
    Neq, // !=
    Gt,
    Lt,
    Gte,
    Lte, // > < >= <=

    // Logical operators
    And,
    Or,
    Not,

    // Parentheses for grouping
    LParen, // (
    RParen, // )
}

/// Node type used as an intermediate representation when
/// building a Condition AST from RPN output
#[derive(Debug)]
pub enum Node {
    Col(String),     // column reference
    Val(Value),      // literal value
    Cond(Condition), // fully built condition subtree
}

#[derive(Debug)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Debug)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug)]
pub enum FromItem {
    Table(TableRef),
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        kind: JoinKind,
        on: Condition,       
    },
}