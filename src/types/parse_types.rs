use super::storage_types::{Column, Value};

// Abstract Syntax Tree (AST) for parsed SQL-like queries
#[derive(Debug)]
pub enum Query {
    // CREATE TABLE table_name (col1 type1, col2 type2, ...)
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    // INSERT INTO table (col1, col2) VALUES (...)
    Insert {
        table: String,
        column_names: Option<Vec<String>>, // Optional list of target columns
        values: Vec<Value>,                // Values to insert
    },
    // SELECT col1, col2 FROM table
    Select {
        table: String,
        column_names: Vec<String>, // '*' is represented as ["*"]
    },
}


#[derive(Debug, Clone)]
pub enum Condition {
    Eq(String, Value),        // column = value
    Neq(String, Value),       // column != value
    Gt(String, Value),        // column > value
    Lt(String, Value),        // column < value
    Gte(String, Value),       // column >= value
    Lte(String, Value),       // column <= value
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String),       // имя колонки
    Int(i64),
    Str(String),
    Bool(bool),

    Eq,                  // =
    Neq,                 // !=
    Gt, Lt, Gte, Lte,    // > < >= <=

    And,
    Or,
    Not,

    LParen,              // (
    RParen,              // )
}

#[derive(Debug)]
pub enum Node {
    Col(String),
    Val(Value),
    Cond(Condition),
}