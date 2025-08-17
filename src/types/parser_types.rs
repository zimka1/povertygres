use std::collections::HashMap;

use crate::types::filter_types::CmpOp;

use super::storage_types::{Column, Value};

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
        aliases: HashMap<String, String>,
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

#[derive(Debug, Clone)]
pub enum Operand {
    Column(String),
    Literal(Value),
}

#[derive(Debug, Clone)]
pub enum Condition {
    Cmp(CmpOp, Operand, Operand),
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
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
pub enum FromItem {
    Table(String),
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        kind: JoinKind,
        on: Condition,
    },
}
