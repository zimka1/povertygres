use std::collections::HashMap;

use crate::types::{filter_types::CmpOp, storage_types::ForeignKeyConstraint};

use super::storage_types::{Column, Value};

/// Abstract Syntax Tree (AST) for parsed SQL-like queries
#[derive(Debug)]
pub enum Query {
    /// CREATE TABLE table_name (col1 type1, col2 type2, ...)
    CreateTable {
        table_name: String,
        columns: Vec<Column>,
        primary_key: Option<String>,
        foreign_keys: Vec<ForeignKeyConstraint>,
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
        aliases: HashMap<String, String>, // Table aliases mapping: alias -> table name
        column_names: Vec<String>,        // "*" is represented as ["*"]
        filter: Option<Condition>,        // Optional WHERE clause condition
    },
    /// DELETE FROM table
    Delete {
        table_name: String,
        filter: Option<Condition>, // Optional WHERE clause condition
    },
    /// UPDATE table SET col = val ...
    Update {
        table_name: String,
        column_names: Vec<String>, // Target columns to update
        values: Vec<Value>,        // New values for those columns
        filter: Option<Condition>, // Optional WHERE clause condition
    },
    CreateIndex {
        index_name: String,
        table_name: String,
        column_names: Vec<String>,
    },
}

#[derive(Debug, Clone)]
pub enum Operand {
    Column(String), // A column reference
    Literal(Value), // A literal constant
}

#[derive(Debug, Clone)]
pub enum Condition {
    Cmp(CmpOp, Operand, Operand),        // Comparison operation
    And(Box<Condition>, Box<Condition>), // Logical AND
    Or(Box<Condition>, Box<Condition>),  // Logical OR
    Not(Box<Condition>),                 // Logical NOT
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
    Gt,  // >
    Lt,  // <
    Gte, // >=
    Lte, // <=

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
    Inner, // INNER JOIN
    Left,  // LEFT JOIN
}

#[derive(Debug)]
pub enum FromItem {
    Table(String), // A simple table reference
    Join {
        left: Box<FromItem>,  // Left side of the join
        right: Box<FromItem>, // Right side of the join
        kind: JoinKind,       // Type of join
        on: Condition,        // Join condition
    },
}
