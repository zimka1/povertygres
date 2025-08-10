use crate::types::parser_types::Query;
use crate::types::storage_types::{Column, ColumnType};

pub fn parse_create_table(input: &str) -> Result<Query, String> {
    let prefix = "create table ";
    let after_prefix = &input[prefix.len()..];

    // Find '(' to split table name and column definitions
    let paren_index = after_prefix
        .find('(')
        .ok_or("Missing '(' in create table")?;

    let table_name = after_prefix[..paren_index].trim();

    // Extract column definitions between parentheses
    let inside_parens = after_prefix[paren_index + 1..].trim_end_matches(')').trim();
    let column_defs: Vec<&str> = inside_parens.split(',').collect();

    let mut columns = Vec::new();

    for col_def in column_defs {
        let mut parts = col_def.trim().split_whitespace();

        let name = parts.next().ok_or("Missing column name")?;
        let type_str = parts.next().ok_or("Missing column type")?;

        // Map string type to enum
        let column_type = match type_str.to_ascii_lowercase().as_str() {
            "int" => ColumnType::Int,
            "text" => ColumnType::Text,
            "bool" => ColumnType::Bool,
            _ => return Err(format!("Unknown column type: {}", type_str)),
        };

        columns.push(Column {
            name: name.to_string(),
            column_type,
        });
    }

    return Ok(Query::CreateTable {
        table_name: table_name.to_string(),
        columns,
    });
}
