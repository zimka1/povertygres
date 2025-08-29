use crate::types::parser_types::Query;
use crate::types::storage_types::{Column, ColumnType, Value};

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
    let mut primary_key: Option<String> = None;

    for col_def in column_defs {
        let tokens: Vec<&str> = col_def.trim().split_whitespace().collect();

        if tokens.is_empty() {
            continue;
        }

        if tokens[0].eq_ignore_ascii_case("primary") {
            if tokens.len() >= 3 && tokens[1].eq_ignore_ascii_case("key") {
                let pk_col = tokens[2].trim_matches(|c| c == '(' || c == ')');
                primary_key = Some(pk_col.to_string());
                continue;
            } else {
                return Err("Invalid PRIMARY KEY syntax".into());
            }
        }

        let name = tokens[0];
        let type_str = tokens.get(1).ok_or("Missing column type")?;

        // Map string type to enum
        let column_type = match type_str.to_ascii_lowercase().as_str() {
            "int" => ColumnType::Int,
            "text" => ColumnType::Text,
            "bool" => ColumnType::Bool,
            _ => return Err(format!("Unknown column type: {}", type_str)),
        };

        let mut not_null = false;
        let mut default: Option<Value> = None;

        let mut i = 2;
        while i < tokens.len() {
            match tokens[i].to_ascii_lowercase().as_str() {
                "not" if i + 1 < tokens.len() && tokens[i + 1].eq_ignore_ascii_case("null") => {
                    not_null = true;
                    i += 2;
                }
                "primary" if i + 1 < tokens.len() && tokens[i + 1].eq_ignore_ascii_case("key") => {
                    primary_key = Some(name.to_string());
                    not_null = true; // PK always NOT NULL
                    i += 2;
                }
                "default" if i + 1 < tokens.len() => {
                    let val_str = tokens[i + 1];
                    let val = if val_str.starts_with('"') && val_str.ends_with('"') {
                        Value::Text(val_str.trim_matches('"').to_string())
                    } else if val_str.eq_ignore_ascii_case("true") {
                        Value::Bool(true)
                    } else if val_str.eq_ignore_ascii_case("false") {
                        Value::Bool(false)
                    } else if let Ok(num) = val_str.parse::<i64>() {
                        Value::Int(num)
                    } else {
                        return Err(format!("Unsupported default value: {}", val_str));
                    };
                    default = Some(val);
                    i += 2;
                }
                _ => {
                    i += 1;
                }
            }
        }

        columns.push(Column {
            name: name.to_string(),
            column_type,
            not_null,
            default
        });
    }

    return Ok(Query::CreateTable {
        table_name: table_name.to_string(),
        columns,
        primary_key,
    });
}
