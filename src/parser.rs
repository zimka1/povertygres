use crate::storage::Column;
use crate::types::Value;
use crate::types::{ColumnType, Query};

pub fn parse_query(input: &str) -> Result<Query, String> {
    let input = input.trim().to_ascii_lowercase();

    if input.starts_with("create table ") {
        let prefix = "create table ";
        let after_prefix = &input[prefix.len()..];

        let paren_index = after_prefix
            .find('(')
            .ok_or("Missing '(' in create table")?;

        let table_name = after_prefix[..paren_index].trim();

        let inside_parens = after_prefix[paren_index + 1..].trim_end_matches(')').trim();

        let column_defs: Vec<&str> = inside_parens.split(',').collect();

        let mut columns = Vec::new();

        for col_def in column_defs {
            let mut parts = col_def.trim().split_whitespace();

            let name = parts.next().ok_or("Missing column name")?;
            let type_str = parts.next().ok_or("Missing column type")?;

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
            name: table_name.to_string(),
            columns,
        });
    } else if input.starts_with("insert into") {
        let prefix = "insert into ";

        let values_index = input.find("values").ok_or("Missing 'values'")?;
        let before_values = input[prefix.len()..values_index].trim();
        let after_values = input[values_index + "values".len()..].trim();

        let (table_name, column_names): (&str, Option<Vec<&str>>) = if before_values.contains('(') {
            let open = before_values
                .find('(')
                .ok_or("Missing '(' in column list")?;
            let close = before_values
                .find(')')
                .ok_or("Missing ')' in column list")?;

            let table = before_values[..open].trim();
            let cols_str = &before_values[open + 1..close];
            let cols: Vec<&str> = cols_str.split(',').map(|s| s.trim()).collect();

            (table, Some(cols))
        } else {
            (before_values, None)
        };

        let open = after_values.find('(').ok_or("Missing '(' in values")?;
        let close = after_values.find(')').ok_or("Missing ')' in values")?;
        let values_str = &after_values[open + 1..close];
        let raw_values: Vec<&str> = values_str.split(',').map(|s| s.trim()).collect();

        let parsed_values: Result<Vec<Value>, String> = raw_values
            .into_iter()
            .map(|raw| {
                if raw.starts_with('"') && raw.ends_with('"') {
                    let text = &raw[1..raw.len() - 1];
                    return Ok(Value::Text(text.to_string()));
                }
                if raw.eq_ignore_ascii_case("true") {
                    return Ok(Value::Bool(true));
                }
                if raw.eq_ignore_ascii_case("false") {
                    return Ok(Value::Bool(false));
                }
                if let Ok(num) = raw.parse::<i64>() {
                    return Ok(Value::Int(num));
                }
                Err(format!("Unrecognized value: {}", raw))
            })
            .collect();

        let values = parsed_values?;
        let column_names =
            column_names.map(|cols| cols.into_iter().map(|s| s.to_string()).collect());

        return Ok(Query::Insert {
            table: table_name.to_string(),
            column_names: column_names,
            values: values,
        });
    } else if input.starts_with("select") {
        let prefix = "select ";

        let from_index = input.find("from").ok_or("Missing 'from'")?;

        let table_name = input[from_index + "from".len()..].trim();

        let column_names: Vec<&str> = input[prefix.len()..from_index].trim().split(',').collect();

        let column_names: Vec<String> = column_names
            .iter()
            .map(|name| name.trim().to_string())
            .collect();

        return Ok(Query::Select {
            table: table_name.to_string(),
            column_names,
        });
    }

    Err("Unrecognized command".to_string())
}
