use crate::types::parser_types::{Condition, Query};
use crate::types::storage_types::Value;

pub fn parse_insert(input: &str) -> Result<Query, String> {
    let prefix = "insert into ";

    let values_index = input.find("values").ok_or("Missing 'values'")?;
    let before_values = input[prefix.len()..values_index].trim();
    let after_values = input[values_index + "values".len()..].trim();

    // Check for optional column list: insert into table(col1, col2) ...
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

    // Parse values from parentheses
    let open = after_values.find('(').ok_or("Missing '(' in values")?;
    let close = after_values.find(')').ok_or("Missing ')' in values")?;
    let values_str = &after_values[open + 1..close];
    let raw_values: Vec<&str> = values_str.split(',').map(|s| s.trim()).collect();

    // Convert each raw string value to a Value enum
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
    let column_names = column_names.map(|cols| cols.into_iter().map(|s| s.to_string()).collect());

    return Ok(Query::Insert {
        table_name: table_name.to_string(),
        column_names: column_names,
        values: values,
    });
}
