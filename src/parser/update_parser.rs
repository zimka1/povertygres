use crate::types::parser_types::Condition;
use crate::types::parser_types::Query;
use crate::types::storage_types::Value;

/// Parses a minimal `UPDATE table SET col=val, ...`
pub fn parse_update(input: &str, filter: Option<Condition>) -> Result<Query, String> {
    // Normalize input (trim and drop a trailing ';' if present)
    let input = input.trim().trim_end_matches(';');
    let lower = input.to_ascii_lowercase();

    // Expect leading "update "
    let prefix = "update ";
    if !lower.starts_with(prefix) {
        return Err("Expected 'update'".to_string());
    }

    // Find the first "set" (case-insensitive) and slice around it
    let set_pos = lower.find("set").ok_or("Missing 'set'")?;
    let table_name = input[prefix.len()..set_pos].trim();
    let after_set = input[set_pos + "set".len()..].trim();

    // Naive split of assignments by comma: "col = val"
    let split_col_val: Vec<(String, String)> = after_set
        .split(',')
        .map(|col_val| {
            // Split only at the first '=' to keep RHS intact
            let mut parts = col_val.splitn(2, '=');
            let col = parts.next().unwrap().trim().to_string();
            let val = parts.next().unwrap().trim().to_string();
            (col, val)
        })
        .collect();

    // Separate columns and raw values
    let (parsed_cols, values): (Vec<String>, Vec<String>) = split_col_val.into_iter().unzip();

    // Convert raw strings to Value (supports "text", true/false, null, integer)
    let parsed_values: Result<Vec<Value>, String> = values
        .into_iter()
        .map(|raw| {
            // double-quoted text literal
            if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
                let text = &raw[1..raw.len() - 1];
                return Ok(Value::Text(text.to_string()));
            }
            // booleans
            if raw.eq_ignore_ascii_case("true") {
                return Ok(Value::Bool(true));
            }
            if raw.eq_ignore_ascii_case("false") {
                return Ok(Value::Bool(false));
            }
            // NULL literal
            if raw.eq_ignore_ascii_case("null") {
                return Ok(Value::Null);
            }
            // integer
            if let Ok(num) = raw.parse::<i64>() {
                return Ok(Value::Int(num));
            }
            Err(format!("Unrecognized value: {}", raw))
        })
        .collect();

    let parsed_values = parsed_values?;

    // Build the parsed query
    Ok(Query::Update {
        table_name: table_name.to_string(),
        column_names: parsed_cols,
        values: parsed_values,
        filter,
    })
}
