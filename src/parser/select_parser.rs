use crate::types::parse_types::{Query};

pub fn parse_select(input: &str) -> Result<Query, String> {
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