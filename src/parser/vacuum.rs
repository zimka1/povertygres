use crate::types::parser_types::{Query};

pub fn parse_vacuum(input: &str) -> Result<Query, String> {
    let prefix = "vacuum ";

    let table_name = input[prefix.len()..].trim();

    Ok(Query::Vacuum {
        table_name: table_name.to_string()
    })
}