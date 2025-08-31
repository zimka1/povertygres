use super::{
    create::parse_create_table, insert::parse_insert, select::parse_select, r#where::parse_where,
};
use crate::{
    parser::{delete::parse_delete, index::parse_create_index, update::parse_update},
    types::parser_types::{Condition, Query},
};

// Parses a raw SQL-like input string into an AST representation (Query enum)
pub fn parse_query(input: &str) -> Result<Query, String> {
    // Normalize input (trim and lowercase)
    let mut input = input.trim();

    let where_index = input.to_ascii_lowercase().find("where");

    let mut condition: Option<Condition> = None;

    if let Some(index) = where_index {
        let after_where = input[index + "where".len()..].trim();
        input = input[..index].trim();
        condition = Some(parse_where(after_where)?);
    }

    if input.to_ascii_lowercase().starts_with("create table ") {
        parse_create_table(input)
    } else if input.to_ascii_lowercase().starts_with("create index") {
        parse_create_index(input)
    } else if input.to_ascii_lowercase().starts_with("insert into") {
        parse_insert(input)
    } else if input.to_ascii_lowercase().starts_with("select ") {
        parse_select(input, condition)
    } else if input.to_ascii_lowercase().starts_with("delete ") {
        parse_delete(input, condition)
    } else if input.to_ascii_lowercase().starts_with("update ") {
        parse_update(input, condition)
    } else {
        Err("Unrecognized command".to_string())
    }
}
