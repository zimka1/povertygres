use super::{
    create_parser::parse_create_table, insert_parser::parse_insert, select_parser::parse_select,
    where_parser::parse_where,
};
use crate::{
    parser::delete_parser::parse_delete,
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
        println!("{:?}", condition);
    }

    if input.to_ascii_lowercase().starts_with("create table ") {
        parse_create_table(input, condition)
    } else if input.to_ascii_lowercase().starts_with("insert into") {
        parse_insert(input)
    } else if input.to_ascii_lowercase().starts_with("select ") {
        parse_select(input, condition)
    } else if input.to_ascii_lowercase().starts_with("delete ") {
        parse_delete(input, condition)
    } else {
        Err("Unrecognized command".to_string())
    }
}
