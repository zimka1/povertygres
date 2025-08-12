use crate::types::parser_types::{Condition, Query};

/// Parses a very minimal `DELETE FROM <table>` statement.
pub fn parse_delete(input: &str, filter: Option<Condition>) -> Result<Query, String> {
    let from_index = input.find("from").ok_or("Missing 'from'")?;

    let table_name = input[from_index + "from".len()..].trim();

    return Ok(Query::Delete {
        table_name: table_name.to_string(),
        filter,
    });
}

