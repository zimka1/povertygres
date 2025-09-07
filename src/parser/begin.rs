use crate::types::parser_types::Query;
use crate::types::transaction_types::IsolationLevel;

pub fn parse_begin(input: &str) -> Result<Query, String> {
    let prefix = "begin";

    let mut rest = input.trim()[prefix.len()..].trim();

    if rest.is_empty() {
        return Ok(Query::Begin { isolation: None });
    }

    let lower = rest.to_ascii_lowercase();

    if lower.starts_with("isolation level") {
        rest = rest[("isolation level".len())..].trim();

        return match rest.to_ascii_lowercase().as_str() {
            "read committed" => Ok(Query::Begin { isolation: Some(IsolationLevel::ReadCommitted) }),
            "repeatable read" => Ok(Query::Begin { isolation: Some(IsolationLevel::RepeatableRead) }),
            "serializable" => {
                Err("Serializable isolation level not implemented".to_string())
            }
            other => Err(format!("Unknown isolation level: {}", other)),
        };
    }

    Err(format!("Unexpected syntax in BEGIN: '{}'", rest))
}
