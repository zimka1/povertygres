use crate::types::parser_types::Query;
use crate::types::transaction_types::IsolationLevel;

pub fn parse_set_session(input: &str) -> Result<Query, String> {
    let prefix = "set session characteristics as transaction isolation level";
    let lower = input.to_ascii_lowercase();

    if !lower.starts_with(prefix) {
        return Err("Invalid SET SESSION syntax".to_string());
    }

    let rest = input[prefix.len()..].trim().to_ascii_lowercase();

    match rest.as_str() {
        "read committed" => Ok(Query::SetSessionIsolationLevel(IsolationLevel::ReadCommitted)),
        "repeatable read" => Ok(Query::SetSessionIsolationLevel(IsolationLevel::RepeatableRead)),
        "serializable" => Err("Serializable isolation level not implemented".to_string()),
        _ => Err(format!("Unknown isolation level: {}", rest)),
    }
}
