use crate::types::parser_types::{Condition, Query};

pub fn parse_select(input: &str, filter: Option<Condition>) -> Result<Query, String> {
    let prefix = "select ";

    let from_index = input.find("from").ok_or("Missing 'from'")?;

    
    let column_names: Vec<&str> = input[prefix.len()..from_index].trim().split(',').collect();
    
    let column_names: Vec<String> = column_names
    .iter()
    .map(|name| name.trim().to_string())
    .collect();

    let after_columns = input[from_index..].trim();
    
    let tokens = tokenize(after_columns);

    println!("{:?}", tokens);
    
    return Ok(Query::Select {
        table_name: table_name.to_string(),
        column_names,
        filter,
    });
}


pub fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' | '\n' | '\r' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            ',' | '(' | ')' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                tokens.push(c.to_string());
            }
            '\'' | '"' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                let quote = c;
                let mut literal = String::new();
                literal.push(quote);
                while let Some(&next) = chars.peek() {
                    literal.push(next);
                    chars.next();
                    if next == quote {
                        break;
                    }
                }
                tokens.push(literal);
            }
            '=' | '!' | '<' | '>' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                let mut op = String::new();
                op.push(c);
                if let Some(&'=') = chars.peek() {
                    op.push('=');
                    chars.next();
                }
                tokens.push(op);
            }
            _ => {
                current.push(c);
            }
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}
