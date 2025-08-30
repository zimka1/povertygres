use std::collections::HashMap;

use super::where_parser::parse_where;
use crate::types::parser_types::{Condition, FromItem, JoinKind, Query};

/// Parse a SELECT query into a Query::Select AST node
pub fn parse_select(input: &str, filter: Option<Condition>) -> Result<Query, String> {
    let prefix = "select ";

    let mut aliases: HashMap<String, String> = HashMap::new();

    let from_index = input.find("from").ok_or("Missing 'from'")?;

    // Split column list before FROM
    let column_names: Vec<&str> = input[prefix.len()..from_index].trim().split(',').collect();

    let column_names: Vec<String> = column_names
        .iter()
        .map(|name| name.trim().to_string())
        .collect();

    let after_columns = input[from_index..].trim();
    let mut current_from: Option<FromItem> = None;
    let tokens = tokenize(after_columns);
    let mut i = 0;
    let mut join_type: Option<JoinKind> = None;

    // Parse FROM + JOIN clauses
    while i < tokens.len() {
        match tokens[i].to_ascii_lowercase().as_str() {
            "from" => {
                let table = parse_from_item(&tokens, &mut i, &mut aliases)?;
                current_from = Some(table);
            }
            "left" => {
                join_type = Some(JoinKind::Left);
                i += 1;
            }
            "inner" => {
                join_type = Some(JoinKind::Inner);
                i += 1;
            }
            "join" => {
                let right_table = parse_from_item(&tokens, &mut i, &mut aliases)?;
                expect_token("on", &tokens, &mut i)?;
                let on_condition: Condition = parse_condition(&tokens, &mut i)?;
                let left_item = current_from.take().expect("JOIN without left side");
                current_from = Some(FromItem::Join {
                    left: Box::new(left_item),
                    right: Box::new(right_table),
                    kind: join_type.take().unwrap_or(JoinKind::Inner),
                    on: on_condition,
                });
            }
            _ => {
                i += 1;
            }
        }
    }

    println!("{:?}", tokens);

    Ok(Query::Select {
        from_table: current_from.unwrap(),
        aliases,
        column_names,
        filter,
    })
}

/// Parse a FROM item (table with optional alias)
fn parse_from_item(
    tokens: &[String],
    i: &mut usize,
    aliases: &mut HashMap<String, String>,
) -> Result<FromItem, String> {
    *i += 1;
    let name = tokens
        .get(*i)
        .ok_or_else(|| format!("Expected table name at position {}", i))?
        .to_string();
    *i += 1;

    let alias = if tokens
        .get(*i)
        .map(|t| t.eq_ignore_ascii_case("as"))
        .unwrap_or(false)
    {
        *i += 1;
        Some(
            tokens
                .get(*i)
                .ok_or_else(|| format!("Expected alias after AS at position {}", i))?
                .to_string(),
        )
    } else {
        None
    };

    if alias.is_some() {
        aliases.insert(alias.unwrap(), name.clone());
    }

    *i += 1;

    Ok(FromItem::Table(name))
}

/// Ensure the next token matches the expected keyword
fn expect_token(exp: &str, tokens: &[String], i: &mut usize) -> Result<(), String> {
    if tokens
        .get(*i)
        .map(|t| t.eq_ignore_ascii_case(exp))
        .unwrap_or(false)
    {
        *i += 1;
        Ok(())
    } else {
        Err(format!("Expected '{}', found {:?}", exp, tokens.get(*i)))
    }
}

/// Parse a join ON condition until the next JOIN/keyword
fn parse_condition(tokens: &[String], i: &mut usize) -> Result<Condition, String> {
    let mut cond = String::from("");
    while *i < tokens.len() {
        match tokens[*i].to_ascii_lowercase().as_str() {
            "left" | "inner" | "join" => break,
            _ => {
                cond.push_str(tokens[*i].as_str());
                cond.push(' ');
            }
        }
        *i += 1;
    }
    parse_where(&cond)
}

/// Tokenize SQL input string into keywords, identifiers and literals
fn tokenize(input: &str) -> Vec<String> {
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
