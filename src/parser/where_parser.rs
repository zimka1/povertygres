use crate::types::parser_types::{Condition, Node, Token};
use crate::types::storage_types::Value;

/// Main entry point for parsing a WHERE clause string into a Condition AST
pub fn parse_where(where_input: &str) -> Result<Condition, String> {
    let tokens = tokenize(where_input)?; // Step 1: tokenize the input string
    let rpn = shunting_yard(tokens)?; // Step 2: convert tokens to RPN using Shunting-Yard
    rpn_to_condition(rpn) // Step 3: build Condition AST from RPN
}

/// Splits a WHERE clause string into a vector of tokens
fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<char> = input.chars().collect();
    let mut tokens: Vec<Token> = Vec::new();
    let mut i = 0;

    while i < chars.len() {
        let char = chars[i];
        match char {
            // Skip whitespace
            ' ' | '\t' | '\n' => {
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '=' => {
                tokens.push(Token::Eq);
                i += 1;
            }
            '!' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Neq);
                i += 2;
            }
            '>' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Gte);
                i += 2;
            }
            '>' => {
                tokens.push(Token::Gt);
                i += 1;
            }
            '<' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                tokens.push(Token::Lte);
                i += 2;
            }
            '<' => {
                tokens.push(Token::Lt);
                i += 1;
            }
            '"' => {
                // Parse string literal
                i += 1;
                let start = i;
                while i < chars.len() && chars[i] != '"' {
                    i += 1;
                }
                if i >= chars.len() {
                    return Err("Unterminated string literal".into());
                }
                let text: String = chars[start..i].iter().collect();
                tokens.push(Token::Str(text));
                i += 1;
            }
            _ if char.is_alphabetic() => {
                // Parse identifier or keyword
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                match word.to_ascii_lowercase().as_str() {
                    "and" => tokens.push(Token::And),
                    "or" => tokens.push(Token::Or),
                    "not" => tokens.push(Token::Not),
                    "true" => tokens.push(Token::Bool(true)),
                    "false" => tokens.push(Token::Bool(false)),
                    _ => tokens.push(Token::Ident(word)),
                }
            }
            _ if char.is_ascii_digit() => {
                // Parse integer literal
                let start = i;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                let number_str: String = chars[start..i].iter().collect();
                let number = number_str
                    .parse::<i64>()
                    .map_err(|_| format!("Invalid number: {}", number_str))?;
                tokens.push(Token::Int(number));
            }
            _ => {
                return Err(format!("Unexpected character: '{}'", char));
            }
        }
    }

    Ok(tokens)
}

/// Checks if a token is an operator (comparison or logical)
fn is_operator(t: &Token) -> bool {
    matches!(
        t,
        Token::Or
            | Token::And
            | Token::Not
            | Token::Eq
            | Token::Neq
            | Token::Gt
            | Token::Lt
            | Token::Gte
            | Token::Lte
    )
}

/// Checks if an operator is right-associative
fn is_right_associative(t: &Token) -> bool {
    matches!(t, Token::Not) // Only NOT is right-associative here
}

/// Returns the precedence of a token (higher = binds stronger)
fn precedence(token: &Token) -> u8 {
    match token {
        Token::Or => 1,
        Token::And => 2,
        Token::Not => 3,
        Token::Eq | Token::Neq | Token::Gt | Token::Lt | Token::Gte | Token::Lte => 4,
        _ => 0,
    }
}

/// Converts an infix token list into RPN using the Shunting-Yard algorithm
fn shunting_yard(tokens: Vec<Token>) -> Result<Vec<Token>, String> {
    let mut out: Vec<Token> = Vec::new(); // Output queue (RPN result)
    let mut stack: Vec<Token> = Vec::new(); // Operator stack

    for token in tokens.into_iter() {
        match token {
            Token::LParen => {
                stack.push(Token::LParen);
            }
            Token::RParen => {
                // Pop operators until matching '('
                let mut found = false;
                while let Some(top) = stack.pop() {
                    if matches!(top, Token::LParen) {
                        found = true;
                        break;
                    } else {
                        out.push(top);
                    }
                }
                if !found {
                    return Err("Mismatched parentheses: unexpected ')'".into());
                }
            }
            op if is_operator(&op) => {
                // Pop higher-precedence operators from stack
                while let Some(top) = stack.last() {
                    if is_operator(top)
                        && (precedence(top) > precedence(&op)
                            || (precedence(top) == precedence(&op) && !is_right_associative(&op)))
                    {
                        out.push(stack.pop().unwrap());
                    } else {
                        break;
                    }
                }
                stack.push(op);
            }
            other => {
                // Operand goes directly to output
                out.push(other);
            }
        }
    }

    // Pop remaining operators
    while let Some(op) = stack.pop() {
        if matches!(op, Token::LParen | Token::RParen) {
            return Err("Mismatched parentheses".into());
        }
        out.push(op);
    }

    Ok(out)
}

/// Convert an RPN token list into a Condition AST
fn rpn_to_condition(rpn: Vec<Token>) -> Result<Condition, String> {
    let mut stack: Vec<Node> = Vec::new(); // Stack for intermediate nodes

    for token in rpn {
        match token {
            // Push columns and literal values directly to the stack
            Token::Ident(name) => stack.push(Node::Col(name)),
            Token::Str(value) => stack.push(Node::Val(Value::Text(value))),
            Token::Int(value) => stack.push(Node::Val(Value::Int(value))),
            Token::Bool(value) => stack.push(Node::Val(Value::Bool(value))),

            // Handle binary comparison operators
            Token::Eq | Token::Neq | Token::Gt | Token::Lt | Token::Gte | Token::Lte => {
                let rhs = stack.pop().ok_or("RPN underflow (rhs cond)")?;
                let lhs = stack.pop().ok_or("RPN underflow (lhs cond)")?;

                // Expect a column on the left and a value on the right
                let (col, val) = match (lhs, rhs) {
                    (Node::Col(c), Node::Val(v)) => (c, v),
                    (l, r) => {
                        return Err(format!("Expected column op value, got {:?} and {:?}", l, r));
                    }
                };

                // Build the appropriate Condition variant
                let cond = match token {
                    Token::Eq => Condition::Eq(col, val),
                    Token::Neq => Condition::Neq(col, val),
                    Token::Gt => Condition::Gt(col, val),
                    Token::Lt => Condition::Lt(col, val),
                    Token::Gte => Condition::Gte(col, val),
                    Token::Lte => Condition::Lte(col, val),
                    _ => unreachable!(),
                };

                stack.push(Node::Cond(cond));
            }

            // Handle logical binary operators
            Token::And | Token::Or => {
                let rhs = stack.pop().ok_or("RPN underflow (rhs cond)")?;
                let lhs = stack.pop().ok_or("RPN underflow (lhs cond)")?;

                // Expect two conditions as operands
                let (lhs_c, rhs_c) = match (lhs, rhs) {
                    (Node::Cond(a), Node::Cond(b)) => (a, b),
                    (l, r) => {
                        return Err(format!(
                            "Expected conditions around AND/OR, got {:?} and {:?}",
                            l, r
                        ));
                    }
                };

                // Build the logical Condition
                stack.push(Node::Cond(match token {
                    Token::And => Condition::And(Box::new(lhs_c), Box::new(rhs_c)),
                    Token::Or => Condition::Or(Box::new(lhs_c), Box::new(rhs_c)),
                    _ => unreachable!(),
                }));
            }

            // Handle NOT operator (unary)
            Token::Not => {
                let x = stack.pop().ok_or("RPN underflow (NOT)")?;
                let c = match x {
                    Node::Cond(c) => c,
                    other => return Err(format!("NOT expects condition, got {:?}", other)),
                };
                stack.push(Node::Cond(Condition::Not(Box::new(c))));
            }

            // Parentheses should not appear in RPN
            Token::LParen | Token::RParen => {
                return Err("Parenthesis leaked into RPN".into());
            }
        }
    }

    // At the end, stack should contain exactly one condition
    match stack.pop() {
        Some(Node::Cond(root)) if stack.is_empty() => Ok(root),
        Some(other) => Err(format!("Leftover on stack: {:?}", other)),
        None => Err("Empty RPN".into()),
    }
}
