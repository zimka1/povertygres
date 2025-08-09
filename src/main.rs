use std::io::{self, Write};
mod executer;
mod parser;
mod types;
mod errors;

use crate::types::storage_types::Database;
use crate::parser::main_parser::parse_query;
use crate::executer::executer::execute;

fn main() {
    // Initialize an empty in-memory database
    let mut db = Database::new();

    loop {
        // Print prompt symbol
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();

        // Read user input from stdin
        if io::stdin().read_line(&mut input).is_err() {
            println!("Failed to read line");
            continue;
        }

        let input = input.trim();

        // Exit on "exit" command
        if input.eq_ignore_ascii_case("exit") {
            break;
        }

        // Parse query into AST
        match parse_query(input) {
            Ok(ast) => {
                // Execute AST on the database
                if let Err(err) = execute(&mut db, ast) {
                    println!("Execution error: {err}");
                }
            }
            Err(err) => println!("Parse error: {err}"),
        }
    }
}
