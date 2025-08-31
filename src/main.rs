use std::io::{self, Write};
mod catalog;
mod consts;
mod engine;
mod errors;
mod executer;
mod parser;
mod storage;
mod types;

use crate::engine::Engine;
use crate::executer::executer::execute;
use crate::parser::main::parse_query;
use std::env;
use std::fs;

fn main() {
    let mut engine = Engine::open().expect("catalog init failed");

    let args: Vec<String> = env::args().collect();

    if args.len() > 1 && args[1] == "--file" {
        let filename = &args[2];
        let contents = fs::read_to_string(filename).expect("Failed to read file");
        for stmt in contents.split(';') {
            let stmt = stmt.trim();
            if stmt.is_empty() {
                continue;
            }
            match parse_query(stmt) {
                Ok(ast) => {
                    if let Err(err) = execute(&mut engine, ast) {
                        eprintln!("Execution error: {err}");
                    }
                }
                Err(err) => eprintln!("Parse error: {err}"),
            }
        }
        return;
    }

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
                if let Err(err) = execute(&mut engine, ast) {
                    println!("Execution error: {err}");
                }
            }
            Err(err) => println!("Parse error: {err}"),
        }
    }
}
