use std::io::{self, Write};
mod parser;
mod storage;
mod types;
mod executer;

use crate::storage::Database;

fn main() {
    let mut db = Database::new();

    loop {
        print!("> ");
        io::stdout().flush().unwrap();

        let mut input = String::new();

        if io::stdin().read_line(&mut input).is_err() {
            println!("Failed to read line");
            continue;
        }

        let input = input.trim();
        if input.eq_ignore_ascii_case("exit") {
            break;
        }

        match parser::parse_query(input) {
            Ok(ast) => {
                if let Err(err) = executer::execute(&mut db, ast) {
                    println!("Execution error: {err}");
                }
            }
            Err(err) => println!("Parse error: {err}"),
        }
    }
}
