use super::printer::print_table;
use crate::types::storage_types::Database;
use crate::types::parse_types::{Query};

// Executes a parsed query (AST) against the database
pub fn execute(db: &mut Database, ast: Query) -> Result<(), String> {
    let _ = match ast {
        // CREATE TABLE name (...) -> create a new table
        Query::CreateTable { name, columns, filter } => db.create_table(&name, columns)?,

        // INSERT INTO table (...) VALUES (...) -> insert a new row
        Query::Insert {
            table,
            column_names,
            values,
            filter
        } => db.insert_into(&table, column_names, values)?,

        // SELECT ... FROM table -> fetch and print matching rows
        Query::Select {
            table,
            column_names,
            filter
        } => {
            // Step 1: Retrieve rows from the table (projection of requested columns)
            let rows = db.select(&table, &column_names, filter)?;

            // Step 2: Get a reference to the full table (to access column metadata)
            let table_ref = db.tables.get(&table).expect("table not found");

            // Step 4: Print results in a formatted table
            print_table(&column_names, &rows);
        }
    };

    Ok(())
}
