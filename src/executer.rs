use crate::printer::print_table;
use crate::storage::Database;
use crate::types::parse_types::Query;

// Executes a parsed query (AST) against the database
pub fn execute(db: &mut Database, ast: Query) -> Result<(), String> {
    let _ = match ast {
        // CREATE TABLE name (...) -> create new table
        Query::CreateTable { name, columns } => db.create_table(&name, columns)?,

        // INSERT INTO table (...) VALUES (...) -> insert new row
        Query::Insert {
            table,
            column_names,
            values,
        } => db.insert_into(&table, column_names, values)?,

        // SELECT ... FROM table -> fetch and print matching rows
        Query::Select {
            table,
            column_names,
        } => {
            let rows = db.select(&table, column_names)?;
            let table = db.tables.get(&table).unwrap();
            print_table(&table.columns, &rows);
        }
    };

    Ok(())
}
