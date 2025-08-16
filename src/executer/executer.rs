use super::printer::print_table;
use super::select::TableArg;
use crate::types::parser_types::{FromItem, Query};
use crate::types::storage_types::Database;

// Executes a parsed query (AST) against the database
pub fn execute(db: &mut Database, ast: Query) -> Result<(), String> {
    let _ = match ast {
        // CREATE TABLE name (...) -> create a new table
        Query::CreateTable {
            table_name,
            columns,
        } => db.create_table(&table_name, columns)?,

        // INSERT INTO table (...) VALUES (...) -> insert a new row
        Query::Insert {
            table_name,
            column_names,
            values,
        } => db.insert_into(&table_name, column_names, values)?,

        // SELECT ... FROM table -> fetch and print matching rows
        Query::Select {
            from_table,
            column_names,
            filter,
        } => {
            let (table_ref, rows) = if let FromItem::Table(tab) = from_table{
                db.select(&TableArg::TableName(tab.name), &column_names, filter)?
            } else {
                let collected_join_table = db.collect_join_table(from_table);
            };
            if column_names.get(0).unwrap() == "*" {
                let column_names = table_ref.columns.iter().map(|c| c.name.clone()).collect();
                print_table(&column_names, &rows);
            } else {
                print_table(&column_names, &rows);
            }
        }
        Query::Delete { table_name, filter } => {
            println!("DELETE {:?}", db.delete(&table_name, filter).unwrap())
        }
        Query::Update {
            table_name,
            column_names,
            values,
            filter,
        } => db.update(&table_name, column_names, values, filter)?,
    };

    Ok(())
}
