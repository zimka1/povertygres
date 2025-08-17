use super::printer::print_table;
use super::select::TableArg;
use crate::types::parser_types::{FromItem, Query};
use crate::types::storage_types::Database;

/// Executes a parsed query (AST) against the database
pub fn execute(db: &mut Database, ast: Query) -> Result<(), String> {
    match ast {
        // CREATE TABLE name (...)
        Query::CreateTable {
            table_name,
            columns,
        } => db.create_table(&table_name, columns)?,

        // INSERT INTO table (...) VALUES (...)
        Query::Insert {
            table_name,
            column_names,
            values,
        } => db.insert_into(&table_name, column_names, values)?,

        // SELECT ... FROM ...
        Query::Select {
            from_table,
            aliases,
            column_names,
            filter,
        } => {
            let (columns, rows) = match from_table {
                FromItem::Table(table_name) => {
                    db.select(&TableArg::TableName(table_name), &column_names, filter)?
                }
                _ => {
                    let join = db.collect_join_table(from_table, &aliases)?;
                    db.select(&TableArg::JoinTable(join), &column_names, filter)?
                }
            };

            if column_names.get(0).map(|s| s.as_str()) == Some("*") {
                // show actual column names, not only table aliases
                let names = columns
                    .iter()
                    .map(|c| format!("{}.{}", c.table_alias, c.column_name))
                    .collect();
                print_table(&names, &rows);
            } else {
                print_table(&column_names, &rows);
            }
        }

        // DELETE FROM ...
        Query::Delete { table_name, filter } => {
            let deleted = db.delete(&table_name, filter)?;
            println!("DELETE {}", deleted);
        }

        // UPDATE ...
        Query::Update {
            table_name,
            column_names,
            values,
            filter,
        } => db.update(&table_name, column_names, values, filter)?,
    };

    Ok(())
}
