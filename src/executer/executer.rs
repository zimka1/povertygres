use super::printer::print_table;
use super::select::TableArg;
use crate::engine::Engine;
use crate::errors::engine_error::EngineError;
use crate::types::parser_types::{FromItem, Query};

/// Executes a parsed query (AST) against the database
pub fn execute(engine: &mut Engine, ast: Query) -> Result<(), EngineError> {
    match ast {
        // CREATE TABLE name (...)
        Query::CreateTable {
            table_name,
            columns,
            primary_key
        } => engine.create_table_in_both(&table_name, columns, primary_key)?,

        // INSERT INTO table (...) VALUES (...)
        Query::Insert {
            table_name,
            column_names,
            values,
        } => engine.db.insert_into(&table_name, column_names, values)?,

        // SELECT ... FROM ...
        Query::Select {
            from_table,
            aliases,
            column_names,
            filter,
        } => {
            let (columns, rows) = match from_table {
                FromItem::Table(table_name) => {
                    engine
                        .db
                        .select(&TableArg::TableName(table_name), &column_names, filter)?
                }
                _ => {
                    let join = engine.db.collect_join_table(from_table, &aliases)?;
                    engine
                        .db
                        .select(&TableArg::JoinTable(join), &column_names, filter)?
                }
            };

            dbg!(&rows);

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
            let deleted = engine.db.delete(&table_name, filter)?;
            println!("DELETE {}", deleted);
        }

        // UPDATE ...
        Query::Update {
            table_name,
            column_names,
            values,
            filter,
        } => engine
            .db
            .update(&table_name, column_names, values, filter)?,
    };

    Ok(())
}
