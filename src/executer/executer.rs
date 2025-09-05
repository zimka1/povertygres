use super::printer::print_table;
use super::select::TableArg;
use crate::engine::Engine;
use crate::errors::engine_error::EngineError;
use crate::types::parser_types::{FromItem, Query};

/// Executes a parsed query (AST) against the database
pub fn execute(engine: &mut Engine, ast: Query) -> Result<(), EngineError> {
    match ast {
        Query::Begin => {
            if engine.current_xid.is_some() {
                return Err("Transaction already in progress".to_string().into());
            }
            let xid = engine.db.transaction_manager.begin();
            engine.current_xid = Some(xid);
            println!("BEGIN (xid = {})", xid);
        },
        Query::Commit => {
            if let Some(xid) = engine.current_xid.take() {
                engine.db.transaction_manager.commit(xid);
                println!("COMMIT (xid = {})", xid);
            } else {
                return Err("No active transaction".to_string().into());
            }
        },
        Query::Rollback => {
            if let Some(xid) = engine.current_xid.take() {
                engine.db.transaction_manager.rollback(xid);
                println!("ROLLBACK (xid = {})", xid);
            } else {
                return Err("No active transaction".to_string().into());
            }
        },
        // CREATE TABLE name (...)
        Query::CreateTable {
            table_name,
            columns,
            primary_key,
            foreign_keys,
        } => engine.create_table_in_both(&table_name, columns, primary_key, foreign_keys)?,

        // INSERT INTO table (...) VALUES (...)
        Query::Insert {
            table_name,
            column_names,
            values,
        } => {
            if let Some(xid) = engine.current_xid {
                // inside active transaction
                engine.db.insert_into(&table_name, column_names, values, xid)?;
            } else {
                // autocommit mode
                let xid = engine.db.transaction_manager.begin();
                engine.db.insert_into(&table_name, column_names, values, xid)?;
                engine.db.transaction_manager.commit(xid);
            }
        },        
        // SELECT ... FROM ...
        Query::Select {
            from_table,
            aliases,
            column_names,
            filter,
        } => {
            // choose xid (active or autocommit)
            let xid = if let Some(x) = engine.current_xid {
                x
            } else {
                let xid = engine.db.transaction_manager.begin();
                // autocommit SELECT doesn't change data, so we can immediately commit
                engine.db.transaction_manager.commit(xid);
                xid
            };
        
            let (columns, rows) = match from_table {
                FromItem::Table(table_name) => {
                    engine
                        .db
                        .select(&TableArg::TableName(table_name), &column_names, filter, xid)?
                }
                _ => {
                    let join = engine.db.collect_join_table(from_table, &aliases)?;
                    engine
                        .db
                        .select(&TableArg::JoinTable(join), &column_names, filter, xid)?
                }
            };
        
            if column_names.get(0).map(|s| s.as_str()) == Some("*") {
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
            if let Some(xid) = engine.current_xid {
                let deleted = engine.db.delete(&table_name, filter, xid)?;
                println!("DELETE {}", deleted);
            } else {
                let xid = engine.db.transaction_manager.begin();
                let deleted = engine.db.delete(&table_name, filter, xid)?;
                engine.db.transaction_manager.commit(xid);
                println!("DELETE {}", deleted);
            }
        }

        // UPDATE ...
        Query::Update {
            table_name,
            column_names,
            values,
            filter,
        } => {
            if let Some(xid) = engine.current_xid {
                engine.db.update(&table_name, column_names, values, filter, xid)?;
            } else {
                let xid = engine.db.transaction_manager.begin();
                engine.db.update(&table_name, column_names, values, filter, xid)?;
                engine.db.transaction_manager.commit(xid);
            }
        }

        Query::CreateIndex {
            index_name,
            table_name,
            column_names,
        } => engine.create_index_in_both(&index_name, &table_name, column_names)?,
    };

    Ok(())
}
