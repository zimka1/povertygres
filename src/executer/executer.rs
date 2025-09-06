use super::printer::print_table;
use super::select::TableArg;
use crate::engine::Engine;
use crate::errors::engine_error::EngineError;
use crate::types::parser_types::{FromItem, Query};
use crate::types::transaction_types::TxStatus;

/// Executes a parsed query (AST) against the database
pub fn execute(engine: &mut Engine, ast: Query) -> Result<(), EngineError> {
    match ast {
        Query::Begin => {
            if engine.current_xid.is_some() {
                return Err("Transaction already in progress".to_string().into());
            }
            let xid = {
                let cat = engine.cat.catalog_mut();
                let xid = cat.next_xid;
                cat.next_xid += 1;
                xid
            };
            engine.db.transaction_manager.begin(xid);
            {
                let cat = engine.cat.catalog_mut();
                cat.transactions.insert(xid, TxStatus::InProgress);
            }
            engine.cat.persist()?;
            engine.current_xid = Some(xid);
            println!("BEGIN (xid = {})", xid);
        }
        Query::Commit => {
            if let Some(xid) = engine.current_xid.take() {
                engine.db.transaction_manager.commit(xid);
                {
                    let cat = engine.cat.catalog_mut();
                    cat.transactions.insert(xid, TxStatus::Committed);
                }
                engine.cat.persist()?;
                println!("COMMIT (xid = {})", xid);
            } else {
                return Err("No active transaction".to_string().into());
            }
        }
        Query::Rollback => {
            if let Some(xid) = engine.current_xid.take() {
                engine.db.transaction_manager.rollback(xid);
                {
                    let cat = engine.cat.catalog_mut();
                    cat.transactions.insert(xid, TxStatus::Aborted);
                }
                engine.cat.persist()?;
                println!("ROLLBACK (xid = {})", xid);
            } else {
                return Err("No active transaction".to_string().into());
            }
        }
        Query::Vacuum { table_name } => {
            if let Some(tab) = engine.db.tables.get(&table_name) {
                let removed = tab.heap.vacuum(&engine.db.transaction_manager, &tab.columns);
                println!("VACUUM {}: removed {} dead tuples", table_name, removed);
            } else {
                return Err(format!("Table '{}' not found", table_name).to_string().into());
            }
        }
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
                engine
                    .db
                    .insert_into(&table_name, column_names, values, xid)?;
            } else {
                // autocommit mode
                let xid = {
                    let cat = engine.cat.catalog_mut();
                    let xid = cat.next_xid; cat.next_xid += 1; xid
                };
                engine.db.transaction_manager.begin(xid);
                { let cat = engine.cat.catalog_mut(); cat.transactions.insert(xid, TxStatus::InProgress); }
                engine.cat.persist()?;
                engine
                    .db
                    .insert_into(&table_name, column_names, values, xid)?;
                engine.db.transaction_manager.commit(xid);
                { let cat = engine.cat.catalog_mut(); cat.transactions.insert(xid, TxStatus::Committed); }
                engine.cat.persist()?;
            }
        }
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
                engine.current_xid.unwrap_or(0)
            };

            let (columns, rows) = match from_table {
                FromItem::Table(table_name) => engine.db.select(
                    &TableArg::TableName(table_name),
                    &column_names,
                    filter,
                    xid,
                )?,
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
                let xid = {
                    let cat = engine.cat.catalog_mut();
                    let xid = cat.next_xid; cat.next_xid += 1; xid
                };
                engine.db.transaction_manager.begin(xid);
                { let cat = engine.cat.catalog_mut(); cat.transactions.insert(xid, TxStatus::InProgress); }
                engine.cat.persist()?;
                let deleted = engine.db.delete(&table_name, filter, xid)?;
                engine.db.transaction_manager.commit(xid);
                { let cat = engine.cat.catalog_mut(); cat.transactions.insert(xid, TxStatus::Committed); }
                engine.cat.persist()?;
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
                engine
                    .db
                    .update(&table_name, column_names, values, filter, xid)?;
            } else {
                let xid = {
                    let cat = engine.cat.catalog_mut();
                    let xid = cat.next_xid; cat.next_xid += 1; xid
                };
                engine.db.transaction_manager.begin(xid);
                { let cat = engine.cat.catalog_mut(); cat.transactions.insert(xid, TxStatus::InProgress); }
                engine.cat.persist()?;
                engine
                    .db
                    .update(&table_name, column_names, values, filter, xid)?;
                engine.db.transaction_manager.commit(xid);
                { let cat = engine.cat.catalog_mut(); cat.transactions.insert(xid, TxStatus::Committed); }
                engine.cat.persist()?;
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
