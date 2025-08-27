use crate::executer::filter::eval_condition;
use crate::executer::join::JoinTableColumn;
use crate::types::parser_types::Condition;
use crate::types::storage_types::{Column, Database};

fn single_meta(table_name: &str, cols: &Vec<Column>) -> Vec<JoinTableColumn> {
    cols.iter()
        .map(|c| JoinTableColumn {
            table_alias: table_name.to_string(),
            column_name: c.name.clone(),
        })
        .collect()
}

impl Database {
    /// Deletes rows from `table_name` that match `filter`.
    /// Returns the number of deleted rows.
    pub fn delete(&mut self, table_name: &str, filter: Option<Condition>) -> Result<usize, String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        if filter.is_none() {
            let n = table.rows.len();
            table.rows.clear();
            return Ok(n);
        }

        let mut deleted_count = 0;

        let metas = single_meta(table_name, &table.columns);

        if let Some(cond) = &filter {
            let rows = table.heap.scan_all_with_pos(&table.columns);

            for (page_no, slot_no, row) in rows {
                match eval_condition(cond, &row, &metas, None, None) {
                    Ok(true) => {
                        table.heap.delete_at(page_no, slot_no)?;
                        deleted_count += 1;
                    }
                    Ok(false) => { /* keep */ }
                    Err(e) => return Err(e.to_string()),
                }
            }
        }

        Ok(deleted_count)

    }
}
