use crate::executer::filter::eval_condition;
use crate::executer::join::JoinTableColumn;
use crate::types::parser_types::Condition;
use crate::types::storage_types::{Column, Database};

/// Build column metadata for a single table
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
        // Find the target table mutably (needed for deletions)
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        // If no filter: remove all rows (unsafe, bypasses FK checks)
        if filter.is_none() {
            let n = table.rows.len();
            table.rows.clear();
            return Ok(n);
        }

        // Immutable borrow for scanning and metadata
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        let mut deleted_count = 0;
        let metas = single_meta(table_name, &table.columns);

        if let Some(cond) = &filter {
            let rows = table.heap.scan_all_with_pos(&table.columns);

            for (page_no, slot_no, row) in rows {
                match eval_condition(cond, &row, &metas, None, None) {
                    Ok(true) => {
                        // For each other table, check if any FK references this row
                        for (other_name, other_table) in &self.tables {
                            if other_name == table_name {
                                continue;
                            }
                            for fk in &other_table.foreign_keys {
                                if fk.referenced_table == table_name {
                                    // Collect referenced values from the row being deleted
                                    let mut ref_values = Vec::new();
                                    for ref_col in &fk.referenced_columns {
                                        let Some(idx) = table.columns.iter().position(|c| c.name == *ref_col) else {
                                            return Err(format!(
                                                "Foreign key error: column '{}' not found in '{}'",
                                                ref_col, table_name
                                            ));
                                        };
                                        ref_values.push(row.values[idx].clone());
                                    }

                                    // Scan child table for matches
                                    let child_rows = other_table.heap.scan_all(&other_table.columns);
                                    for child in child_rows {
                                        let mut match_all = true;
                                        for (local_col, ref_val) in fk.local_columns.iter().zip(ref_values.iter()) {
                                            let Some(idx) = other_table.columns.iter().position(|c| c.name == *local_col) else {
                                                return Err(format!(
                                                    "Foreign key error: column '{}' not found in '{}'",
                                                    local_col, other_name
                                                ));
                                            };
                                            if &child.values[idx] != ref_val {
                                                match_all = false;
                                                break;
                                            }
                                        }
                                        if match_all {
                                            return Err(format!(
                                                "delete from '{}' violates foreign key constraint in '{}': {:?} -> {}({:?})",
                                                table_name, other_name, fk.local_columns, fk.referenced_table, fk.referenced_columns
                                            ));
                                        }
                                    }
                                }
                            }
                        }

                        // If all FK checks passed → delete row
                        table.heap.delete_at(page_no, slot_no)?;
                        deleted_count += 1;
                    }
                    Ok(false) => { /* keep row */ }
                    Err(e) => return Err(e.to_string()),
                }
            }
        }

        Ok(deleted_count)
    }
}
