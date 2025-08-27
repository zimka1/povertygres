use super::filter::eval_condition;
use crate::executer::join::JoinTableColumn;
use crate::types::parser_types::Condition;
use crate::types::storage_types::{Column, Database};
use crate::types::storage_types::{ColumnType, Value};
use std::collections::HashMap;

/// Build column metadata for a single table
fn single_meta(table_name: &str, cols: &Vec<Column>) -> Vec<JoinTableColumn> {
    cols.iter()
        .map(|c| JoinTableColumn {
            table_alias: table_name.to_string(),
            column_name: c.name.clone(),
        })
        .collect()
}

/// Normalize a possibly qualified column name ("table.col" → "col")
fn normalize_col<'a>(name: &'a str, table_name: &str) -> Result<&'a str, String> {
    if let Some((t, c)) = name.split_once('.') {
        if t == table_name {
            Ok(c)
        } else {
            Err(format!(
                "Qualified column '{}' does not belong to table '{}'",
                name, table_name
            ))
        }
    } else {
        Ok(name)
    }
}

impl Database {
    /// Updates rows in a table. The last assignment for the same column wins.
    pub fn update(
        &mut self,
        table_name: &str,
        parsed_columns: Vec<String>,
        parsed_values: Vec<Value>,
        filter: Option<Condition>,
    ) -> Result<(), String> {
        // Find the table
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        // Column/value counts must match
        if parsed_columns.len() != parsed_values.len() {
            return Err(format!(
                "Expected {} values for specified columns, but got {}",
                parsed_columns.len(),
                parsed_values.len()
            ));
        }

        // Keep only the last assignment per column
        let mut last: HashMap<&str, &Value> = HashMap::new();
        for (name, val) in parsed_columns.iter().zip(parsed_values.iter()) {
            last.insert(name.as_str(), val);
        }

        // Map column names to schema indexes
        let mut targets: Vec<(usize, &Value)> = Vec::with_capacity(last.len());
        for (i, col) in table.columns.iter().enumerate() {
            if let Some(v) = last.get(col.name.as_str()) {
                targets.push((i, v));
            }
        }

        // Verify all provided columns exist
        if targets.len() != last.len() {
            let mut missing = Vec::new();
            for k in last.keys() {
                match normalize_col(k, table_name) {
                    Ok(col) if table.columns.iter().any(|c| c.name == col) => {}
                    _ => missing.push(*k),
                }
            }
            if !missing.is_empty() {
                return Err(format!(
                    "Unknown column(s) {:?} for table '{}'",
                    missing, table_name
                ));
            }
        }

        // Type-check each assignment
        for (idx, val) in &targets {
            let column = &table.columns[*idx];
            let ok = match (val, &column.column_type) {
                (Value::Int(_), ColumnType::Int) => true,
                (Value::Text(_), ColumnType::Text) => true,
                (Value::Bool(_), ColumnType::Bool) => true,
                (Value::Null, _) => true, // NULL allowed
                _ => false,
            };
            if !ok {
                return Err(format!(
                    "Type mismatch for column '{}' (index {})",
                    column.name, idx
                ));
            }
        }

        // Build metadata for evaluation
        let metas = single_meta(table_name, &table.columns);

        // Walk rows and apply updates
        for (page_no, slot_no, mut row) in table.heap.scan_all_with_pos(&table.columns) {
            if let Some(cond) = &filter {
                // Apply WHERE condition
                let keep =
                    eval_condition(cond, &row, &metas, None, None).map_err(|e| e.to_string())?;
                if !keep {
                    continue;
                }
            }
            // Write new values
            for (idx, val) in &targets {
                row.values[*idx] = (*val).clone();
            }
            table.heap.update_row(page_no, slot_no, row)?;
        }

        Ok(())
    }
}
