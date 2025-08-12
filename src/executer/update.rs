use super::filter::eval_condition;
use crate::types::parser_types::Condition;
use crate::types::storage_types::Database;
use crate::types::storage_types::{ColumnType, Value};
use std::collections::HashMap;

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
            let mut missing: Vec<&str> = Vec::new();
            for name in last.keys() {
                if !table.columns.iter().any(|c| c.name == *name) {
                    missing.push(*name);
                }
            }
            if !missing.is_empty() {
                return Err(format!(
                    "There is no column(s) {:?} in table '{}'",
                    missing, table_name
                ));
            }
        }

        // Type-check each assignment
        for (idx, val) in &targets {
            let column = &table.columns[*idx];
            let ok = match (val, &column.column_type) {
                (Value::Int(_),  ColumnType::Int)  => true,
                (Value::Text(_), ColumnType::Text) => true,
                (Value::Bool(_), ColumnType::Bool) => true,
                (Value::Null,   _)                 => true, // NULL allowed
                _ => false,
            };
            if !ok {
                return Err(format!(
                    "Type mismatch for column '{}' (index {})",
                    column.name, idx
                ));
            }
        }

        // Walk rows, apply WHERE, then write values
        for row in table.rows.iter_mut() {
            if let Some(cond) = &filter {
                let keep = eval_condition(cond, row, &table.columns)
                    .map_err(|e| e.to_string())?;
                if !keep {
                    continue;
                }
            }
            for (idx, val) in &targets {
                row.values[*idx] = (*val).clone();
            }
        }

        Ok(())
    }
}
