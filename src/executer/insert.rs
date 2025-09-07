use crate::executer::help_functions::validate_foreign_keys;
use crate::types::page_types::TupleHeader;
use crate::types::storage_types::Database;
use crate::types::storage_types::{ColumnType, Row, Value};
use crate::types::transaction_types::Snapshot;

impl Database {
    // Inserts a new row into a table
    pub fn insert_into(
        &mut self,
        table_name: &str,
        column_names: Option<Vec<String>>, // Optional: user can specify columns
        values: Vec<Value>,                // Values to insert
        xid: u32,
        snapshot: &Snapshot
    ) -> Result<(), String> {
        // Get the target table (immutable for now)
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        // Reorder or validate values if column names are specified
        let mut final_values: Vec<Value> = if let Some(ref col_names) = column_names {
            // Check if column count matches
            if col_names.len() != values.len() {
                return Err(format!(
                    "Expected {} values for specified columns, but got {}",
                    col_names.len(),
                    values.len()
                ));
            }

            // Check if all columns exist
            for name in col_names {
                if !table.columns.iter().any(|c| c.name == *name) {
                    return Err(format!(
                        "There is no '{}' column in table '{}'",
                        name, table_name
                    ));
                }
            }

            // Fill row with NULLs, then replace values for matched columns
            let mut row = vec![Value::Null; table.columns.len()];

            for (col_name, value) in col_names.iter().zip(values.iter()) {
                let Some(index) = table.columns.iter().position(|c| c.name == *col_name) else {
                    return Err(format!(
                        "Unexpected error: column '{}' disappeared",
                        col_name
                    ));
                };
                row[index] = value.clone();
            }

            row
        } else {
            // No column names specified — use positionally
            if values.len() != table.columns.len() {
                return Err(format!(
                    "Expected {} values, but got {}",
                    table.columns.len(),
                    values.len()
                ));
            }

            values
        };

        // Type checking for each value against column type
        for (i, (value, column)) in final_values.iter().zip(&table.columns).enumerate() {
            let compatible = match (value, &column.column_type) {
                (Value::Int(_), ColumnType::Int) => true,
                (Value::Text(_), ColumnType::Text) => true,
                (Value::Bool(_), ColumnType::Bool) => true,
                (Value::Null, _) => true, // NULL allowed in any column
                _ => false,
            };

            if !compatible {
                return Err(format!("Type mismatch at column {} ('{}')", i, column.name));
            }
        }

        // Apply defaults + check NOT NULL constraints
        for (i, column) in table.columns.iter().enumerate() {
            if let Value::Null = final_values[i] {
                if let Some(def) = &column.default {
                    final_values[i] = def.clone();
                } else if column.not_null {
                    return Err(format!("Column '{}' cannot be NULL", column.name));
                }
            }
        }

        // Enforce primary key uniqueness
        if let Some(pk_name) = &table.primary_key {
            let Some(pk_idx) = table.columns.iter().position(|c| c.name == *pk_name) else {
                return Err(format!("Primary key column '{}' not found", pk_name));
            };

            let pk_val = &final_values[pk_idx];
            if let Value::Null = pk_val {
                return Err(format!("Primary key '{}' cannot be NULL", pk_name));
            }

            let existing_rows: Vec<(Row, TupleHeader)> = table
                .heap
                .scan_all(&table.columns)
                .into_iter()
                .map(|(_, _, header, row)| (row, header))
                .collect();
            for (row, header) in existing_rows {
                if header.is_visible(xid, snapshot, &self.transaction_manager) {
                    if row.values[pk_idx] == *pk_val {
                        return Err(format!(
                            "duplicate key value violates primary key constraint on '{}'",
                            pk_name
                        ));
                    }
                }
            }
        }

        // Foreign key validation
        validate_foreign_keys(self, table, &final_values)?;

        // Now reopen table mutably to perform the actual insert
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        // Insert row into the table's heap
        let row = Row {
            values: final_values,
        };

        let (page_no, slot_no) = table.heap.insert_row(row.clone(), xid)?;

        // Update all indexes for this table
        for idx in self.indexes.values_mut() {
            if idx.table == table_name {
                let mut key = Vec::new();
                for col in &idx.columns {
                    let col_idx = table
                        .columns
                        .iter()
                        .position(|c| c.name == *col)
                        .ok_or_else(|| {
                            format!("Index column '{}' not found in '{}'", col, table_name)
                        })?;
                    key.push(row.values[col_idx].clone());
                }
                idx.insert(key, (page_no, slot_no));
            }
        }

        Ok(())
    }
}
