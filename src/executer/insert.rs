use crate::types::parser_types::Condition;
use crate::types::storage_types::Database;
use crate::types::storage_types::{ColumnType, Row, Value};

impl Database {
    // Inserts a new row into a table
    pub fn insert_into(
        &mut self,
        table_name: &str,
        column_names: Option<Vec<String>>, // Optional: user can specify columns
        values: Vec<Value>,                // Values to insert
    ) -> Result<(), String> {
        // Get the target table
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        // Reorder or validate values if column names are specified
        let final_values: Vec<Value> = if let Some(ref col_names) = column_names {
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

            // Fill row with Nulls, then replace values for matched columns
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

        // Insert row into the table
        table.rows.push(Row {
            values: final_values,
        });
        Ok(())
    }
}
