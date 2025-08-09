use crate::types::storage_types::{Column, ColumnType, Row, Table, Value};
use std::collections::{HashMap, HashSet};

pub struct Database {
    // Stores tables by their name
    pub tables: HashMap<String, Table>,
}

impl Database {
    // Creates a new empty database
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    // Adds a new table to the database
    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<(), String> {
        // Check if table already exists
        if self.tables.contains_key(name) {
            return Err(format!("Table '{}' already exists", name));
        }

        let table = Table {
            name: name.to_string(),
            columns,
            rows: Vec::new(),
        };

        // Insert table into database
        self.tables.insert(name.to_string(), table);

        Ok(())
    }

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

    // Selects rows from a table with specified column names
    pub fn select(&self, table_name: &str, column_names: Vec<String>) -> Result<Vec<Row>, String> {
        // Get the table
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        let column_name_set: HashSet<_> = column_names.iter().collect();

        // Special case: SELECT *
        if let Some(first) = column_names.get(0) {
            if first == "*" {
                return Ok(table.rows.clone());
            }
        }

        // Check if all selected columns exist
        for name in &column_names {
            if !table.columns.iter().any(|c| c.name == *name) {
                return Err(format!(
                    "There is no '{}' column in table '{}'",
                    name, table_name
                ));
            }
        }

        // Get indexes of requested columns
        let mut needed_value_indexes = Vec::new();
        for (i, column) in table.columns.iter().enumerate() {
            if column_name_set.contains(&column.name) {
                needed_value_indexes.push(i);
            }
        }

        // Build new rows with only selected columns
        let rows: Vec<Row> = table
            .rows
            .iter()
            .map(|row| {
                let filtered_values = row
                    .values
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| needed_value_indexes.contains(i))
                    .map(|(_, v)| v.clone())
                    .collect();

                Row {
                    values: filtered_values,
                }
            })
            .collect();

        Ok(rows)
    }
}
