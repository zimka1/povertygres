use crate::types::storage_types::Database;
use std::collections::{HashSet};
use crate::executer::filter::eval_condition;
use crate::types::{parse_types::Condition, storage_types::Row};

impl Database {
    // Selects rows from a table with specified column names
    pub fn select(&self, table_name: &str, column_names: &Vec<String>, filter: Option<Condition>) -> Result<Vec<Row>, String> {
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
        for name in column_names {
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

        // Prepare output
        let mut result = Vec::new();

        // Iterate over all rows
        for row in &table.rows {
            // If filter exists, check condition
            if let Some(cond) = &filter {
                let keep = eval_condition(cond, row, &table.columns)
                    .map_err(|e| e.to_string())?;
                if !keep {
                    continue; // skip this row
                }
            }

            // Build row with only selected columns
            let filtered_values = row
                .values
                .iter()
                .enumerate()
                .filter(|(i, _)| needed_value_indexes.contains(i))
                .map(|(_, v)| v.clone())
                .collect();

            result.push(Row { values: filtered_values });
        }

        Ok(result)
    }
}