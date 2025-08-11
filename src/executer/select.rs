use crate::executer::filter::eval_condition;
use crate::types::storage_types::Database;
use crate::types::{parser_types::Condition, storage_types::Row};

impl Database {
    // Selects rows from a table with specified column names
    pub fn select(
        &self,
        table_name: &str,
        column_names: &Vec<String>,
        filter: Option<Condition>,
    ) -> Result<Vec<Row>, String> {
        // Look up the table by name
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        // Detect if this is a "SELECT *"
        let is_star = matches!(column_names.get(0).map(|s| s.as_str()), Some("*"));

        // If not "*", resolve the column names to their indexes
        let mut idxs: Vec<usize> = Vec::new();
        if !is_star {
            for name in column_names {
                let i = table
                    .columns
                    .iter()
                    .position(|c| c.name == *name)
                    .ok_or_else(|| {
                        format!("There is no '{}' column in table '{}'", name, table_name)
                    })?;
                idxs.push(i);
            }
        }

        // Prepare the result set
        let mut result = Vec::new();

        // Iterate over rows
        for row in &table.rows {
            // Apply the WHERE filter if provided
            if let Some(cond) = &filter {
                let keep = eval_condition(cond, row, &table.columns).map_err(|e| e.to_string())?;
                if !keep {
                    continue; // Skip rows that don't match
                }
            }

            // Project columns: either all or the selected ones
            if is_star {
                result.push(Row {
                    values: row.values.clone(),
                });
            } else {
                let projected = idxs.iter().map(|&i| row.values[i].clone()).collect();
                result.push(Row { values: projected });
            }
        }

        // Return the final rows
        Ok(result)
    }
}
