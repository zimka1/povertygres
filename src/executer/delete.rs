use crate::types::storage_types::Database;
use crate::types::{parser_types::Condition};
use crate::executer::filter::eval_condition;

impl Database {
    // Selects rows from a table with specified column names
    pub fn delete(
        &mut self,
        table_name: &str,
        filter: Option<Condition>,
    ) -> Result<usize, String> {
        // Get the table
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        if filter.is_none() {
            let n = table.rows.len();
            table.rows.clear();
            return Ok(n);
        }

        let deleted = table.rows.len();

        if let Some(cond) = &filter {
            table.rows = table.rows
                .iter()
                .map(|row| {
                    eval_condition(cond, row, &table.columns)
                        .map_err(|e| e.to_string())
                        .map(|keep| (keep, row.clone()))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|(keep, _)| !*keep)
                .map(|(_, row)| row)
                .collect();
        }
        
        Ok(deleted - table.rows.len())
    }
}
