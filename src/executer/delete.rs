use crate::executer::filter::eval_condition;
use crate::types::parser_types::Condition;
use crate::types::storage_types::Database;

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
    
        let before_len = table.rows.len();
    
        if let Some(cond) = &filter {
            // Remove rows that match the filter (retain returns rows to KEEP)
            // If eval fails, capture the error and return it after.
            let mut err: Option<String> = None;
            table.rows.retain(|row| {
                match eval_condition(cond, row, &table.columns) {
                    Ok(matches) => !matches, // keep only non-matching rows
                    Err(e) => { err = Some(e.to_string()); true } // keep row; report error later
                }
            });
            if let Some(e) = err {
                return Err(e);
            }
        }
    
        Ok(before_len - table.rows.len())
    }    
}
