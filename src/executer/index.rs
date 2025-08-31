use crate::errors::engine_error::EngineError;
use crate::types::b_tree::BTreeIndex;
use crate::types::storage_types::Database;

impl Database {
    pub fn create_index(
        &mut self,
        name: &str,
        table_name: &str,
        columns: Vec<String>,
    ) -> Result<(), EngineError> {
        if self.indexes.contains_key(name) {
            return Err(EngineError::Database(format!(
                "Index '{}' already exists",
                name
            )));
        }

        let table = self.tables.get(table_name).ok_or_else(|| {
            EngineError::Database(format!("Table '{}' does not exist", table_name))
        })?;

        for col in &columns {
            if !table.columns.iter().any(|c| c.name == *col) {
                return Err(EngineError::Database(format!(
                    "Column '{}' does not exist in '{}'",
                    col, table_name
                )));
            }
        }

        let idx = BTreeIndex::new(name.to_string(), table_name.to_string(), columns);
        self.indexes.insert(name.to_string(), idx);

        Ok(())
    }
}
