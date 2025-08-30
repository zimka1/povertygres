use crate::errors::engine_error::EngineError;
use crate::page::heap_file::HeapFile;
use crate::types::storage_types::{Database, ForeignKeyConstraint};
use crate::types::storage_types::{Column, Table};

impl Database {
    // Adds a new table to the database
    pub fn create_table(
        &mut self,
        name: &str,
        columns: Vec<Column>,
        heap_file: HeapFile,
        primary_key: Option<String>,
        foreign_keys: Vec<ForeignKeyConstraint>
    ) -> Result<(), EngineError> {
        // Check if table already exists
        if self.tables.contains_key(name) {
            return Err(EngineError::Database(format!(
                "Table '{}' already exists",
                name
            )));
        }

        let table = Table {
            name: name.to_string(),
            columns,
            rows: Vec::new(),
            heap: heap_file,
            primary_key,
            foreign_keys
        };

        // Insert table into database
        self.tables.insert(name.to_string(), table);

        Ok(())
    }
}
