use crate::types::storage_types::Database;
use crate::types::storage_types::{Column, Table};

impl Database {
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
}
