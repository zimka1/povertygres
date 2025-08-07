use crate::types::{ColumnType, Value};
use std::collections::HashMap;

pub struct Database {
    pub tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<(), String> {
        if self.tables.contains_key(name) {
            return Err(format!("Table '{}' already exists", name));
        }

        let table = Table {
            name: name.to_string(),
            columns,
            rows: Vec::new(),
        };

        self.tables.insert(name.to_string(), table);

        Ok(())
    }

    pub fn insert_into(&mut self, table_name: &str, values: Vec<Value>) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        if values.len() != table.columns.len() {
            return Err(format!(
                "Expected {} values, but got {}",
                table.columns.len(),
                values.len()
            ));
        }

        for (i, (value, column)) in values.iter().zip(&table.columns).enumerate() {
            let compatible = match (value, &column.column_type) {
                (Value::Int(_), ColumnType::Int) => true,
                (Value::Text(_), ColumnType::Text) => true,
                (Value::Bool(_), ColumnType::Bool) => true,
                (Value::Null, _) => true,
                _ => false,
            };
            if !compatible {
                return Err(format!("Type mismatch at column {} ('{}')", i, column.name));
            }
        }

        let row = Row { values };
        table.rows.push(row);

        Ok(())
    }

    pub fn select_all(&self, table_name: &str) -> Result<&[Row], String> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        Ok(&table.rows)
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}
