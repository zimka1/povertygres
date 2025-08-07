use crate::types::{ColumnType, Value};
use std::collections::{HashMap, HashSet};

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

    pub fn insert_into(
        &mut self,
        table_name: &str,
        column_names: Option<Vec<String>>,
        values: Vec<Value>,
    ) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;
    
        let final_values: Vec<Value> = if let Some(ref col_names) = column_names {
            if col_names.len() != values.len() {
                return Err(format!(
                    "Expected {} values for specified columns, but got {}",
                    col_names.len(),
                    values.len()
                ));
            }
    
            for name in col_names {
                if !table.columns.iter().any(|c| c.name == *name) {
                    return Err(format!(
                        "There is no '{}' column in table '{}'",
                        name, table_name
                    ));
                }
            }

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
            if values.len() != table.columns.len() {
                return Err(format!(
                    "Expected {} values, but got {}",
                    table.columns.len(),
                    values.len()
                ));
            }
    
            values
        };
    
        for (i, (value, column)) in final_values.iter().zip(&table.columns).enumerate() {
            let compatible = match (value, &column.column_type) {
                (Value::Int(_), ColumnType::Int) => true,
                (Value::Text(_), ColumnType::Text) => true,
                (Value::Bool(_), ColumnType::Bool) => true,
                (Value::Null, _) => true,
                _ => false,
            };
    
            if !compatible {
                return Err(format!(
                    "Type mismatch at column {} ('{}')",
                    i, column.name
                ));
            }
        }
    
        table.rows.push(Row { values: final_values });
        Ok(())
    }
    

    pub fn select(&self, table_name: &str, column_names: Vec<String>) -> Result<Vec<Row>, String> {
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{}' doesn't exist", table_name))?;

        let column_name_set: HashSet<_> = column_names.iter().collect();

        if let Some(first) = column_names.get(0) {
            if first == "*" {
                return Ok(table.rows.clone());
            }
        }
    
        for name in &column_names {
            if !table.columns.iter().any(|c| c.name == *name) {
                return Err(format!(
                    "There is no '{}' column in table '{}'",
                    name, table_name
                ));
            }
        }

        let mut needed_value_indexes = Vec::new();

        for (i, column) in table.columns.iter().enumerate() {
            if column_name_set.contains(&column.name) {
                needed_value_indexes.push(i);
            }
        }

        let rows: Vec<Row> = table.rows.iter().map(|row| {
            let filtered_values = row
            .values
            .iter()
            .enumerate()
            .filter(|(i, _)| needed_value_indexes.contains(i))
            .map(|(_, v)| v.clone())
            .collect();

            Row { values: filtered_values }
        }).collect();

        Ok(rows)
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


pub fn print_table(columns: &[Column], rows: &[Row]) {
    let mut widths: Vec<usize> = columns
        .iter()
        .map(|col| col.name.len())
        .collect();

    for row in rows {
        for (i, value) in row.values.iter().enumerate() {
            let s = match value {
                Value::Int(v) => v.to_string(),
                Value::Text(s) => s.clone(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
            };
            if s.len() > widths[i] {
                widths[i] = s.len();
            }
        }
    }

    let sep_line = widths
        .iter()
        .map(|w| format!("+{}+", "-".repeat(*w + 2)))
        .collect::<Vec<_>>()
        .join("")
        .replace("++", "+"); // чтобы не было двойных плюсов
    let sep_line = format!("+{}+", sep_line.trim_matches('+'));

    println!("{}", sep_line);

    let header = columns
        .iter()
        .zip(&widths)
        .map(|(col, w)| format!("| {:width$} ", col.name, width = *w))
        .collect::<String>() + "|";
    println!("{}", header);
    println!("{}", sep_line);

    for row in rows {
        let line = row
            .values
            .iter()
            .zip(&widths)
            .map(|(val, w)| {
                let s = match val {
                    Value::Int(v) => v.to_string(),
                    Value::Text(s) => s.clone(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "NULL".to_string(),
                };
                format!("| {:width$} ", s, width = *w)
            })
            .collect::<String>() + "|";
        println!("{}", line);
    }

    println!("{}", sep_line);
}
