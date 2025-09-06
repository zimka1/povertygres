use crate::types::storage_types::{Column, Database, Table};
use crate::types::storage_types::{Row, Value};

pub fn build_key(
    index_columns: &Vec<String>,
    table_columns: &Vec<Column>,
    values: &Vec<Value>,
    table_name: &str,
) -> Result<Vec<Value>, String> {
    let mut key = Vec::new();
    for col in index_columns {
        let col_idx = table_columns
            .iter()
            .position(|c| c.name == *col)
            .ok_or_else(|| format!("Index column '{}' not found in '{}'", col, table_name))?;
        key.push(values[col_idx].clone());
    }
    Ok(key)
}

pub fn validate_foreign_keys(
    db: &Database,
    table: &Table,
    row_values: &Vec<Value>,
) -> Result<(), String> {
    for fk in &table.foreign_keys {
        let mut local_values = Vec::new();
        for col_name in &fk.local_columns {
            let Some(idx) = table.columns.iter().position(|c| c.name == *col_name) else {
                return Err(format!(
                    "Foreign key error: local column '{}' not found in '{}'",
                    col_name, table.name
                ));
            };
            local_values.push(row_values[idx].clone());
        }

        if local_values.iter().all(|v| matches!(v, Value::Null)) {
            continue;
        }

        let parent_table = db.tables.get(&fk.referenced_table).ok_or_else(|| {
            format!(
                "Foreign key error: referenced table '{}' not found",
                fk.referenced_table
            )
        })?;

        let mut ref_indices = Vec::new();
        for ref_col in &fk.referenced_columns {
            let Some(idx) = parent_table.columns.iter().position(|c| c.name == *ref_col) else {
                return Err(format!(
                    "Foreign key error: referenced column '{}' not found in '{}'",
                    ref_col, fk.referenced_table
                ));
            };
            ref_indices.push(idx);
        }

        let parent_rows: Vec<Row> = parent_table
            .heap
            .scan_all(&parent_table.columns)
            .into_iter()
            .map(|(_, _, _, row)| row)
            .collect();
        let mut found = false;
        for prow in parent_rows {
            if local_values
                .iter()
                .zip(ref_indices.iter())
                .all(|(lv, ri)| &prow.values[*ri] == lv)
            {
                found = true;
                break;
            }
        }

        if !found {
            return Err(format!(
                "violates foreign key constraint: {:?} -> {}({:?})",
                fk.local_columns, fk.referenced_table, fk.referenced_columns
            ));
        }
    }
    Ok(())
}

pub fn ensure_not_referenced(
    db: &Database,
    table_name: &str,
    row_values: &Vec<Value>,
) -> Result<(), String> {
    let table = db.tables.get(table_name).unwrap();

    for (other_name, other_table) in &db.tables {
        if other_name == table_name {
            continue;
        }
        for fk in &other_table.foreign_keys {
            if fk.referenced_table == table_name {
                // соберём значения из удаляемой строки по ref_columns
                let mut ref_values = Vec::new();
                for ref_col in &fk.referenced_columns {
                    let Some(idx) = table.columns.iter().position(|c| c.name == *ref_col) else {
                        return Err(format!(
                            "Foreign key error: column '{}' not found in '{}'",
                            ref_col, table_name
                        ));
                    };
                    ref_values.push(row_values[idx].clone());
                }

                let child_rows: Vec<Row> = other_table
                    .heap
                    .scan_all(&other_table.columns)
                    .into_iter()
                    .map(|(_, _, _, row)| row)
                    .collect();
                for child in child_rows {
                    let mut match_all = true;
                    for (local_col, ref_val) in fk.local_columns.iter().zip(ref_values.iter()) {
                        let Some(idx) = other_table
                            .columns
                            .iter()
                            .position(|c| c.name == *local_col)
                        else {
                            return Err(format!(
                                "Foreign key error: column '{}' not found in '{}'",
                                local_col, other_name
                            ));
                        };
                        if &child.values[idx] != ref_val {
                            match_all = false;
                            break;
                        }
                    }
                    if match_all {
                        return Err(format!(
                            "delete from '{}' violates foreign key constraint in '{}': {:?} -> {}({:?})",
                            table_name,
                            other_name,
                            fk.local_columns,
                            fk.referenced_table,
                            fk.referenced_columns
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
