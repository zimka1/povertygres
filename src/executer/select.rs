use crate::executer::filter::eval_condition;
use crate::executer::join::{JoinTable, JoinTableColumn};
use crate::types::parser_types::Condition;
use crate::types::storage_types::{Column, Database, Row, Table};

/// Argument to SELECT: either a table name or a prebuilt JoinTable
pub enum TableArg {
    TableName(String),
    JoinTable(JoinTable),
}

/// Convert physical table into JoinTable with alias
fn phys_to_join(table: &Table, alias: &str) -> JoinTable {
    let cols = table
        .columns
        .iter()
        .map(|c: &Column| JoinTableColumn {
            table_alias: alias.to_string(),
            column_name: c.name.clone(),
        })
        .collect();

    let rows = table.heap.scan_all(&table.columns);

    JoinTable {
        columns: cols,
        rows,
    }
}

/// Find index of column in metadata by name or alias.col
fn find_idx(meta: &[JoinTableColumn], name: &str) -> Result<usize, String> {
    let mut parts = name.split('.');
    match (parts.next(), parts.next(), parts.next()) {
        // Qualified name: alias.col
        (Some(a), Some(c), None) => meta
            .iter()
            .position(|m| m.table_alias == a && m.column_name == c)
            .ok_or_else(|| format!("Unknown column '{}.{}'", a, c)),
        // Unqualified name: must be unique
        (Some(c), None, None) => {
            let mut idx: Option<usize> = None;
            for (i, m) in meta.iter().enumerate() {
                if m.column_name == c {
                    if idx.is_some() {
                        return Err(format!("Ambiguous column '{}'", c));
                    }
                    idx = Some(i);
                }
            }
            idx.ok_or_else(|| format!("Unknown column '{}'", c))
        }
        // Invalid format
        _ => Err(format!("Bad column reference '{}'", name)),
    }
}

impl Database {
    /// Execute SELECT on a single table or join
    pub fn select(
        &self,
        table_arg: &TableArg,
        column_names: &Vec<String>,
        filter: Option<Condition>,
    ) -> Result<(Vec<JoinTableColumn>, Vec<Row>), String> {
        // 1) Normalize input into JoinTable
        let exec: JoinTable = match table_arg {
            TableArg::JoinTable(jt) => jt.clone(),
            TableArg::TableName(name) => {
                let t = self
                    .tables
                    .get(name)
                    .ok_or_else(|| format!("Table '{}' doesn't exist", name))?;
                // For plain table, use its name as alias
                phys_to_join(t, name)
            }
        };

        let mut rows: Vec<Row> = Vec::new();
        let is_star = matches!(column_names.get(0).map(|s| s.as_str()), Some("*"));

        // Resolve selected columns to indexes
        let idxs: Option<Vec<usize>> = if is_star {
            None
        } else {
            let mut v = Vec::with_capacity(column_names.len());
            for name in column_names {
                v.push(find_idx(&exec.columns, name)?);
            }
            Some(v)
        };

        // Filter and project rows
        for r in &exec.rows {
            if let Some(cond) = &filter {
                let keep = eval_condition(
                    cond,
                    r,
                    &exec.columns, // metadata with aliases
                    None,
                    None,
                )
                .map_err(|e| e.to_string())?;
                if !keep {
                    continue;
                }
            }

            if is_star {
                rows.push(Row {
                    values: r.values.clone(),
                });
            } else {
                let mut vals = Vec::with_capacity(idxs.as_ref().unwrap().len());
                for &i in idxs.as_ref().unwrap() {
                    vals.push(r.values[i].clone());
                }
                rows.push(Row { values: vals });
            }
        }

        Ok((exec.columns, rows))
    }
}
