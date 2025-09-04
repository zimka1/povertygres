use crate::executer::filter::eval_condition;
use crate::executer::join::{JoinTable, JoinTableColumn};
use crate::types::filter_types::CmpOp;
use crate::types::parser_types::{Condition, Operand};
use crate::types::storage_types::{Column, Database, Row, Table, Value};
use std::ops::Bound;

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

fn extract_eq_conditions(cond: &Condition) -> Option<Vec<(String, Value)>> {
    match cond {
        Condition::Cmp(CmpOp::Eq, Operand::Column(c), Operand::Literal(v)) => {
            Some(vec![(c.clone(), v.clone())])
        }
        Condition::And(left, right) => {
            let mut out = Vec::new();
            if let Some(mut l) = extract_eq_conditions(left) {
                out.append(&mut l);
            } else {
                return None;
            }
            if let Some(mut r) = extract_eq_conditions(right) {
                out.append(&mut r);
            } else {
                return None;
            }
            Some(out)
        }
        _ => None,
    }
}

fn extract_range_condition(cond: &Condition) -> Option<(String, Bound<Vec<Value>>, Bound<Vec<Value>>)> {
    match cond {
        Condition::Cmp(op, Operand::Column(c), Operand::Literal(v)) => {
            let key = vec![v.clone()];
            match op {
                CmpOp::Gt  => Some((c.clone(), Bound::Excluded(key.clone()), Bound::Unbounded)),
                CmpOp::Gte => Some((c.clone(), Bound::Included(key.clone()), Bound::Unbounded)),
                CmpOp::Lt  => Some((c.clone(), Bound::Unbounded, Bound::Excluded(key.clone()))),
                CmpOp::Lte => Some((c.clone(), Bound::Unbounded, Bound::Included(key.clone()))),
                _ => None,
            }
        }
        _ => None,
    }
}

impl Database {
    fn try_index_lookup(
        &self,
        table: &Table,
        filter: &Option<Condition>,
    ) -> Result<Option<Vec<Row>>, String> {
        if let Some(cols_vals) = filter.as_ref().and_then(|c| extract_eq_conditions(c)) {
            let filter_cols: Vec<String> = cols_vals.iter().map(|(c, _)| c.clone()).collect();
    
            for idx in self.indexes.values() {
                if idx.table == table.name && idx.columns == filter_cols {
                    let key: Vec<Value> = cols_vals.iter().map(|(_, v)| v.clone()).collect();
                    if let Some(positions) = idx.search_eq(&key) {
                        let mut rows = Vec::new();
                        for (page_no, slot_no) in positions {
                            if let Some(row) =
                                table.heap.get_tuple(*page_no as u32, *slot_no, &table.columns)
                            {
                                rows.push(row);
                            }
                        }
                        return Ok(Some(rows));
                    }
                }
            }
        }
        if let Some((col, lower, upper)) = filter.as_ref().and_then(|c| extract_range_condition(c)) {
            for idx in self.indexes.values() {
                if idx.table == table.name && idx.columns.len() == 1 && idx.columns[0] == col {
                    let positions = idx.search_range(lower, upper);
                    let mut rows = Vec::new();
                    for (page_no, slot_no) in positions {
                        if let Some(row) = table.heap.get_tuple(page_no as u32, slot_no, &table.columns) {
                            rows.push(row);
                        }
                    }
                    return Ok(Some(rows));
                }
            }
        }        
        Ok(None)
    }
    

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
                if let Some(rows) = self.try_index_lookup(t, &filter)? {
                    JoinTable { 
                        columns: t.columns.iter().map(|c| JoinTableColumn {
                            table_alias: name.clone(),
                            column_name: c.name.clone(),
                        }).collect(),
                        rows,
                    }
                } else {
                    // For plain table, use its name as alias
                    phys_to_join(t, name)
                }
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
