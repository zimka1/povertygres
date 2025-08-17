use crate::executer::filter::eval_condition;
use crate::types::parser_types::{FromItem, JoinKind};
use crate::types::storage_types::{Database, Row, Value};
use std::collections::HashMap;

/// Metadata for a single column in a join result
#[derive(Clone, Debug)]
pub struct JoinTableColumn {
    pub table_alias: String,
    pub column_name: String,
}

/// Result of a join: columns metadata + rows
#[derive(Clone, Debug)]
pub struct JoinTable {
    pub columns: Vec<JoinTableColumn>,
    pub rows: Vec<Row>,
}

impl Database {
    /// Recursively build a JoinTable from a FromItem tree
    pub fn collect_join_table(
        &self,
        join_struct: FromItem,
        aliases: &HashMap<String, String>,
    ) -> Result<JoinTable, String> {
        match join_struct {
            FromItem::Join {
                left,
                right,
                kind,
                on,
            } => {
                // Recursively collect left and right sides
                let left_item = self.collect_join_table(*left, aliases)?;
                let right_item = self.collect_join_table(*right, aliases)?;

                // Columns = concatenation of left + right metadata
                let mut columns =
                    Vec::with_capacity(left_item.columns.len() + right_item.columns.len());
                columns.extend(left_item.columns.iter().cloned());
                columns.extend(right_item.columns.iter().cloned());

                // Build rows for join
                let mut rows = Vec::new();

                for lrow in &left_item.rows {
                    let mut matched = false;

                    for rrow in &right_item.rows {
                        let keep = eval_condition(
                            &on,
                            lrow,
                            &left_item.columns,
                            Some(rrow),
                            Some(&right_item.columns),
                        )
                        .map_err(|e| e.to_string())?;

                        if keep {
                            matched = true;
                            // Concatenate row values (left + right)
                            let mut vals =
                                Vec::with_capacity(lrow.values.len() + rrow.values.len());
                            vals.extend(lrow.values.iter().cloned());
                            vals.extend(rrow.values.iter().cloned());
                            rows.push(Row { values: vals });
                        }
                    }

                    // For LEFT JOIN: keep left row even if no match, pad right with NULLs
                    if !matched && matches!(kind, JoinKind::Left) {
                        let mut vals =
                            Vec::with_capacity(lrow.values.len() + right_item.columns.len());
                        vals.extend(lrow.values.iter().cloned());
                        vals.extend(std::iter::repeat(Value::Null).take(right_item.columns.len()));
                        rows.push(Row { values: vals });
                    }
                }

                Ok(JoinTable { columns, rows })
            }

            FromItem::Table(table_name) => {
                // Lookup real table
                let table = self
                    .tables
                    .get(&table_name)
                    .ok_or_else(|| format!("Table '{}' doesn't exist", &table_name))?;

                // Find alias for this real table, if provided (aliases: alias -> real_name)
                let alias = aliases
                    .iter()
                    .find_map(|(a, real)| {
                        if real == &table_name {
                            Some(a.clone())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(table_name.clone());

                // Build column metadata
                let columns = table
                    .columns
                    .iter()
                    .map(|c| JoinTableColumn {
                        table_alias: alias.clone(),
                        column_name: c.name.clone(),
                    })
                    .collect();

                Ok(JoinTable {
                    columns,
                    rows: table.rows.clone(),
                })
            }
        }
    }
}
