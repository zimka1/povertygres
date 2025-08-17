use super::join::JoinTableColumn;
use crate::errors::eval_error::{EvalError, EvalResult};
use crate::types::filter_types::CmpOp;
use crate::types::parser_types::{Condition, Operand};
use crate::types::storage_types::{Row, Value, ValueType};

/// Compares two values in strict mode
pub fn cmp_values(op: CmpOp, left: &Value, right: &Value) -> EvalResult<bool> {
    // NULL on the left-hand side → automatically false in strict mode
    if matches!(left, Value::Null) {
        return Ok(false);
    }

    // Ensure types match before comparing
    let lt = left.vtype();
    let rt = right.vtype();
    if lt != rt {
        return Err(EvalError::TypeMismatch {
            left: lt,
            right: rt,
            op,
        });
    }

    // Compare according to type
    use CmpOp::*;
    match (left, right) {
        // Integer comparison
        (Value::Int(a), Value::Int(b)) => Ok(match op {
            Eq => a == b,
            Ne => a != b,
            Lt => a < b,
            Lte => a <= b,
            Gt => a > b,
            Gte => a >= b,
        }),
        // String comparison (lexicographical)
        (Value::Text(a), Value::Text(b)) => Ok(match op {
            Eq => a == b,
            Ne => a != b,
            Lt => a < b,
            Lte => a <= b,
            Gt => a > b,
            Gte => a >= b,
        }),
        // Boolean comparison (only Eq/Ne are valid)
        (Value::Bool(a), Value::Bool(b)) => match op {
            Eq => Ok(a == b),
            Ne => Ok(a != b),
            _ => Err(EvalError::InvalidOpForType {
                ty: ValueType::Bool,
                op,
            }),
        },
        // Fallback: mismatch that slipped through earlier checks
        _ => Err(EvalError::TypeMismatch {
            left: lt,
            right: rt,
            op,
        }),
    }
}

/// Search column index in metadata: alias.col or just col
fn find_col_index(metas: &[JoinTableColumn], alias: Option<&str>, col: &str) -> Option<usize> {
    if let Some(a) = alias {
        return metas
            .iter()
            .position(|c| c.table_alias == a && c.column_name == col);
    }
    // Unqualified name: search uniquely by column name
    let mut idx = None;
    for (i, c) in metas.iter().enumerate() {
        if c.column_name == col {
            if idx.is_some() {
                return None;
            } // ambiguous
            idx = Some(i);
        }
    }
    idx
}

/// Evaluate one operand (column or literal)
fn eval_operand<'a>(
    op: &'a Operand,
    left_row: &'a Row,
    left_cols: &'a Vec<JoinTableColumn>,
    right_row: Option<&'a Row>,
    right_cols: Option<&'a Vec<JoinTableColumn>>,
) -> EvalResult<&'a Value> {
    match op {
        Operand::Column(name) => {
            // Parse alias.col or just col
            let mut parts = name.split('.');
            let (alias_opt, colname) = match (parts.next(), parts.next(), parts.next()) {
                (Some(a), Some(c), None) => (Some(a), c), // alias.col
                (Some(c), None, None) => (None, c),       // col
                _ => return Err(EvalError::UnknownColumn(name.clone())),
            };

            // Try left side
            if let Some(idx) = find_col_index(left_cols, alias_opt, colname) {
                return left_row
                    .values
                    .get(idx)
                    .ok_or(EvalError::Internal("row.values index out of bounds (left)"));
            }
            // Try right side if exists
            if let (Some(rcols), Some(rrow)) = (right_cols, right_row) {
                if let Some(idx) = find_col_index(rcols, alias_opt, colname) {
                    return rrow.values.get(idx).ok_or(EvalError::Internal(
                        "row.values index out of bounds (right)",
                    ));
                }
            }
            Err(EvalError::UnknownColumn(name.clone()))
        }
        Operand::Literal(val) => Ok(val), // return literal value directly
    }
}

/// Evaluate full condition tree for a row (with optional join)
pub fn eval_condition(
    cond: &Condition,
    left_row: &Row,
    left_cols: &Vec<JoinTableColumn>,
    right_row: Option<&Row>,
    right_cols: Option<&Vec<JoinTableColumn>>,
) -> EvalResult<bool> {
    match cond {
        Condition::Cmp(op, lhs, rhs) => {
            let lv = eval_operand(lhs, left_row, left_cols, right_row, right_cols)?;
            let rv = eval_operand(rhs, left_row, left_cols, right_row, right_cols)?;
            cmp_values(*op, lv, rv) // do actual comparison
        }
        Condition::And(a, b) => {
            let la = eval_condition(a, left_row, left_cols, right_row, right_cols)?;
            if !la {
                return Ok(false);
            } // short-circuit
            eval_condition(b, left_row, left_cols, right_row, right_cols)
        }
        Condition::Or(a, b) => {
            let la = eval_condition(a, left_row, left_cols, right_row, right_cols)?;
            if la {
                return Ok(true);
            } // short-circuit
            eval_condition(b, left_row, left_cols, right_row, right_cols)
        }
        Condition::Not(x) => {
            let v = eval_condition(x, left_row, left_cols, right_row, right_cols)?;
            Ok(!v)
        }
    }
}
