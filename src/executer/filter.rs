use crate::errors::eval_error::{EvalError, EvalResult};
use crate::types::filter_types::CmpOp;
use crate::types::parse_types::Condition;
use crate::types::storage_types::ValueType;
use crate::types::storage_types::{Column, Row, Value};

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

/// Looks up a column's value in a row by name
pub fn lookup_col<'a>(columns: &'a Vec<Column>, row: &'a Row, name: &str) -> EvalResult<&'a Value> {
    // Find column index by name
    let idx = columns
        .iter()
        .position(|c| c.name == *name)
        .ok_or_else(|| EvalError::UnknownColumn(name.to_string()))?;
    // Return the value at the corresponding index in the row
    row.values
        .get(idx)
        .ok_or(EvalError::Internal("row.values index out of bounds"))
}

/// Recursively evaluates a condition against a given row
pub fn eval_condition(cond: &Condition, row: &Row, columns: &Vec<Column>) -> EvalResult<bool> {
    match cond {
        // Equality
        Condition::Eq(col_name, lit) => {
            let lv = lookup_col(columns, row, col_name)?;
            cmp_values(CmpOp::Eq, lv, lit)
        }

        // Not equal
        Condition::Neq(col_name, lit) => {
            let lv = lookup_col(columns, row, col_name)?;
            cmp_values(CmpOp::Ne, lv, lit)
        }

        // Less than
        Condition::Lt(col_name, lit) => {
            let lv = lookup_col(columns, row, col_name)?;
            cmp_values(CmpOp::Lt, lv, lit)
        }

        // Less than or equal
        Condition::Lte(col_name, lit) => {
            let lv = lookup_col(columns, row, col_name)?;
            cmp_values(CmpOp::Lte, lv, lit)
        }

        // Greater than
        Condition::Gt(col_name, lit) => {
            let lv = lookup_col(columns, row, col_name)?;
            cmp_values(CmpOp::Gt, lv, lit)
        }

        // Greater than or equal
        Condition::Gte(col_name, lit) => {
            let lv = lookup_col(columns, row, col_name)?;
            cmp_values(CmpOp::Gte, lv, lit)
        }

        // Logical AND (short-circuits if left is false)
        Condition::And(a, b) => {
            let la = eval_condition(a, row, columns)?;
            if !la {
                return Ok(false);
            }
            eval_condition(b, row, columns)
        }

        // Logical OR (short-circuits if left is true)
        Condition::Or(a, b) => {
            let la = eval_condition(a, row, columns)?;
            if la {
                return Ok(true);
            }
            eval_condition(b, row, columns)
        }

        // Logical NOT
        Condition::Not(x) => {
            let v = eval_condition(x, row, columns)?;
            Ok(!v)
        }
    }
}


/// Looks up a column's value either in left or right table
pub fn lookup_col_with_side<'a>(
    name: &str,
    left_cols: &'a Vec<Column>,
    left_row: &'a Row,
    right_cols: &'a Vec<Column>,
    right_row: &'a Row,
) -> EvalResult<&'a Value> {
    if let Some(idx) = left_cols.iter().position(|c| c.name == *name) {
        return left_row
            .values
            .get(idx)
            .ok_or(EvalError::Internal("row.values index out of bounds (left)"));
    }

    if let Some(idx) = right_cols.iter().position(|c| c.name == *name) {
        return right_row
            .values
            .get(idx)
            .ok_or(EvalError::Internal("row.values index out of bounds (right)"));
    }

    Err(EvalError::UnknownColumn(name.to_string()))
}

/// Evaluates a condition in JOIN context (both left & right rows available)
pub fn eval_condition_on(
    cond: &Condition,
    left_row: &Row,
    left_cols: &Vec<Column>,
    right_row: &Row,
    right_cols: &Vec<Column>,
) -> EvalResult<bool> {
    match cond {
        Condition::Eq(col_name, lit) => {
            let lv = lookup_col_with_side(col_name, left_cols, left_row, right_cols, right_row)?;
            cmp_values(CmpOp::Eq, lv, lit)
        }
        Condition::Neq(col_name, lit) => {
            let lv = lookup_col_with_side(col_name, left_cols, left_row, right_cols, right_row)?;
            cmp_values(CmpOp::Ne, lv, lit)
        }
        Condition::Lt(col_name, lit) => {
            let lv = lookup_col_with_side(col_name, left_cols, left_row, right_cols, right_row)?;
            cmp_values(CmpOp::Lt, lv, lit)
        }
        Condition::Lte(col_name, lit) => {
            let lv = lookup_col_with_side(col_name, left_cols, left_row, right_cols, right_row)?;
            cmp_values(CmpOp::Lte, lv, lit)
        }
        Condition::Gt(col_name, lit) => {
            let lv = lookup_col_with_side(col_name, left_cols, left_row, right_cols, right_row)?;
            cmp_values(CmpOp::Gt, lv, lit)
        }
        Condition::Gte(col_name, lit) => {
            let lv = lookup_col_with_side(col_name, left_cols, left_row, right_cols, right_row)?;
            cmp_values(CmpOp::Gte, lv, lit)
        }
        Condition::And(a, b) => {
            let la = eval_condition_on(a, left_row, left_cols, right_row, right_cols)?;
            if !la {
                return Ok(false);
            }
            eval_condition_on(b, left_row, left_cols, right_row, right_cols)
        }
        Condition::Or(a, b) => {
            let la = eval_condition_on(a, left_row, left_cols, right_row, right_cols)?;
            if la {
                return Ok(true);
            }
            eval_condition_on(b, left_row, left_cols, right_row, right_cols)
        }
        Condition::Not(x) => {
            let v = eval_condition_on(x, left_row, left_cols, right_row, right_cols)?;
            Ok(!v)
        }
    }
}
