use crate::printer::print_table;
use crate::storage::Database;
use crate::types::parse_types::{Condition, Query};
use crate::types::storage_types::{Column, Row, Value};
use crate::errors::eval_error::{EvalError, CmpOp, EvalResult, ValueType};

// Executes a parsed query (AST) against the database
pub fn execute(db: &mut Database, ast: Query) -> Result<(), String> {
    let _ = match ast {
        // CREATE TABLE name (...) -> create a new table
        Query::CreateTable { name, columns, filter } => db.create_table(&name, columns)?,

        // INSERT INTO table (...) VALUES (...) -> insert a new row
        Query::Insert {
            table,
            column_names,
            values,
            filter
        } => db.insert_into(&table, column_names, values)?,

        // SELECT ... FROM table -> fetch and print matching rows
        Query::Select {
            table,
            column_names,
            filter
        } => {
            println!("Step 1");
            // Step 1: Retrieve rows from the table (projection of requested columns)
            let rows = db.select(&table, column_names)?;

            println!("Step 2");
            // Step 2: Get a reference to the full table (to access column metadata)
            let table_ref = db.tables.get(&table).expect("table not found");

            println!("Step 3");
            // Step 3: Apply WHERE filter if present
            let rows: Vec<Row> = match &filter {
                Some(cond) => {
                    let mut out = Vec::with_capacity(rows.len());
                    for row in rows {
                        println!("{:?}", row);
                        // Evaluate the condition for this row
                        // Convert EvalError to String to match this function's return type
                        if eval_condition(cond, &row, &table_ref.columns)
                            .map_err(|e| e.to_string())?
                        {
                            out.push(row);
                        }
                    }
                    out
                },
                None => rows,
            };

            println!("Step 4");
            // Step 4: Print results in a formatted table
            print_table(&table_ref.columns, &rows);
        }
    };

    Ok(())
}

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
        return Err(EvalError::TypeMismatch { left: lt, right: rt, op });
    }

    // Compare according to type
    use CmpOp::*;
    match (left, right) {
        // Integer comparison
        (Value::Int(a),  Value::Int(b))   => Ok(match op {
            Eq => a == b,
            Ne => a != b,
            Lt => a < b,
            Lte => a <= b,
            Gt => a > b,
            Gte => a >= b
        }),
        // String comparison (lexicographical)
        (Value::Text(a), Value::Text(b))  => Ok(match op {
            Eq => a == b,
            Ne => a != b,
            Lt => a < b,
            Lte => a <= b,
            Gt => a > b,
            Gte => a >= b
        }),
        // Boolean comparison (only Eq/Ne are valid)
        (Value::Bool(a), Value::Bool(b))  => {
            match op {
                Eq => Ok(a == b),
                Ne => Ok(a != b),
                _  => Err(EvalError::InvalidOpForType { ty: ValueType::Bool, op })
            }
        }
        // Fallback: mismatch that slipped through earlier checks
        _ => Err(EvalError::TypeMismatch { left: lt, right: rt, op }),
    }
}

/// Looks up a column's value in a row by name
pub fn lookup_col<'a>(columns: &'a Vec<Column>, row: &'a Row, name: &str) -> EvalResult<&'a Value> {
    // Find column index by name
    let idx = columns.iter().position(|c| c.name == *name)
        .ok_or_else(|| EvalError::UnknownColumn(name.to_string()))?;
    // Return the value at the corresponding index in the row
    row.values.get(idx).ok_or(EvalError::Internal("row.values index out of bounds"))
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
            if !la { return Ok(false); }
            eval_condition(b, row, columns)
        }

        // Logical OR (short-circuits if left is true)
        Condition::Or(a, b) => {
            let la = eval_condition(a, row, columns)?;
            if la { return Ok(true); }
            eval_condition(b, row, columns)
        }

        // Logical NOT
        Condition::Not(x) => {
            let v = eval_condition(x, row, columns)?;
            Ok(!v)
        }
    }
}
