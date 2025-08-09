use std::fmt;
use crate::types::storage_types::{ValueType};
use crate::types::filter_types::{CmpOp};
use std::error::Error;

/// Represents possible errors that can occur during condition evaluation.
#[derive(Debug)]
pub enum EvalError {
    /// The specified column name does not exist in the table.
    UnknownColumn(String),

    /// The types of the left and right operands are incompatible for the operation.
    TypeMismatch { left: ValueType, right: ValueType, op: CmpOp },

    /// The comparison operator is not valid for the given data type.
    /// Example: trying to use `<` on a BOOL column.
    InvalidOpForType { ty: ValueType, op: CmpOp },

    /// Internal consistency error — indicates a bug or unexpected state.
    Internal(&'static str),
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EvalError::UnknownColumn(name) =>
                write!(f, "unknown column: {}", name),
            EvalError::TypeMismatch { left, right, op } =>
                write!(f, "type mismatch for {}: left is {}, right is {}", op, left, right),
            EvalError::InvalidOpForType { ty, op } =>
                write!(f, "invalid operator {} for type {}", op, ty),
            EvalError::Internal(msg) =>
                write!(f, "internal error: {}", msg),
        }
    }
}

// Allows EvalError to be used as a standard error type.
impl Error for EvalError {}

/// Convenience alias for results returned by evaluation functions.
pub type EvalResult<T> = Result<T, EvalError>;
