use povertygres::parser::main_parser::parse_query;
use povertygres::types::parser_types::{Condition, Query, Operand};
use povertygres::types::filter_types::CmpOp;
use povertygres::types::storage_types::Value;

#[test]
fn test_update_basic() {
    let q = parse_query(r#"update users set name = "alice""#).unwrap();
    if let Query::Update { table_name, column_names, values, filter } = q {
        assert_eq!(table_name, "users");
        assert_eq!(column_names, vec!["name"]);
        assert_eq!(values, vec![Value::Text("alice".to_string())]);
        assert!(filter.is_none());
    } else {
        panic!("Unexpected query variant: {:?}", q);
    }
}

#[test]
fn test_update_multiple_columns() {
    let q = parse_query(r#"update users set age = 20, active = true"#).unwrap();
    if let Query::Update { column_names, values, .. } = q {
        assert_eq!(column_names, vec!["age", "active"]);
        assert_eq!(values, vec![Value::Int(20), Value::Bool(true)]);
    }
}

#[test]
fn test_update_with_null() {
    let q = parse_query(r#"update users set nickname = null"#).unwrap();
    if let Query::Update { column_names, values, .. } = q {
        assert_eq!(column_names, vec!["nickname"]);
        assert_eq!(values, vec![Value::Null]);
    }
}

#[test]
fn test_update_with_where() {
    let q = parse_query(r#"update users set age = 30 where id = 1"#).unwrap();
    if let Query::Update { filter, .. } = q {
        let cond = filter.expect("Expected filter");
        match cond {
            Condition::Cmp(CmpOp::Eq, Operand::Column(c), Operand::Literal(Value::Int(n))) => {
                assert_eq!(c, "id");
                assert_eq!(n, 1);
            }
            other => panic!("Unexpected condition: {:?}", other),
        }
    }
}

#[test]
fn test_update_missing_set() {
    let res = parse_query("update users name = 20");
    assert!(res.is_err());
}

#[test]
fn test_update_unrecognized_value() {
    let res = parse_query("update users set name = foo");
    assert!(res.is_err());
}
