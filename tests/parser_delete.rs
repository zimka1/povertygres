use povertygres::parser::main_parser::parse_query;
use povertygres::types::parser_types::{Condition, Query, Operand};
use povertygres::types::filter_types::CmpOp;
use povertygres::types::storage_types::Value;

#[test]
fn test_delete_basic() {
    let q = parse_query("delete from users").unwrap();
    if let Query::Delete { table_name, filter } = q {
        assert_eq!(table_name, "users");
        assert!(filter.is_none());
    } else {
        panic!("Unexpected query variant: {:?}", q);
    }
}

#[test]
fn test_delete_with_filter() {
    let q = parse_query("delete from users where age > 18").unwrap();
    if let Query::Delete { table_name, filter } = q {
        assert_eq!(table_name, "users");
        let cond = filter.expect("Expected filter");
        match cond {
            Condition::Cmp(CmpOp::Gt, Operand::Column(col), Operand::Literal(Value::Int(n))) => {
                assert_eq!(col, "age");
                assert_eq!(n, 18);
            }
            other => panic!("Unexpected condition: {:?}", other),
        }
    }
}

#[test]
fn test_delete_missing_from() {
    let res = parse_query("delete users");
    assert!(res.is_err());
}
