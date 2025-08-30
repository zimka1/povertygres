use povertygres::types::parser_types::Query;
use povertygres::types::storage_types::Value;
use povertygres::parser::main_parser::parse_query;

#[test]
fn test_insert_basic() {
    let query = parse_query(r#"insert into users values (1, "alice", true)"#).unwrap();
    if let Query::Insert { table_name, column_names, values } = query {
        assert_eq!(table_name, "users");
        assert!(column_names.is_none());
        assert_eq!(values, vec![Value::Int(1), Value::Text("alice".to_string()), Value::Bool(true)]);
    } else {
        panic!("Unexpected query variant");
    }
}

#[test]
fn test_insert_with_columns() {
    let query = parse_query(r#"insert into users(id, name) values (2, "bob")"#).unwrap();
    if let Query::Insert { table_name, column_names, values } = query {
        assert_eq!(table_name, "users");
        assert_eq!(column_names, Some(vec!["id".to_string(), "name".to_string()]));
        assert_eq!(values, vec![Value::Int(2), Value::Text("bob".to_string())]);
    }
}

#[test]
fn test_insert_with_null_and_bool() {
    let query = parse_query("insert into users values (null, true, false)").unwrap();
    if let Query::Insert { values, .. } = query {
        assert_eq!(values, vec![Value::Null, Value::Bool(true), Value::Bool(false)]);
    }
}

#[test]
fn test_insert_missing_values_keyword() {
    let res = parse_query("insert into users (id) (1)");
    assert!(res.is_err());
}

#[test]
fn test_insert_unrecognized_value() {
    let res = parse_query("insert into users values (foo)");
    assert!(res.is_err());
}
