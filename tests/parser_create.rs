use povertygres::parser::main_parser::parse_query;
use povertygres::types::parser_types::Query;
use povertygres::types::storage_types::{ColumnType, Value};

#[test]
fn test_basic_create() {
    let query = parse_query("create table users (id int, name text)").unwrap();
    if let Query::CreateTable { table_name, columns, primary_key, foreign_keys } = query {
        assert_eq!(table_name, "users");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].column_type, ColumnType::Int);
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[1].column_type, ColumnType::Text);
        assert!(primary_key.is_none());
        assert!(foreign_keys.is_empty());
    } else {
        panic!("Unexpected query variant");
    }
}

#[test]
fn test_primary_key_inline() {
    let query = parse_query("create table users (id int primary key, name text)").unwrap();
    if let Query::CreateTable { primary_key, columns, .. } = query {
        assert_eq!(primary_key, Some("id".to_string()));
        assert!(columns[0].not_null); // PK всегда NOT NULL
    } else {
        panic!("Unexpected query variant");
    }
}

#[test]
fn test_primary_key_table_level() {
    let query = parse_query("create table users (id int, name text, primary key id)").unwrap();
    if let Query::CreateTable { primary_key, .. } = query {
        assert_eq!(primary_key, Some("id".to_string()));
    } else {
        panic!("Unexpected query variant");
    }
}

#[test]
fn test_not_null_and_default() {
    let query = parse_query(
        "create table t (a int not null default 5, b bool default true, c text default \"hi\")"
    ).unwrap();
    if let Query::CreateTable { columns, .. } = query {
        assert!(columns[0].not_null);
        assert_eq!(columns[0].default, Some(Value::Int(5)));
        assert_eq!(columns[1].default, Some(Value::Bool(true)));
        assert_eq!(columns[2].default, Some(Value::Text("hi".to_string())));
    }
}

#[test]
fn test_foreign_key_inline() {
    let query = parse_query(
        "create table orders (id int, user_id int references users(id))"
    ).unwrap();
    if let Query::CreateTable { foreign_keys, .. } = query {
        assert_eq!(foreign_keys.len(), 1);
        assert_eq!(foreign_keys[0].local_columns, vec!["user_id"]);
        assert_eq!(foreign_keys[0].referenced_table, "users");
        assert_eq!(foreign_keys[0].referenced_columns, vec!["id"]);
    }
}

#[test]
fn test_foreign_key_table_level() {
    let query = parse_query(
        "create table orders (id int, user_id int, foreign key user_id references users(id))"
    ).unwrap();
    if let Query::CreateTable { foreign_keys, .. } = query {
        assert_eq!(foreign_keys.len(), 1);
        assert_eq!(foreign_keys[0].local_columns, vec!["user_id"]);
        assert_eq!(foreign_keys[0].referenced_table, "users");
        assert_eq!(foreign_keys[0].referenced_columns, vec!["id"]);
    }
}

#[test]
fn test_invalid_type() {
    let res = parse_query("create table t (x float)");
    assert!(res.is_err());
}

#[test]
fn test_invalid_syntax_missing_paren() {
    let res = parse_query("create table t id int, name text)");
    assert!(res.is_err());
}
