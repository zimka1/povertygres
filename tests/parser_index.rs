use povertygres::parser::main::parse_query;
use povertygres::types::parser_types::Query;

#[test]
fn test_create_index_with_name() {
    let q = parse_query("create index idx_users_id on users(id)").unwrap();
    if let Query::CreateIndex {
        index_name,
        table_name,
        column_names,
    } = q
    {
        assert_eq!(index_name, "idx_users_id");
        assert_eq!(table_name, "users");
        assert_eq!(column_names, vec!["id"]);
    } else {
        panic!("Expected CreateIndex");
    }
}

#[test]
fn test_create_index_auto_name_single_col() {
    let q = parse_query("create index on users(id)").unwrap();
    if let Query::CreateIndex {
        index_name,
        table_name,
        column_names,
    } = q
    {
        assert_eq!(index_name, "users_id_idx");
        assert_eq!(table_name, "users");
        assert_eq!(column_names, vec!["id"]);
    } else {
        panic!("Expected CreateIndex");
    }
}

#[test]
fn test_create_index_auto_name_multi_col() {
    let q = parse_query("create index on users(id, name)").unwrap();
    if let Query::CreateIndex {
        index_name,
        table_name,
        column_names,
    } = q
    {
        assert_eq!(index_name, "users_id_name_idx");
        assert_eq!(table_name, "users");
        assert_eq!(column_names, vec!["id", "name"]);
    } else {
        panic!("Expected CreateIndex");
    }
}
