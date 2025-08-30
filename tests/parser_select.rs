use povertygres::parser::main_parser::parse_query;
use povertygres::types::parser_types::{Condition, FromItem, JoinKind, Operand, Query};
use povertygres::types::filter_types::CmpOp;
use povertygres::types::storage_types::Value;

#[test]
fn test_simple_select() {
    let q = parse_query("select id, name from users").unwrap();
    if let Query::Select { from_table, column_names, filter, .. } = q {
        assert_eq!(column_names, vec!["id".to_string(), "name".to_string()]);
        assert!(matches!(from_table, FromItem::Table(t) if t == "users"));
        assert!(filter.is_none());
    } else {
        panic!("Unexpected query: {:?}", q);
    }
}

#[test]
fn test_select_star() {
    let q = parse_query("select * from users").unwrap();
    if let Query::Select { column_names, .. } = q {
        assert_eq!(column_names, vec!["*"]);
    }
}

#[test]
fn test_select_with_where() {
    let q = parse_query("select id from users where age > 18 and active = true").unwrap();
    if let Query::Select { filter, .. } = q {
        let cond = filter.expect("Expected filter");
        match cond {
            Condition::And(l, r) => {
                assert!(matches!(*l, Condition::Cmp(CmpOp::Gt, Operand::Column(_), Operand::Literal(Value::Int(18)))));
                assert!(matches!(*r, Condition::Cmp(CmpOp::Eq, Operand::Column(_), Operand::Literal(Value::Bool(true)))));
            }
            _ => panic!("Unexpected condition: {:?}", cond),
        }
    }
}

#[test]
fn test_select_with_alias() {
    let q = parse_query("select u.id from users as u").unwrap();
    if let Query::Select { aliases, .. } = q {
        assert_eq!(aliases.get("u"), Some(&"users".to_string()));
    }
}

#[test]
fn test_select_with_inner_join() {
    let q = parse_query("select u.id, o.amount from users u inner join orders o on u.id = o.user_id").unwrap();
    if let Query::Select { from_table, .. } = q {
        match from_table {
            FromItem::Join { left, right, kind, .. } => {
                assert!(matches!(*left, FromItem::Table(ref t) if t == "users"));
                assert!(matches!(*right, FromItem::Table(ref t) if t == "orders"));
                assert!(matches!(kind, JoinKind::Inner));
            }
            _ => panic!("Expected join"),
        }
    }
}

#[test]
fn test_select_with_left_and_inner_join() {
    let q = parse_query(
        "select u.id, o.amount, p.name \
         from users as u \
         left join orders as o on u.id = o.user_id \
         inner join products p on u.id = p.user_id"
    ).unwrap();
    if let Query::Select { from_table, .. } = q {
        // Проверяем, что верхний join — inner
        match from_table {
            FromItem::Join { kind, .. } => assert!(matches!(kind, JoinKind::Inner)),
            _ => panic!("Expected join"),
        }
    }
}

#[test]
fn test_select_missing_from() {
    let res = parse_query("select id, name users");
    assert!(res.is_err());
}
