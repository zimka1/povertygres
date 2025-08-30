use povertygres::parser::where_parser::parse_where;
use povertygres::types::parser_types::{Condition, Operand};
use povertygres::types::filter_types::CmpOp;
use povertygres::types::storage_types::Value;

#[test]
fn test_simple_eq() {
    let cond = parse_where(r#"age = 18"#).unwrap();
    match cond {
        Condition::Cmp(CmpOp::Eq, Operand::Column(col), Operand::Literal(Value::Int(n))) => {
            assert_eq!(col, "age");
            assert_eq!(n, 18);
        }
        _ => panic!("Unexpected AST: {:?}", cond),
    }
}

#[test]
fn test_and_or() {
    let cond = parse_where(r#"(age > 18 and active = true) or name = "Alice""#).unwrap();
    // Проверяем верхний уровень
    match cond {
        Condition::Or(left, right) => {
            // Левое поддерево должно быть AND
            if let Condition::And(_, _) = *left {
            } else {
                panic!("Expected AND inside left side");
            }
            // Правое поддерево должно быть сравнение name = "Alice"
            if let Condition::Cmp(CmpOp::Eq, Operand::Column(c), Operand::Literal(Value::Text(v))) =
                *right
            {
                assert_eq!(c, "name");
                assert_eq!(v, "Alice");
            } else {
                panic!("Expected name = \"Alice\"");
            }
        }
        _ => panic!("Unexpected AST: {:?}", cond),
    }
}

#[test]
fn test_not_condition() {
    let cond = parse_where("not active = true").unwrap();
    match cond {
        Condition::Not(inner) => {
            assert!(matches!(*inner, Condition::Cmp(..)));
        }
        _ => panic!("Expected NOT condition"),
    }
}

#[test]
fn test_invalid_unterminated_string() {
    let res = parse_where(r#"name = "Alice"#);
    assert!(res.is_err());
}

#[test]
fn test_invalid_character() {
    let res = parse_where("age # 20");
    assert!(res.is_err());
}

#[test]
fn test_mismatched_parenthesis() {
    let res = parse_where("(age = 10");
    assert!(res.is_err());
}
