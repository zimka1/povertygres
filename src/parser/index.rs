use crate::types::parser_types::Query;

pub fn parse_create_index(input: &str) -> Result<Query, String> {
    let input = input.trim();

    let lower = input.to_ascii_lowercase();
    if !lower.starts_with("create index") {
        return Err("Invalid CREATE INDEX syntax".into());
    }

    let after_prefix = input["create index".len()..].trim();
    let lower_tail = after_prefix.to_ascii_lowercase();

    let (index_name_opt, after_on) = if lower_tail.starts_with("on ") {
        let mut rest = &after_prefix[2..];
        rest = rest.trim_start();
        (None, rest)
    } else {
        if let Some(pos) = lower_tail.find(" on ") {
            let idx_name = after_prefix[..pos].trim().to_string();
            let mut rest = &after_prefix[pos + 4..]; // пропускаем " on "
            rest = rest.trim_start();
            (Some(idx_name), rest)
        } else {
            return Err("Missing 'ON' in CREATE INDEX".into());
        }
    };

    let open = after_on.find('(').ok_or("Missing '(' in column list")?;
    let close = after_on.rfind(')').ok_or("Missing ')' in column list")?;

    let table_name = after_on[..open].trim().to_string();
    let inside = &after_on[open + 1..close];

    let column_names: Vec<String> = inside
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if column_names.is_empty() {
        return Err("No columns in index".into());
    }

    let index_name = match index_name_opt {
        Some(n) if !n.is_empty() => n,
        _ => format!("{}_{}_idx", table_name, column_names.join("_")),
    };

    Ok(Query::CreateIndex {
        index_name,
        table_name,
        column_names,
    })
}
