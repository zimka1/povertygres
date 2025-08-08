use crate::types::{Column, Row, Value};

pub fn print_table(columns: &[Column], rows: &[Row]) {
    // Step 1: Determine column widths based on header names
    let mut widths: Vec<usize> = columns.iter().map(|col| col.name.len()).collect();

    // Step 2: Adjust widths to fit the widest value in each column
    for row in rows {
        for (i, value) in row.values.iter().enumerate() {
            let s = match value {
                Value::Int(v) => v.to_string(),
                Value::Text(s) => s.clone(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
            };
            if s.len() > widths[i] {
                widths[i] = s.len();
            }
        }
    }

    // Step 3: Build the horizontal separator line based on column widths
    let sep_line = widths
        .iter()
        .map(|w| format!("+{}+", "-".repeat(*w + 2)))
        .collect::<Vec<_>>()
        .join("")
        .replace("++", "+"); // Remove duplicate plus signs
    let sep_line = format!("+{}+", sep_line.trim_matches('+'));

    // Step 4: Print header
    println!("{}", sep_line);
    let header = columns
        .iter()
        .zip(&widths)
        .map(|(col, w)| format!("| {:width$} ", col.name, width = *w))
        .collect::<String>()
        + "|";
    println!("{}", header);
    println!("{}", sep_line);

    // Step 5: Print each row
    for row in rows {
        let line = row
            .values
            .iter()
            .zip(&widths)
            .map(|(val, w)| {
                let s = match val {
                    Value::Int(v) => v.to_string(),
                    Value::Text(s) => s.clone(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "NULL".to_string(),
                };
                format!("| {:width$} ", s, width = *w)
            })
            .collect::<String>()
            + "|";
        println!("{}", line);
    }

    // Step 6: Print bottom border
    println!("{}", sep_line);
}
