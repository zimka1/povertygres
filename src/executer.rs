use crate::storage::{Database, print_table};
use crate::types::Query;

pub fn execute(db: &mut Database, ast: Query) -> Result<(), String> {
    let _ = match ast {
        Query::CreateTable { name, columns } => db.create_table(&name, columns)?,
        Query::Insert { table, column_names, values } => db.insert_into(&table, column_names, values)?,
        Query::Select { table, column_names } => {
            let rows = db.select(&table, column_names)?;
            let table = db.tables.get(&table).unwrap();
            print_table(&table.columns, &rows);
        },
    };
    Ok(())
}