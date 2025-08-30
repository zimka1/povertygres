use crate::catalog::catalog_manager::CatalogManager;
use crate::consts::catalog_consts::DATA_DIR;
use crate::errors::engine_error::EngineError;
use crate::page::heap_file::{HeapFile};
use crate::types::catalog_types::{CatColumnType, ColumnMeta};
use crate::types::storage_types::{ColumnType, ForeignKeyConstraint};
use crate::types::storage_types::{Column, Database, Table};

use std::path::{Path, PathBuf};


pub struct Engine {
    pub db: Database,
    pub cat: CatalogManager,
}

impl Engine {
    pub fn open() -> Result<Self, EngineError> {
        let cat = CatalogManager::open(Path::new(DATA_DIR))?;
        let mut db = Database::new();
        for (name, tm) in cat.catalog().tables.iter() {
            let cols: Vec<Column> = tm
                .columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    column_type: match c.ty {
                        CatColumnType::Int32 => ColumnType::Int,
                        CatColumnType::Text => ColumnType::Text,
                        CatColumnType::Bool => ColumnType::Bool,
                    },
                    not_null: c.not_null,
                    default: c.default.clone()
                })
                .collect();
            db.tables.insert(
                name.clone(),
                Table {
                    name: name.clone(),
                    columns: cols,
                    rows: Vec::new(),
                    heap: HeapFile {
                        path: PathBuf::from(&tm.file),
                    },
                    primary_key: tm.primary_key.clone(),
                    foreign_keys: tm.foreign_keys.clone(),
                },
            );
        }
        Ok(Self { db, cat })
    }

    pub fn create_table_in_both(
        &mut self,
        name: &str,
        columns: Vec<Column>,
        primary_key: Option<String>,
        foreign_keys: Vec<ForeignKeyConstraint>
    ) -> Result<(), EngineError> {
        let cols_meta: Vec<ColumnMeta> = columns
            .iter()
            .map(|col| {
                let ty = match col.column_type {
                    ColumnType::Int => CatColumnType::Int32,
                    ColumnType::Text => CatColumnType::Text,
                    ColumnType::Bool => CatColumnType::Bool,
                };
                ColumnMeta {
                    name: col.name.clone(),
                    ty,
                    not_null: col.not_null,
                    default: col.default.clone()
                }
            })
            .collect();

        self.cat.create_table(name, cols_meta, primary_key.clone(), foreign_keys.clone())?;

        let file_path = format!("{DATA_DIR}/{name}.tbl");

        let heap_file = HeapFile::new(file_path.as_str());

        self.db
            .create_table(&name.to_string(), columns, heap_file, primary_key, foreign_keys)?;

        Ok(())
    }
}
