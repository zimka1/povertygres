use super::io::*;
use crate::consts::catalog_consts::DATA_DIR;
use crate::errors::catalog_error::CatalogError;
use crate::types::catalog_types::{Catalog, ColumnMeta, TableMeta};
use crate::types::storage_types::ForeignKeyConstraint;
use std::path::{Path, PathBuf};

pub struct CatalogManager {
    data_dir: PathBuf, // path where catalog and tables are stored
    catalog: Catalog,  // in-memory catalog state
}

impl CatalogManager {
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self, CatalogError> {
        // Load catalog if exists, otherwise create new one
        let cat = load_or_create_catalog(data_dir.as_ref())?;
        Ok(Self {
            data_dir: data_dir.as_ref().to_path_buf(), // store base dir
            catalog: cat,                              // keep loaded catalog
        })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog // return immutable reference to catalog
    }

    pub fn create_table(
        &mut self,
        name: &str,
        columns: Vec<ColumnMeta>, // schema definition for new table
        primary_key: Option<String>,
        foreign_keys: Vec<ForeignKeyConstraint>,
    ) -> Result<&TableMeta, CatalogError> {
        // Prevent duplicate table creation
        if self.catalog.has_table(name) {
            return Err(CatalogError::TableExists(name.into()));
        }

        // Run catalog integrity checks
        super::validate::validate_catalog(&self.catalog)?;

        // Assign new table OID
        let oid = self.catalog.next_table_oid;

        // Define file path for this table
        let file = format!("{DATA_DIR}/{name}.tbl");

        // Construct table metadata
        let tm = TableMeta {
            oid,
            file,
            columns,
            next_rowid: 1, // start row id counter
            primary_key,
            foreign_keys,
        };

        // Update catalog state
        self.catalog.next_table_oid += 1;
        self.catalog.tables.insert(name.to_string(), tm);

        // Persist catalog changes to disk atomically
        save_catalog_atomic(&self.data_dir, &self.catalog)?;

        // Return reference to the created table metadata
        Ok(self.catalog.tables.get(name).unwrap())
    }
}
