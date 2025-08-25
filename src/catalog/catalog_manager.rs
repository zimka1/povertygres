use super::io::*;
use crate::consts::catalog_consts::DATA_DIR;
use crate::errors::catalog_error::CatalogError;
use crate::types::catalog_types::{Catalog, ColumnMeta, TableMeta};
use std::path::{Path, PathBuf};

pub struct CatalogManager {
    data_dir: PathBuf,
    catalog: Catalog,
}

impl CatalogManager {
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self, CatalogError> {
        let cat = load_or_create_catalog(data_dir.as_ref())?;
        Ok(Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            catalog: cat,
        })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn create_table(
        &mut self,
        name: &str,
        columns: Vec<ColumnMeta>,
    ) -> Result<&TableMeta, CatalogError> {
        if self.catalog.has_table(name) {
            return Err(CatalogError::TableExists(name.into()));
        }
        super::validate::validate_catalog(&self.catalog)?;
        let oid = self.catalog.next_table_oid;
        let file = format!("{DATA_DIR}/{name}.tbl");
        let tm = TableMeta {
            oid,
            file,
            columns,
            next_rowid: 1,
        };
        self.catalog.next_table_oid += 1;
        self.catalog.tables.insert(name.to_string(), tm);
        save_catalog_atomic(&self.data_dir, &self.catalog)?;
        Ok(self.catalog.tables.get(name).unwrap())
    }
}
