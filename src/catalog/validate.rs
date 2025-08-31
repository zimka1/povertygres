use crate::consts::catalog_consts::PAGE_SIZE;
use crate::errors::catalog_error::CatalogError;
use crate::types::catalog_types::Catalog;
use std::collections::HashSet;

pub fn validate_catalog(cat: &Catalog) -> Result<(), CatalogError> {
    // check catalog version
    if cat.version != 1 {
        return Err(CatalogError::Invalid(format!(
            "unsupported version {}",
            cat.version
        )));
    }

    // check page size consistency
    if cat.page_size != PAGE_SIZE {
        return Err(CatalogError::Invalid("page size mismatch".into()));
    }

    let mut oids = HashSet::new(); // track unique table ids
    let mut files = HashSet::new(); // track unique table files

    for (name, t) in &cat.tables {
        // enforce unique table OIDs
        if !oids.insert(t.oid) {
            return Err(CatalogError::Invalid(format!(
                "duplicate oid in table {name}"
            )));
        }

        // enforce unique table files
        if !files.insert(t.file.clone()) {
            return Err(CatalogError::Invalid(format!(
                "file reused by multiple tables: {}",
                t.file
            )));
        }

        // enforce unique column names within table
        let mut cols = HashSet::new();
        for c in &t.columns {
            if !cols.insert(c.name.clone()) {
                return Err(CatalogError::Invalid(format!(
                    "duplicate column '{}' in table {}",
                    c.name, name
                )));
            }
        }
    }

    Ok(())
}
