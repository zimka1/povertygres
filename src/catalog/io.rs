use crate::consts::catalog_consts::{CATALOG_FILE, PAGE_SIZE};
use crate::errors::catalog_error::CatalogError;
use crate::types::catalog_types::Catalog;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::Path;
use tempfile::NamedTempFile;

pub fn load_or_create_catalog(data_dir: &Path) -> Result<Catalog, CatalogError> {
    fs::create_dir_all(data_dir)?; // ensure data dir exists
    let path = data_dir.join(CATALOG_FILE);

    if !path.exists() {
        // create empty catalog if no file yet
        let cat = Catalog::empty(PAGE_SIZE);
        save_catalog_atomic(data_dir, &cat)?;
        return Ok(cat);
    }

    // load catalog from file
    let f = File::open(&path)?;
    let reader = BufReader::new(f);
    let cat: Catalog = serde_json::from_reader(reader)?;

    // check page size consistency
    if cat.page_size != PAGE_SIZE {
        return Err(CatalogError::Invalid(format!(
            "page_size mismatch: catalog={}, expected={}",
            cat.page_size, PAGE_SIZE
        )));
    }

    // validate structure
    super::validate::validate_catalog(&cat)?;
    Ok(cat)
}

pub fn save_catalog_atomic(data_dir: &Path, cat: &Catalog) -> Result<(), CatalogError> {
    let json = serde_json::to_string_pretty(cat)?; // serialize catalog
    fs::create_dir_all(data_dir)?;                 // ensure dir exists

    // write to temp file for atomic replace
    let tmp = NamedTempFile::new_in(data_dir)?;
    {
        let mut f = tmp.as_file();
        f.write_all(json.as_bytes())?; // write json
        f.sync_all()?;                 // flush data
    }

    // move temp file into final path
    let final_path = data_dir.join(CATALOG_FILE);
    tmp.persist(&final_path)
        .map_err(|e| CatalogError::Invalid(format!("persist failed: {}", e)))?;

    // on unix: also sync directory metadata
    #[cfg(unix)]
    {
        let dirfd = File::open(data_dir)?;
        dirfd.sync_all()?;
    }

    Ok(())
}
