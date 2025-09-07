use crate::catalog::catalog_manager::CatalogManager;
use crate::consts::catalog_consts::DATA_DIR;
use crate::errors::engine_error::EngineError;
use crate::storage::heap_file::HeapFile;
use crate::types::b_tree::BTreeIndex;
use crate::types::catalog_types::{CatColumnType, ColumnMeta};
use crate::types::storage_types::{Column, Database, Table};
use crate::types::storage_types::{ColumnType, ForeignKeyConstraint};
use crate::types::transaction_types::{IsolationLevel, Snapshot, TransactionManager, TxStatus};

use std::path::{Path, PathBuf};

/// Main database engine: holds in-memory DB + catalog manager
pub struct Engine {
    pub db: Database,           // in-memory database state (tables, indexes, tx manager)
    pub cat: CatalogManager,    // persistent catalog manager (metadata on disk)
    pub current_xid: Option<u32>, // active transaction ID (if any)
    pub session_isolation: IsolationLevel,
    pub tx_isolation: Option<IsolationLevel>,
    pub repeatable_snapshot: Option<Snapshot>,
}

impl Engine {
    /// Open engine by loading catalog from disk and reconstructing in-memory DB
    pub fn open() -> Result<Self, EngineError> {
        // Load catalog (tables, indexes, transactions) from catalog file
        let cat = CatalogManager::open(Path::new(DATA_DIR))?;
        let mut db = Database::new();

        // Rebuild in-memory tables from catalog metadata
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
                    default: c.default.clone(),
                })
                .collect();

            // Insert table into in-memory DB
            db.tables.insert(
                name.clone(),
                Table {
                    name: name.clone(),
                    columns: cols,
                    heap: HeapFile {
                        path: PathBuf::from(&tm.file), // attach heap file
                    },
                    primary_key: tm.primary_key.clone(),
                    foreign_keys: tm.foreign_keys.clone(),
                },
            );
        }

        // Restore transaction statuses from catalog into transaction manager
        db.transaction_manager = TransactionManager::from_map(
            cat.catalog().transactions.clone(),
            cat.catalog().next_xid
        );

        // Rebuild indexes from catalog metadata
        for (iname, imeta) in cat.catalog().indexes.iter() {
            let mut idx = BTreeIndex::new(
                imeta.name.clone(),
                imeta.table.clone(),
                imeta.columns.clone(),
            );
        
            if let Some(table) = db.tables.get(&imeta.table) {
                for (page_no, slot_no, _hdr, row) in table.heap.scan_all(&table.columns) {
                    let mut key = Vec::new();
                    for col in &imeta.columns {
                        let col_idx = table
                            .columns
                            .iter()
                            .position(|c| c.name == *col)
                            .expect("Index column not found in table");
                        key.push(row.values[col_idx].clone());
                    }
                    idx.insert(key, (page_no as usize, slot_no));
                }
            }
        
            db.indexes.insert(iname.clone(), idx);
        }

        Ok(Self {
            db,
            cat,
            current_xid: None,
            session_isolation: IsolationLevel::ReadCommitted,
            tx_isolation: None,
            repeatable_snapshot: None,
        })
    }

    /// Create a new table both in catalog (persistent) and in DB (in-memory)
    pub fn create_table_in_both(
        &mut self,
        name: &str,
        columns: Vec<Column>,
        primary_key: Option<String>,
        foreign_keys: Vec<ForeignKeyConstraint>,
    ) -> Result<(), EngineError> {
        // Convert in-memory Column → catalog ColumnMeta
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
                    default: col.default.clone(),
                }
            })
            .collect();

        // Create table in catalog (persist to disk)
        self.cat
            .create_table(name, cols_meta, primary_key.clone(), foreign_keys.clone())?;

        // Create empty heap file for table
        let file_path = format!("{DATA_DIR}/{name}.tbl");
        let heap_file = HeapFile::new(file_path.as_str());

        // Create table in in-memory DB
        self.db.create_table(
            &name.to_string(),
            columns,
            heap_file,
            primary_key,
            foreign_keys,
        )?;

        Ok(())
    }

    /// Create a new index both in catalog (persistent) and in DB (in-memory)
    pub fn create_index_in_both(
        &mut self,
        index_name: &str,
        table_name: &str,
        columns: Vec<String>,
    ) -> Result<(), EngineError> {
        // Register index in catalog
        self.cat.create_index(index_name, table_name, &columns)?;
        // Build index in in-memory DB
        self.db.create_index(index_name, table_name, columns)?;
        Ok(())
    }

    /// Allocate next transaction ID
    pub fn next_xid(&mut self) -> u32 {
        let xid = self.db.transaction_manager.alloc_xid();
        self.cat.catalog_mut().next_xid = self.db.transaction_manager.next_xid;
        xid
    }
    

    /// Start a new transaction
    pub fn begin_tx(&mut self, xid: u32) {
        self.db.transaction_manager.begin(xid);
        self.cat.catalog_mut().transactions.insert(xid, TxStatus::InProgress);
        self.cat.persist().unwrap();
    }

    /// Commit a transaction
    pub fn commit_tx(&mut self, xid: u32) {
        self.db.transaction_manager.commit(xid);
        self.cat.catalog_mut().transactions.insert(xid, TxStatus::Committed);
        self.cat.persist().unwrap();
    }

    /// Rollback a transaction
    pub fn rollback_tx(&mut self, xid: u32) {
        self.db.transaction_manager.rollback(xid);
        self.cat.catalog_mut().transactions.insert(xid, TxStatus::Aborted);
        self.cat.persist().unwrap();
    }

}
