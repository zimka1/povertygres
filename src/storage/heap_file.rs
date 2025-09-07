use crate::consts::catalog_consts::PAGE_SIZE;
use crate::consts::page_consts::{ITEM_ID_SIZE, PAGE_HEADER_SIZE};
use crate::executer::help_functions::build_key;
use crate::types::b_tree::BTreeIndex;
use crate::types::page_types::{ItemId, Page, TupleHeader};
use crate::types::storage_types::{Column, Row};
use crate::types::transaction_types::TransactionManager;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct HeapFile {
    pub path: PathBuf, // path to the physical heap file
}

impl HeapFile {
    pub fn new(path: &str) -> Self {
        // create file and initialize with first empty page
        let page = Page::new(0);
        let bytes = page.to_bytes();

        let mut file = File::create(path).expect("create file failed");
        file.write_all(&bytes).expect("write failed");
        file.sync_all().unwrap();

        Self {
            path: PathBuf::from(path),
        }
    }

    pub fn read_page(&self, page_no: u32) -> Page {
        // open file and seek to correct page offset
        let mut file = File::open(&self.path).expect("open failed");

        let offset = page_no as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).expect("seek failed");

        // read page bytes into buffer
        let mut buf = [0u8; PAGE_SIZE as usize];
        file.read_exact(&mut buf).expect("read failed");

        Page::from_bytes(buf) // deserialize page
    }

    pub fn write_page(&self, page: &Page) {
        // open file for writing
        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .expect("open failed");

        // seek to page offset
        let offset = page.header.page_no as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).expect("seek failed");

        // serialize and write page
        let buf = page.to_bytes();
        file.write_all(&buf).expect("write failed");
        file.sync_all().unwrap();
    }

    pub fn append_page(&self) -> Page {
        // open file in append mode
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .expect("open failed");

        // compute new page number from file length
        let metadata = file.metadata().expect("metadata failed");
        let page_no = (metadata.len() / PAGE_SIZE as u64) as u32;

        // create and append empty page
        let page = Page::new(page_no);
        let buf = page.to_bytes();
        file.write_all(&buf).expect("write failed");
        file.sync_all().unwrap();

        page
    }

    pub fn get_tuple(
        &self,
        page_no: u32,
        slot_no: usize,
        schema: &[Column],
    ) -> Option<(TupleHeader, Row)> {
        let page = self.read_page(page_no);
        page.get_tuple(slot_no, schema)
    }

    pub fn insert_row(&self, row: Row, xid: u32) -> Result<(usize, usize), String> {
        // find last page number
        let metadata = std::fs::metadata(&self.path).map_err(|e| e.to_string())?;
        let page_count = (metadata.len() / PAGE_SIZE as u64) as u32;
        let last_page_no = if page_count == 0 { 0 } else { page_count - 1 };

        // load last page
        let mut page = self.read_page(last_page_no);

        // try insert row
        if let Ok(slot_no) = page.insert_tuple(row.clone(), xid) {
            // row fits into existing page
            self.write_page(&page);
            Ok((last_page_no as usize, slot_no))
        } else {
            // not enough space → create new page
            let mut page = self.append_page();
            let slot_no = page.insert_tuple(row, xid).map_err(|e| e.to_string())?;
            let new_page_no = page_count;
            self.write_page(&page);
            Ok((new_page_no as usize, slot_no))
        }
    }

    pub fn scan_all(&self, schema: &[Column]) -> Vec<(u32, usize, TupleHeader, Row)> {
        let mut rows = Vec::new();
        let metadata = std::fs::metadata(&self.path).expect("metadata failed");
        let page_count = (metadata.len() / PAGE_SIZE as u64) as u32;

        for page_no in 0..page_count {
            let page = self.read_page(page_no);
            for slot_no in 0..page.header.slot_count {
                if let Some((header, row)) = page.get_tuple(slot_no as usize, schema) {
                    rows.push((page_no, slot_no as usize, header, row));
                }
            }
        }
        rows
    }

    pub fn delete_at(
        &self,
        page_no: u32,
        slot_no: usize,
        xid: u32,
    ) -> Result<(), String> {
        let mut page = self.read_page(page_no);
        if slot_no as u16 >= page.header.slot_count {
            return Err("Invalid slot_no".into());
        }
    
        // compute offset of the item header in the page
        let slot_offset: usize = PAGE_SIZE as usize - (slot_no + 1) * ITEM_ID_SIZE;
        let item = ItemId::from_bytes(&page.data[slot_offset..slot_offset + ITEM_ID_SIZE]);
    
        if !item.is_used() {
            return Err("Slot already unused".into());
        }
    
        const XMAX_OFFSET: usize = 4;
        let tuple_range_lo = item.offset as usize;
        let tuple_range_hi = (item.offset + item.len) as usize;
        let tuple_bytes = &mut page.data[tuple_range_lo..tuple_range_hi];
    
        if tuple_bytes.len() < XMAX_OFFSET + 4 {
            return Err("Corrupted tuple header: too short to hold xmax".into());
        }
    
        tuple_bytes[XMAX_OFFSET..XMAX_OFFSET + 4].copy_from_slice(&xid.to_le_bytes());
    
        self.write_page(&page);
        Ok(())
    }
    

    /// Update an existing row at a given page/slot, or move it if it no longer fits
    pub fn update_row(
        &self,
        page_no: u32,
        slot_no: usize,
        new_row: Row,
        xid: u32,
    ) -> Result<(u32, usize), String> {
        // mark old tuple as deleted for this xid
        self.delete_at(page_no, slot_no, xid)?;
    
        // insert new tuple with xmin = xid
        let mut page: Page = self.read_page(page_no);
        if let Ok(new_slot) = page.insert_tuple(new_row.clone(), xid) {
            self.write_page(&page);
            return Ok((page_no, new_slot));
        } else {
            let mut new_page = self.append_page();
            let new_slot = new_page.insert_tuple(new_row, xid).map_err(|e| e.to_string())?;
            let new_page_no = new_page.header.page_no;
            self.write_page(&new_page);
            return Ok((new_page_no, new_slot));
        }
    }
    

    pub fn vacuum(
        &self,
        tm: &TransactionManager,
        columns: &[Column],
        table_name: &str,
        indexes: &mut HashMap<String, BTreeIndex>,
    ) -> usize {
        let mut removed = 0;
        let metadata = std::fs::metadata(&self.path).expect("metadata failed");
        let page_count = (metadata.len() / PAGE_SIZE as u64) as u32;
    
        for page_no in 0..page_count {
            let mut page = self.read_page(page_no);
    
            let mut write_ptr: usize = PAGE_HEADER_SIZE as usize;
    
            for slot_no in 0..page.header.slot_count as usize {
                let slot_off = PAGE_SIZE as usize - (slot_no + 1) * ITEM_ID_SIZE;
                let mut item = ItemId::from_bytes(&page.data[slot_off..slot_off + ITEM_ID_SIZE]);
    
                if !item.is_used() { continue; }
    
                if let Some((header, row)) = page.get_tuple(slot_no, columns) {
                    if header.is_dead(tm) {
                        for idx in indexes.values_mut().filter(|i| i.table == table_name) {
                            let key = build_key(&idx.columns, columns, &row.values, table_name)
                                .expect("failed to build key");
                            idx.remove(&key, (page_no as usize, slot_no));
                        }
                        item.mark_unused();
                        page.data[slot_off..slot_off + ITEM_ID_SIZE]
                            .copy_from_slice(&item.to_bytes());
                        removed += 1;
                        continue;
                    }
    
                    let src_lo = item.offset as usize;
                    let src_hi = src_lo + item.len as usize;
                    let len = item.len as usize;
    
                    if write_ptr != src_lo {
                        let tmp = page.data[src_lo..src_hi].to_vec();
                        page.data[write_ptr..write_ptr + len].copy_from_slice(&tmp);
                    }
    
                    item.offset = write_ptr as u16;
                    item.len = len as u16;
                    page.data[slot_off..slot_off + ITEM_ID_SIZE]
                        .copy_from_slice(&item.to_bytes());
    
                    write_ptr += len;
                }
            }
            page.header.free_start = write_ptr as u16;
            self.write_page(&page);
        }
    
        removed
    }    
}
