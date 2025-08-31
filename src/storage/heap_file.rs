use crate::consts::catalog_consts::PAGE_SIZE;
use crate::consts::page_consts::ITEM_ID_SIZE;
use crate::types::page_types::{ItemId, Page};
use crate::types::storage_types::{Column, Row};
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

    pub fn insert_row(&self, row: Row) -> Result<(usize, usize), String> {
        // find last page number
        let metadata = std::fs::metadata(&self.path).map_err(|e| e.to_string())?;
        let page_count = (metadata.len() / PAGE_SIZE as u64) as u32;
        let last_page_no = if page_count == 0 { 0 } else { page_count - 1 };

        // load last page
        let mut page = self.read_page(last_page_no);

        // try insert row
        if let Ok(slot_no) = page.insert_tuple(row.clone()) {
            // row fits into existing page
            self.write_page(&page);
            Ok((last_page_no as usize, slot_no))
        } else {
            // not enough space → create new page
            let mut page = self.append_page();
            let slot_no = page.insert_tuple(row).map_err(|e| e.to_string())?;
            let new_page_no = page_count;
            self.write_page(&page);
            Ok((new_page_no as usize, slot_no))
        }
    }

    /// Scan all rows from all pages, decoding tuples using the provided schema
    pub fn scan_all(&self, schema: &[Column]) -> Vec<Row> {
        let mut rows = Vec::new();

        // compute number of pages in file
        let metadata = std::fs::metadata(&self.path).expect("metadata failed");
        let page_count = (metadata.len() / PAGE_SIZE as u64) as u32;

        // iterate through all pages
        for page_no in 0..page_count {
            let page = self.read_page(page_no);
            // iterate over slots
            for slot_no in 0..page.header.slot_count {
                if let Some(row) = page.get_tuple(slot_no as usize, schema) {
                    rows.push(row); // collect valid row
                }
            }
        }
        rows
    }

    /// Scan all rows and also return their physical position (page_no, slot_no)
    pub fn scan_all_with_pos(&self, schema: &[Column]) -> Vec<(u32, usize, Row)> {
        let mut rows = Vec::new();
        let metadata = std::fs::metadata(&self.path).expect("metadata failed");
        let page_count = (metadata.len() / PAGE_SIZE as u64) as u32;

        for page_no in 0..page_count {
            let page = self.read_page(page_no);
            for slot_no in 0..page.header.slot_count {
                if let Some(row) = page.get_tuple(slot_no as usize, schema) {
                    rows.push((page_no, slot_no as usize, row));
                }
            }
        }
        rows
    }

    pub fn delete_at(&self, page_no: u32, slot_no: usize) -> Result<(), String> {
        let mut page = self.read_page(page_no);
        if slot_no as u16 >= page.header.slot_count {
            return Err("Invalid slot_no".into());
        }

        // compute offset of the item header in the page
        let slot_offset: usize = PAGE_SIZE as usize - (slot_no + 1) * ITEM_ID_SIZE;
        let mut item = ItemId::from_bytes(&page.data[slot_offset..slot_offset + ITEM_ID_SIZE]);
        item.flags = 0; // mark as deleted
        page.data[slot_offset..slot_offset + ITEM_ID_SIZE].copy_from_slice(&item.to_bytes());

        self.write_page(&page);
        Ok(())
    }

    /// Update an existing row at a given page/slot, or move it if it no longer fits
    pub fn update_row(&self, page_no: u32, slot_no: usize, new_row: Row) -> Result<(), String> {
        self.delete_at(page_no, slot_no)?;

        let mut page: Page = self.read_page(page_no);
        if page.insert_tuple(new_row.clone()).is_ok() {
            self.write_page(&page);
            Ok(())
        } else {
            let mut new_page = self.append_page();
            new_page.insert_tuple(new_row).map_err(|e| e.to_string())?;
            self.write_page(&new_page);
            Ok(())
        }
    }
}
