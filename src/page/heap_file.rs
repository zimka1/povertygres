use crate::consts::catalog_consts::PAGE_SIZE;
use crate::types::page_types::{Page};
use crate::types::storage_types::{Column, Row};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct HeapFile {
    pub path: PathBuf,
}

impl HeapFile {
    pub fn new(path: &str) -> Self {
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
        let mut file = File::open(&self.path).expect("open failed");

        let offset = page_no as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).expect("seek failed");

        let mut buf = [0u8; PAGE_SIZE as usize];
        file.read_exact(&mut buf).expect("read failed");

        Page::from_bytes(buf)
    }

    pub fn write_page(&self, page: &Page) {
        let mut file = OpenOptions::new()
            .write(true)
            .open(&self.path)
            .expect("open failed");

        let offset = page.header.page_no as u64 * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).expect("seek failed");

        let buf = page.to_bytes();
        file.write_all(&buf).expect("write failed");
        file.sync_all().unwrap();
    }

    pub fn append_page(&self) -> Page {
        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .expect("open failed");

        let metadata = file.metadata().expect("metadata failed");
        let page_no = (metadata.len() / PAGE_SIZE as u64) as u32;

        let page = Page::new(page_no);
        let buf = page.to_bytes();
        file.write_all(&buf).expect("write failed");
        file.sync_all().unwrap();

        page
    }

    pub fn insert_row(&self, row: Row) -> Result<(), String> {
        let metadata = std::fs::metadata(&self.path).map_err(|e| e.to_string())?;
        let page_count = (metadata.len() / PAGE_SIZE as u64) as u32;
        let last_page_no = if page_count == 0 { 0 } else { page_count - 1 };

        let mut page = self.read_page(last_page_no);

        if let Err(_) = page.insert_tuple(row.clone()) {
            let mut page = self.append_page();
            page.insert_tuple(row).map_err(|e| e.to_string())?;
            self.write_page(&page);
        } else {
            self.write_page(&page);
        }

        Ok(())
    }

    pub fn scan_all(&self, schema: &[Column]) -> Vec<Row> {
        let mut rows = Vec::new();

        let metadata = std::fs::metadata(&self.path).expect("metadata failed");
        let page_count = (metadata.len() / PAGE_SIZE as u64) as u32;

        for page_no in 0..page_count {
            let page = self.read_page(page_no);
            for slot_no in 0..page.header.slot_count {
                if let Some(row) = page.get_tuple(slot_no as usize, schema) {
                    rows.push(row);
                }
            }
        }
        rows
    }
}
