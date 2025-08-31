use crate::consts::page_consts::{PAGE_HEADER_SIZE, PAGE_SIZE};
use crate::types::page_types::PageHeader;

impl PageHeader {
    pub fn new(page_no: u32) -> Self {
        Self {
            page_no,                             // page number in file
            slot_count: 0,                       // number of item slots
            free_start: PAGE_HEADER_SIZE as u16, // beginning of free space
            free_end: PAGE_SIZE as u16,          // end of free space
            checksum: 0,                         // checksum (not computed yet)
        }
    }

    pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
        let mut buf = [0u8; PAGE_HEADER_SIZE];
        // serialize header fields into fixed-size buffer
        buf[0..4].copy_from_slice(&self.page_no.to_le_bytes());
        buf[4..6].copy_from_slice(&self.slot_count.to_le_bytes());
        buf[6..8].copy_from_slice(&self.free_start.to_le_bytes());
        buf[8..10].copy_from_slice(&self.free_end.to_le_bytes());
        buf[10..14].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        // deserialize fields from byte buffer
        Self {
            page_no: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
            slot_count: u16::from_le_bytes(buf[4..6].try_into().unwrap()),
            free_start: u16::from_le_bytes(buf[6..8].try_into().unwrap()),
            free_end: u16::from_le_bytes(buf[8..10].try_into().unwrap()),
            checksum: u32::from_le_bytes(buf[10..14].try_into().unwrap()),
        }
    }
}
