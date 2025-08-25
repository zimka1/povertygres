use crate::consts::page_consts::PAGE_SIZE;

pub struct PageHeader {
    pub page_no: u32,
    pub slot_count: u16,
    pub free_start: u16,
    pub free_end: u16,
    pub checksum: u32,
}

pub struct Page {
    pub header: PageHeader,
    pub data: [u8; PAGE_SIZE],
}

pub struct ItemId {
    pub offset: u16,
    pub len: u16,
    pub flags: u16,
}

#[derive(Debug, Clone)]
pub struct TupleHeader {
    pub xmin: u32,
    pub nullmap_bytes: NullBitmap,
    pub flags: u16,
}

#[derive(Debug, Clone)]
pub struct NullBitmap {
    pub bytes: Vec<u8>,
    pub column_count: usize,
}
