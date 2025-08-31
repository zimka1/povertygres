use crate::consts::page_consts::PAGE_SIZE;

// metadata at beginning of each page
pub struct PageHeader {
    pub page_no: u32,    // page number in file
    pub slot_count: u16, // number of slots (tuples)
    pub free_start: u16, // start of free space
    pub free_end: u16,   // end of free space
    pub checksum: u32,   // checksum for validation
}

// full page = header + raw data
pub struct Page {
    pub header: PageHeader,    // page header
    pub data: [u8; PAGE_SIZE], // raw byte array
}

// item pointer in slot array
pub struct ItemId {
    pub offset: u16, // offset to tuple data
    pub len: u16,    // length of tuple
    pub flags: u16,  // flags (valid/deleted/etc.)
}

#[derive(Debug, Clone)]
pub struct TupleHeader {
    pub xmin: u32,                 // transaction id of inserter
    pub nullmap_bytes: NullBitmap, // bitmap for NULL values
    pub flags: u16,                // tuple flags
}

#[derive(Debug, Clone)]
pub struct NullBitmap {
    pub bytes: Vec<u8>,      // raw bitmap bytes
    pub column_count: usize, // number of columns tracked
}
