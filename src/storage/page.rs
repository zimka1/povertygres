use crate::consts::page_consts::{ITEM_ID_SIZE, PAGE_HEADER_SIZE, PAGE_SIZE};
use crate::types::page_types::{ItemId, NullBitmap, Page, PageHeader, TupleHeader};
use crate::types::storage_types::{Column, ColumnType, Row, Value};

impl Page {
    pub fn new(page_no: u32) -> Self {
        // initialize empty page with header
        let mut data = [0u8; PAGE_SIZE];
        let header = PageHeader::new(page_no);
        let hdr_bytes = header.to_bytes();
        data[0..PAGE_HEADER_SIZE].copy_from_slice(&hdr_bytes);

        Self { header, data }
    }

    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        // serialize header + data
        let mut buf = self.data;
        let hdr_bytes = self.header.to_bytes();
        buf[0..PAGE_HEADER_SIZE].copy_from_slice(&hdr_bytes);
        buf
    }

    pub fn from_bytes(buf: [u8; PAGE_SIZE]) -> Self {
        // deserialize page from raw bytes
        let header = PageHeader::from_bytes(&buf[0..PAGE_HEADER_SIZE]);
        Self { header, data: buf }
    }

    pub fn insert_tuple(&mut self, row: Row, xid: u32) -> Result<usize, String> {
        // build null bitmap
        let mut nullmap_bytes = NullBitmap::new(row.values.len());
        for (i, val) in row.values.iter().enumerate() {
            if let Value::Null = val {
                nullmap_bytes.set_null(i);
            }
        }

        // build tuple header
        let header = TupleHeader {
            xmin: xid,
            xmax: None,
            flags: 0,
            nullmap_bytes,
        };

        // serialize tuple (header + values)
        let mut tuple_bytes = header.to_bytes();
        for val in row.values.iter() {
            match val {
                Value::Int(i) => tuple_bytes.extend_from_slice(&(*i as i32).to_le_bytes()),
                Value::Text(s) => {
                    let bytes = s.as_bytes();
                    let len = bytes.len() as u16;
                    tuple_bytes.extend_from_slice(&len.to_le_bytes());
                    tuple_bytes.extend_from_slice(bytes);
                }
                Value::Bool(b) => tuple_bytes.push(if *b { 1 } else { 0 }),
                _ => {}
            }
        }

        let tuple_len = tuple_bytes.len() as u16;

        // check available free space
        let needed_space = tuple_len as usize + ITEM_ID_SIZE;
        let available_space = (self.header.free_end - self.header.free_start) as usize;
        if needed_space > available_space {
            return Err("Not enough space on page".into());
        }

        // write tuple bytes into free space
        let offset = self.header.free_start;
        self.data[offset as usize..offset as usize + tuple_len as usize]
            .copy_from_slice(&tuple_bytes);

        // create item id for slot array
        let item = ItemId {
            offset,
            len: tuple_len,
            flags: 1,
        };
        let item_bytes = item.to_bytes();
        let item_pos = (self.header.free_end as usize) - ITEM_ID_SIZE;
        self.data[item_pos..item_pos + ITEM_ID_SIZE].copy_from_slice(&item_bytes);

        // update page header
        self.header.slot_count += 1;
        self.header.free_start += tuple_len;
        self.header.free_end -= ITEM_ID_SIZE as u16;

        Ok((self.header.slot_count - 1) as usize)
    }

    pub fn get_tuple(&self, slot_no: usize, columns: &[Column]) -> Option<(TupleHeader, Row)> {
        // out of bounds
        if slot_no as u16 >= self.header.slot_count {
            return None;
        }

        // find item id in slot array
        let slot_offset = PAGE_SIZE - (slot_no + 1) * ITEM_ID_SIZE;
        let item_bytes = &self.data[slot_offset..slot_offset + ITEM_ID_SIZE];
        let item = ItemId::from_bytes(item_bytes);

        if item.flags == 0 {
            return None; // deleted / unused slot
        }

        // slice out tuple bytes
        let tuple_bytes = &self.data[item.offset as usize..(item.offset + item.len) as usize];

        // parse tuple header (xmin, xmax, nullmap, flags)
        let xmin = u32::from_le_bytes(tuple_bytes[0..4].try_into().unwrap());
        let raw_xmax = u32::from_le_bytes(tuple_bytes[4..8].try_into().unwrap());
        let xmax = if raw_xmax == 0 { None } else { Some(raw_xmax) };

        let nullmap_len = u16::from_le_bytes(tuple_bytes[8..10].try_into().unwrap()) as usize;
        let nullmap_bytes = tuple_bytes[10..10 + nullmap_len].to_vec();
        let flags_offset = 10 + nullmap_len;
        let flags = u16::from_le_bytes(
            tuple_bytes[flags_offset..flags_offset + 2]
                .try_into()
                .unwrap(),
        );

        // construct null bitmap
        let nullmap = NullBitmap {
            bytes: nullmap_bytes,
            column_count: columns.len(),
        };

        // parse values
        let mut values = Vec::new();
        let mut cursor = flags_offset + 2;
        for (i, col) in columns.iter().enumerate() {
            if nullmap.is_null(i) {
                values.push(Value::Null);
                continue;
            }
            match col.column_type {
                ColumnType::Int => {
                    let v = i32::from_le_bytes(tuple_bytes[cursor..cursor + 4].try_into().unwrap());
                    values.push(Value::Int(v as i64));
                    cursor += 4;
                }
                ColumnType::Text => {
                    let len =
                        u16::from_le_bytes(tuple_bytes[cursor..cursor + 2].try_into().unwrap())
                            as usize;
                    cursor += 2;
                    let s = String::from_utf8(tuple_bytes[cursor..cursor + len].to_vec()).unwrap();
                    values.push(Value::Text(s));
                    cursor += len;
                }
                ColumnType::Bool => {
                    let v = tuple_bytes[cursor] != 0;
                    values.push(Value::Bool(v));
                    cursor += 1;
                }
            }
        }

        let header = TupleHeader {
            xmin,
            xmax,
            nullmap_bytes: nullmap,
            flags,
        };

        Some((header, Row { values }))
    }
}
