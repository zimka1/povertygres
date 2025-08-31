use crate::types::page_types::{NullBitmap, TupleHeader};
use std::convert::TryInto;

impl TupleHeader {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // transaction id (xmin)
        buf.extend_from_slice(&self.xmin.to_le_bytes());

        // length of null bitmap (u16)
        let null_len = self.nullmap_bytes.size() as u16;
        buf.extend_from_slice(&null_len.to_le_bytes());

        // raw null bitmap bytes
        buf.extend_from_slice(&self.nullmap_bytes.bytes);

        // tuple flags
        buf.extend_from_slice(&self.flags.to_le_bytes());

        buf
    }

    pub fn from_bytes(buf: &[u8], column_count: usize) -> Self {
        // read xmin
        let xmin = u32::from_le_bytes(buf[0..4].try_into().unwrap());

        // read null bitmap length and bytes
        let null_len = u16::from_le_bytes(buf[4..6].try_into().unwrap()) as usize;
        let null_bytes = buf[6..6 + null_len].to_vec();

        // read flags after nullmap
        let flags_offset = 6 + null_len;
        let flags = u16::from_le_bytes(buf[flags_offset..flags_offset + 2].try_into().unwrap());

        // construct nullmap
        let nullmap = NullBitmap {
            bytes: null_bytes,
            column_count,
        };

        Self {
            xmin,
            nullmap_bytes: nullmap,
            flags,
        }
    }
}

impl NullBitmap {
    pub fn new(column_count: usize) -> Self {
        // number of bytes needed (round up)
        let byte_count = (column_count + 7) / 8;
        Self {
            bytes: vec![0; byte_count],
            column_count,
        }
    }

    pub fn set_null(&mut self, idx: usize) {
        // mark column as NULL
        let byte = idx / 8;
        let bit = idx % 8;
        self.bytes[byte] |= 1 << bit;
    }

    pub fn is_null(&self, idx: usize) -> bool {
        // check if column is NULL
        let byte = idx / 8;
        let bit = idx % 8;
        (self.bytes[byte] & (1 << bit)) != 0
    }

    pub fn size(&self) -> usize {
        // number of bytes in bitmap
        self.bytes.len()
    }
}
