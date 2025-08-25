use crate::types::page_types::{NullBitmap, TupleHeader};
use std::convert::TryInto;

impl TupleHeader {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // xmin
        buf.extend_from_slice(&self.xmin.to_le_bytes());

        // длина nullmap (u16)
        let null_len = self.nullmap_bytes.size() as u16;
        buf.extend_from_slice(&null_len.to_le_bytes());

        // сами байты nullmap
        buf.extend_from_slice(&self.nullmap_bytes.bytes);

        // flags
        buf.extend_from_slice(&self.flags.to_le_bytes());

        buf
    }

    pub fn from_bytes(buf: &[u8], column_count: usize) -> Self {
        let xmin = u32::from_le_bytes(buf[0..4].try_into().unwrap());

        let null_len = u16::from_le_bytes(buf[4..6].try_into().unwrap()) as usize;
        let null_bytes = buf[6..6 + null_len].to_vec();

        let flags_offset = 6 + null_len;
        let flags = u16::from_le_bytes(buf[flags_offset..flags_offset + 2].try_into().unwrap());

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
        let byte_count = (column_count + 7) / 8;
        Self {
            bytes: vec![0; byte_count],
            column_count,
        }
    }

    pub fn set_null(&mut self, idx: usize) {
        let byte = idx / 8;
        let bit = idx % 8;
        self.bytes[byte] |= 1 << bit;
    }

    pub fn is_null(&self, idx: usize) -> bool {
        let byte = idx / 8;
        let bit = idx % 8;
        (self.bytes[byte] & (1 << bit)) != 0
    }

    pub fn size(&self) -> usize {
        self.bytes.len()
    }
}
