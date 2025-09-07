use crate::types::{
    page_types::{NullBitmap, TupleHeader},
    transaction_types::{Snapshot, TransactionManager, TxStatus},
};
use std::convert::TryInto;

impl TupleHeader {
    pub fn is_visible(&self, cur_xid: u32, snapshot: &Snapshot, tm: &TransactionManager) -> bool {
        let xmin_result = if self.xmin == cur_xid {
            true
        } else {
            if snapshot.active_xids.contains(&self.xmin) {
                return false;
            }
            if self.xmin >= snapshot.xmax {
                return false;
            }
            match tm.status(self.xmin) {
                TxStatus::Aborted => false,
                TxStatus::InProgress => false,
                TxStatus::Committed => true
            }
        };

        let xmax_result = if let Some(xmax) = self.xmax {
            if xmax == cur_xid {
                return false;
            }
            if snapshot.active_xids.contains(&xmax) {
                return true;
            }
            if xmax >= snapshot.xmax {
                return true;
            }
            match tm.status(xmax) {
                TxStatus::Aborted => true,
                TxStatus::InProgress => true,
                TxStatus::Committed => false
            }
        } else {
            true
        };

        xmin_result && xmax_result

    }

    pub fn is_dead(&self, tm: &TransactionManager) -> bool {
        match self.xmax {
            None => false,
            Some(x) => match tm.status(x) {
                TxStatus::InProgress => false,
                TxStatus::Aborted   => false,
                TxStatus::Committed => true,
            },
        }
    }
    

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // xmin
        buf.extend_from_slice(&self.xmin.to_le_bytes());

        // xmax (0 means None)
        buf.extend_from_slice(&self.xmax.unwrap_or(0).to_le_bytes());

        // length of null bitmap
        let null_len = self.nullmap_bytes.size() as u16;
        buf.extend_from_slice(&null_len.to_le_bytes());

        // raw null bitmap
        buf.extend_from_slice(&self.nullmap_bytes.bytes);

        // flags
        buf.extend_from_slice(&self.flags.to_le_bytes());

        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        // read xmin
        let xmin = u32::from_le_bytes(buf[0..4].try_into().unwrap());

        // read xmax
        let raw_xmax = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        let xmax = if raw_xmax == 0 { None } else { Some(raw_xmax) };

        // read null bitmap length and bytes
        let null_len = u16::from_le_bytes(buf[8..10].try_into().unwrap()) as usize;
        let null_bytes = buf[10..10 + null_len].to_vec();

        // read flags after nullmap
        let flags_offset = 10 + null_len;
        let flags = u16::from_le_bytes(buf[flags_offset..flags_offset + 2].try_into().unwrap());

        // construct nullmap
        let nullmap = NullBitmap {
            bytes: null_bytes,
        };

        Self {
            xmin,
            xmax,
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
