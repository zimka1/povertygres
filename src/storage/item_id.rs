use crate::consts::page_consts::ITEM_ID_SIZE;
use crate::types::page_types::ItemId;
use std::convert::TryInto;

impl ItemId {
    pub fn mark_unused(&mut self) {
        self.flags = 0;
    }

    pub fn is_used(&self) -> bool {
        self.flags != 0
    }
    
    pub fn to_bytes(&self) -> [u8; ITEM_ID_SIZE] {
        let mut buf = [0u8; ITEM_ID_SIZE];

        // serialize offset (2 bytes)
        buf[0..2].copy_from_slice(&self.offset.to_le_bytes());
        // serialize length (2 bytes)
        buf[2..4].copy_from_slice(&self.len.to_le_bytes());
        // serialize flags (2 bytes)
        buf[4..6].copy_from_slice(&self.flags.to_le_bytes());

        buf
    }

    pub fn from_bytes(buf: &[u8]) -> Self {
        assert!(buf.len() >= ITEM_ID_SIZE); // must have enough bytes

        // deserialize fields from little-endian byte slices
        let offset = u16::from_le_bytes(buf[0..2].try_into().unwrap());
        let len = u16::from_le_bytes(buf[2..4].try_into().unwrap());
        let flags = u16::from_le_bytes(buf[4..6].try_into().unwrap());

        Self { offset, len, flags }
    }
}
