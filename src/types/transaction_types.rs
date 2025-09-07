use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Transaction status: in-progress, committed, or aborted
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxStatus {
    InProgress,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub xmin: u32,
    pub xmax: u32,
    pub active_xids: Vec<u32>
}

/// In-memory transaction manager (maps xid -> status)
pub struct TransactionManager {
    pub transactions: HashMap<u32, TxStatus>,
    pub next_xid: u32,
    pub active_xids: Vec<u32>, 
}

impl TransactionManager {
    /// Create empty transaction manager
    pub fn new() -> Self {
        Self { 
            transactions: HashMap::new(),
            next_xid: 1,
            active_xids: Vec::new(),
        }
    }

    /// Initialize from existing map (e.g. loaded from catalog)
    pub fn from_map(m: HashMap<u32, TxStatus>, next_xid: u32) -> Self {
        let active: Vec<u32> = m
            .iter()
            .filter(|(_, st)| **st == TxStatus::InProgress)
            .map(|(xid, _)| *xid)
            .collect();

        Self {
            transactions: m,
            next_xid,
            active_xids: active,
        }
    }

    pub fn alloc_xid(&mut self) -> u32 {
        let xid = self.next_xid;
        self.next_xid += 1;
        xid
    }

    /// Mark transaction as started
    pub fn begin(&mut self, xid: u32) {
        self.transactions.insert(xid, TxStatus::InProgress);
        self.active_xids.push(xid);
    }

    /// Mark transaction as committed
    pub fn commit(&mut self, xid: u32) {
        self.transactions.insert(xid, TxStatus::Committed);
        self.active_xids.retain(|&x| x != xid);
    }

    /// Mark transaction as aborted
    pub fn rollback(&mut self, xid: u32) {
        self.transactions.insert(xid, TxStatus::Aborted);
        self.active_xids.retain(|&x| x != xid);
    }

    /// Get current status of a transaction
    /// Unknown xids are treated as committed by default
    pub fn status(&self, xid: u32) -> TxStatus {
        *self.transactions.get(&xid).unwrap_or(&TxStatus::Committed)
    }

    /// Build snapshot of current state
    pub fn snapshot(&self) -> Snapshot {
        let xmin = self.active_xids.iter().min().copied().unwrap_or(self.next_xid);
        let xmax = self.next_xid;
        let active_xids = self.active_xids.clone();

        Snapshot { xmin, xmax, active_xids }
    }
}