use crate::types::storage_types::Value;
use std::{collections::BTreeMap, ops::Bound};

pub type IndexKey = Vec<Value>;

#[derive(Debug, Clone)]
pub struct BTreeIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub map: BTreeMap<IndexKey, Vec<(usize, usize)>>, // key -> list of (page_no, slot_no)
}

impl BTreeIndex {
    /// Create new empty index
    pub fn new(name: String, table: String, columns: Vec<String>) -> Self {
        Self {
            name,
            table,
            columns,
            map: BTreeMap::new(),
        }
    }

    /// Insert entry: key -> (page_no, slot_no)
    pub fn insert(&mut self, key: IndexKey, pos: (usize, usize)) {
        self.map.entry(key).or_default().push(pos);
    }

    /// Exact lookup: return positions for given key
    pub fn search_eq(&self, key: &IndexKey) -> Option<&Vec<(usize, usize)>> {
        self.map.get(key)
    }

    /// Delete one position from key (returns true if something removed)
    pub fn delete(&mut self, key: &IndexKey, pos: (usize, usize)) -> bool {
        if let Some(vec) = self.map.get_mut(key) {
            let before = vec.len();
            vec.retain(|&p| p != pos);
            return before != vec.len();
        }
        false
    }

    /// Remove pos from key, drop whole key if empty
    pub fn remove(&mut self, key: &Vec<Value>, pos: (usize, usize)) {
        if let Some(entries) = self.map.get_mut(key) {
            entries.retain(|p| p != &pos);
            if entries.is_empty() {
                self.map.remove(key);
            }
        }
    }

    /// Range scan: return positions for keys in [lower, upper)
    pub fn search_range(
        &self,
        lower: Bound<Vec<Value>>,
        upper: Bound<Vec<Value>>,
    ) -> Vec<(usize, usize)> {
        let mut res = Vec::new();
        for (_k, v) in self.map.range((lower, upper)) {
            res.extend(v.clone());
        }
        res
    }

    /// Prefix match (useful for composite indexes)
    pub fn search_prefix(&self, prefix: &IndexKey) -> Vec<(usize, usize)> {
        let mut res = Vec::new();
        for (k, v) in &self.map {
            if k.starts_with(prefix) {
                res.extend(v.clone());
            }
        }
        res
    }
}
