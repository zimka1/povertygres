use crate::types::storage_types::Value;
use std::{collections::BTreeMap, ops::Bound};

pub type IndexKey = Vec<Value>;

#[derive(Debug, Clone)]
pub struct BTreeIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub map: BTreeMap<IndexKey, Vec<(usize, usize)>>,
}

impl BTreeIndex {
    pub fn new(name: String, table: String, columns: Vec<String>) -> Self {
        Self {
            name,
            table,
            columns,
            map: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: IndexKey, pos: (usize, usize)) {
        self.map.entry(key).or_default().push(pos);
    }

    pub fn search_eq(&self, key: &IndexKey) -> Option<&Vec<(usize, usize)>> {
        self.map.get(key)
    }

    pub fn delete(&mut self, key: &IndexKey, pos: (usize, usize)) -> bool {
        if let Some(vec) = self.map.get_mut(key) {
            let before = vec.len();
            vec.retain(|&p| p != pos);
            return before != vec.len();
        }
        false
    }

    pub fn remove(&mut self, key: &Vec<Value>, pos: (usize, usize)) {
        if let Some(entries) = self.map.get_mut(key) {
            entries.retain(|p| p != &pos);
            if entries.is_empty() {
                self.map.remove(key);
            }
        }
    }

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
