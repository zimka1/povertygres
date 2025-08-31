use crate::types::storage_types::Value;
use std::collections::BTreeMap;

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
