use crate::types::storage_types::Value;

pub struct BTreeNode {
    pub keys: Vec<Value>,
    pub children: Vec<Box<BTreeNode>>,
    pub rows: Vec<Vec<(usize, usize)>>,
    pub leaf: bool,
}

pub struct BTreeIndex {
    pub table: String,
    pub column: String,
    pub root: Box<BTreeNode>,
    pub t: usize,
}

impl BTreeIndex {
    // pub fn new(table: String, column: String, t: usize) -> Self {}
}
