use super::write_batch::ValueType;
use crossbeam_skiplist::SkipMap;

pub struct MemTable {
    table: SkipMap<String, String>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: SkipMap::new(),
        }
    }

    pub fn add(&mut self, sequence: u64, k_type: ValueType, k: String, v: String) {
        match k_type {
            ValueType::KTypeValue => self.table.insert(k, v),
            ValueType::KTypeDeletion => self.table.remove(&k).unwrap(),
            _ => panic!("something wrong"),
        };
    }

    pub fn get(&self, k: &String) -> Option<String> {
        match &self.table.get(k) {
            Some(kv) => Some(kv.value().clone()),
            None => None,
        }
    }
}
