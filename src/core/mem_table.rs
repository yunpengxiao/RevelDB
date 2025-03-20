use std::collections::HashMap;

pub struct MemTable {
    table: HashMap<String, String>,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    pub fn put(&mut self, k: String, v: String) {
        self.table.insert(k, v);
    }

    pub fn get(&self, k: &String) -> Option<&String> {
        self.table.get(k)
    }

    pub fn delete(&mut self, k: &String) {
        self.table.remove(k);
    }
}
