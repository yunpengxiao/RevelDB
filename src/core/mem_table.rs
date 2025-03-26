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

    pub fn put(&mut self, k: String, v: String) {
        self.table.insert(k, v);
    }

    pub fn get(&self, k: &String) -> Option<String> {
        match &self.table.get(k) {
            Some(kv) => Some(kv.value().clone()),
            None => None,
        }
    }

    pub fn delete(&mut self, k: &String) {
        self.table.remove(k);
    }
}
