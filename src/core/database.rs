use thiserror::Error;

use crate::core::log_writter::LogWritter;
use crate::core::mem_table::MemTable;
use crate::core::write_batch::WriteBatch;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("Write Error")]
    WriteError,

    #[error("Read Error")]
    ReadError,
}

pub type Result<T> = core::result::Result<T, DBError>;

pub struct Database {
    mmtable: MemTable,
    log_writter: LogWritter,
}

impl Database {
    pub fn new() -> Self {
        Self {
            mmtable: MemTable::new(),
            log_writter: LogWritter::new("/tmp/db.log"),
        }
    }

    pub fn put(&mut self, k: &String, v: &String) -> Result<()> {
        let mut write_batch = WriteBatch::new();
        write_batch.put(k.clone(), v.clone());
        self.log_writter.add_record(write_batch);
        self.mmtable.put(k.clone(), v.clone());
        Ok(())
    }

    pub fn get(&self, k: &String) -> Result<&String> {
        match self.mmtable.get(k) {
            Some(v) => Ok(v),
            None => Err(DBError::ReadError),
        }
    }

    pub fn delete(&mut self, k: &String) -> Result<()> {
        let mut write_batch = WriteBatch::new();
        write_batch.delete(k.clone());
        self.mmtable.delete(k);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_read() {
        let mut db = Database::new();
        db.put(&String::from("123"), &String::from("456")).unwrap();
        assert_eq!(db.get(&String::from("123")).unwrap(), &String::from("456"));
    }

    #[test]
    fn test_delete() {
        let mut db = Database::new();
        db.put(&String::from("123"), &String::from("456")).unwrap();
        assert_eq!(db.get(&String::from("123")).unwrap(), &String::from("456"));
        db.delete(&String::from("123")).unwrap();
        assert_eq!(db.get(&String::from("123")).is_err(), true);
    }
}
