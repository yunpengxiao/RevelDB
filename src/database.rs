use thiserror::Error;

use crate::log_writter::LogWritter;
use crate::mem_table::MemTable;
use crate::write_batch::WriteBatch;

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
            log_writter: LogWritter::new(),
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

    pub fn delte(&mut self, k: &String) -> Result<()> {
        self.mmtable.delete(k);
        Ok(())
    }
}
