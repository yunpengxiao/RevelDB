use crate::core::write_batch::WriteBatch;
use std::fs::File;

pub struct LogWritter {
    dst: File,
}

impl LogWritter {
    pub fn new(path: &str) -> Self {
        Self {
            dst: File::create(path).unwrap(),
        }
    }

    pub fn add_record(&mut self, write_batch: WriteBatch) {}
}
