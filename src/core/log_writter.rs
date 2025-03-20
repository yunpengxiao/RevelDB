use crate::core::write_batch::WriteBatch;

pub struct LogWritter;

impl LogWritter {
    pub fn new() -> Self {
        Self
    }

    pub fn add_record(&mut self, write_batch: WriteBatch) {}
}
