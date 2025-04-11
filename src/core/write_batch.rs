use bytes::{Bytes, BytesMut};

use super::mem_table::{self, MemTable};
use super::utils;

/*
WriteBatch format:
+---------------+---------------+----------------------------------------+
| Sequence (8B) | Count (4B)    | Record 1 | Record 2 | ... | Record N   |
+---------------+---------------+----------------------------------------+

Record format:
+-----------+------------+-----------+------------+-----------+
| Type (1B) | Key Length | Key Bytes | Val Length | Val Bytes |
+-----------+------------+-----------+------------+-----------+
*/

pub struct WriteBatch {
    data: BytesMut,
}

#[repr(u8)]
pub enum ValueType {
    KTypeDeletion = 0x0,
    KTypeValue = 0x1,
}

impl WriteBatch {
    const HEADER_SIZE: usize = 12;

    pub fn new() -> Self {
        let data = BytesMut::zeroed(Self::HEADER_SIZE);
        Self { data }
    }

    pub fn get_sequence(&self) -> u64 {
        u64::from_le_bytes(self.data[..8].try_into().unwrap())
    }

    pub fn put(&mut self, key: String, value: String) {
        self.increase_count();
        self.data.extend_from_slice(&[ValueType::KTypeValue as u8]);
        self.put_length_prefixed_data(key.clone());
        self.put_length_prefixed_data(value.clone());
    }

    pub fn delete(&mut self, key: String) {
        self.increase_count();
        self.data
            .extend_from_slice(&[ValueType::KTypeDeletion as u8]);
        self.put_length_prefixed_data(key.clone());
    }

    pub fn get_data(&self) -> Bytes {
        self.data.clone().freeze()
    }

    fn increase_count(&mut self) {
        let count = self.get_count() + 1;
        self.data[8..12].copy_from_slice(&count.to_le_bytes());
    }

    fn get_count(&self) -> u32 {
        u32::from_le_bytes(self.data[8..12].try_into().unwrap())
    }

    fn put_length_prefixed_data(&mut self, data: String) {
        let mut buf = [0; 5];
        let write_len = utils::encode_varint_32(&mut buf, data.as_bytes().len() as u32);
        self.data.extend_from_slice(&buf[..write_len]);
        self.data.extend_from_slice(data.as_bytes());
    }
}
