use bytes::{BufMut, BytesMut};

pub struct WriteBatch {
    data: BytesMut,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            data: BytesMut::new(),
        }
    }

    pub fn put(&mut self, key: String, value: String) {
        let k_type = [0x01] as [u8; 1];
        self.data.put(&k_type[..]);
        self.data.put(key.as_bytes());
        self.data.put(value.as_bytes());
    }

    pub fn delete(&mut self, key: String) {
        let k_type = [0x00] as [u8; 1];
        self.data.put(&k_type[..]);
        self.data.put(key.as_bytes());
    }
}
