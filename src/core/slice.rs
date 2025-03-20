use bytes::BytesMut;

pub struct Slice {
    data: BytesMut,
}

impl Slice {
    pub fn new() -> Self {
        Self {
            data: BytesMut::new(),
        }
    }

    pub fn from(data: &[u8]) -> Self {
        Self {
            data: BytesMut::from(data),
        }
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    pub fn append(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }
}
