use bytes::{BufMut, BytesMut};

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
enum ValueType {
    KTypeDeletion = 0x0,
    KTypeValue = 0x1,
}

impl WriteBatch {
    const HEADER_SIZE: usize = 12;

    pub fn new() -> Self {
        let data = BytesMut::zeroed(Self::HEADER_SIZE);
        Self { data }
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

    fn increase_count(&mut self) {
        let count = self.get_count() + 1;
        self.data[8..12].copy_from_slice(&count.to_le_bytes());
    }

    fn get_count(&self) -> u32 {
        u32::from_le_bytes(self.data[8..12].try_into().unwrap())
    }

    fn put_length_prefixed_data(&mut self, data: String) {
        let mut buf = [0; 5];
        let write_len = self.encode_varint_32(&mut buf, data.as_bytes().len() as u32);
        self.data.extend_from_slice(&buf[..write_len]);
        self.data.extend_from_slice(data.as_bytes());
    }

    fn encode_varint_32(&self, dst: &mut [u8], v: u32) -> usize {
        const B: u8 = 128;
        let mut bytes_written = 0;

        if v < (1 << 7) {
            dst[0] = v as u8;
            bytes_written = 1;
        } else if v < (1 << 14) {
            dst[0] = (v as u8) | B;
            dst[1] = (v >> 7) as u8;
            bytes_written = 2;
        } else if v < (1 << 21) {
            dst[0] = (v as u8) | B;
            dst[1] = ((v >> 7) as u8) | B;
            dst[2] = (v >> 14) as u8;
            bytes_written = 3;
        } else if v < (1 << 28) {
            dst[0] = (v as u8) | B;
            dst[1] = ((v >> 7) as u8) | B;
            dst[2] = ((v >> 14) as u8) | B;
            dst[3] = (v >> 21) as u8;
            bytes_written = 4;
        } else {
            dst[0] = (v as u8) | B;
            dst[1] = ((v >> 7) as u8) | B;
            dst[2] = ((v >> 14) as u8) | B;
            dst[3] = ((v >> 21) as u8) | B;
            dst[4] = (v >> 28) as u8;
            bytes_written = 5;
        }

        bytes_written
    }
}
