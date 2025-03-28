use bytes::{Buf, BytesMut};
use crc::{CRC_32_AUTOSAR, Crc};

use crate::core::write_batch::WriteBatch;
use std::{fs::File, io::Write};

/* Block Format:

+-----------------+--------------------------------------------------+
|     Header      |                     Data/Payload                  |
+-----------------+--------------------------------------------------+
| Record Type (1B)| Actual data written by the application...         |
| Length (2B)     |                                                  |
| CRC (4B)        |                                                  |
+-----------------+--------------------------------------------------+
*/

pub struct LogWritter {
    dst: File,
    block_offset: usize,
}

#[repr(u8)]
enum RecordType {
    ZERO_TYPE = 0, // why?
    FULL_TYPE = 1,
    FIRST_TYPE = 2,
    MIDDLE_TYPE = 3,
    LAST_TYPE = 4,
}

impl LogWritter {
    const BLOCK_SIZE: usize = 32768;
    const HEADER_SIZE: usize = 4 + 2 + 1;

    const CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_AUTOSAR);

    pub fn new(path: &str) -> Self {
        Self {
            dst: File::create(path).unwrap(),
            block_offset: 0,
        }
    }

    // It will append a header even the write batch has 0 len data.
    pub fn add_record(&mut self, write_batch: &WriteBatch) {
        let wb_data = write_batch.get_data();
        let mut left = wb_data.len();
        let mut written = 0;
        loop {
            self.padding_if_needed();
            let avail = Self::BLOCK_SIZE - self.block_offset - Self::HEADER_SIZE;
            if left <= avail {
                let r_type = if written == 0 {
                    RecordType::FULL_TYPE
                } else {
                    RecordType::LAST_TYPE
                };
                self.emit_physical_record(r_type, &wb_data[written..], left);
                left = 0;
                written = left;
                self.block_offset += left;
            } else {
                let r_type = if written == 0 {
                    RecordType::FIRST_TYPE
                } else {
                    RecordType::MIDDLE_TYPE
                };
                self.emit_physical_record(r_type, &wb_data[written..], avail);
                left -= avail;
                written += avail;
                self.block_offset += avail;
            }

            if left == 0 {
                break;
            }
        }
    }

    fn emit_physical_record(&mut self, r_type: RecordType, data: &[u8], len: usize) {
        let mut data_to_write = BytesMut::with_capacity(len + Self::HEADER_SIZE);
        data_to_write.extend_from_slice(&[r_type as u8]);
        data_to_write.extend_from_slice(&len.to_be_bytes());
        let crc = Self::CRC.checksum(&data[..len]);
        data_to_write.extend_from_slice(&crc.to_be_bytes());
        data_to_write.extend_from_slice(&data[..len]);
        self.dst.write(&data_to_write).unwrap();
    }

    fn padding_if_needed(&mut self) {
        let leftover = Self::BLOCK_SIZE - self.block_offset;
        if leftover < Self::HEADER_SIZE {
            if leftover != 0 {
                let zeros = [0; 6];
                self.dst.write_all(&zeros).unwrap();
            }
            self.block_offset = 0;
        }
    }
}
