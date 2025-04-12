use super::utils;
use super::write_batch::ValueType;
use bytes::{Bytes, BytesMut};
use crossbeam_skiplist::SkipSet;

pub struct MemTable {
    table: SkipSet<InternalKeyValue>,
}

// Format of an entry is concatenation of:
//  key_size     : varint32 of internal_key.size()
//  key bytes    : char[internal_key.size()]
//  tag          : uint64((sequence << 8) | type)
//  value_size   : varint32 of value.size()
//  value bytes  : char[value.size()]
#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct InternalKeyValue {
    data: Bytes,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: SkipSet::new(),
        }
    }

    pub fn add(&mut self, sequence: u64, k_type: ValueType, k: String, v: String) {
        let kv = InternalKeyValue::from(sequence, k_type, k, v);
        self.table.insert(kv);
    }

    pub fn get(&self, k: &String) -> Option<String> {
        let lookup_key = InternalKeyValue::from_key(k.clone());
        match &self.table.get(&lookup_key) {
            Some(_) => Some("Found it!".to_string()),
            None => None,
        }
    }
}

impl InternalKeyValue {
    pub fn from(sequence: u64, k_type: ValueType, k: String, v: String) -> Self {
        let key_size = k.len() as u32;
        let val_size = v.len() as u32;
        let internal_key_size = key_size + 8;
        let encoded_len = utils::varint_length(internal_key_size as u64)
            + internal_key_size
            + utils::varint_length(val_size as u64)
            + val_size;
        let mut buff = BytesMut::zeroed(encoded_len as usize);
        let mut offset: usize = 0;
        let written = utils::encode_varint_32(&mut buff, internal_key_size);
        offset += written;

        buff[offset..offset + key_size as usize].copy_from_slice(k.as_bytes());
        offset += key_size as usize;

        let tag = (sequence << 8) | k_type as u64;
        buff[offset..offset + 8].copy_from_slice(&tag.to_be_bytes());
        offset += 8;

        let written = utils::encode_varint_32(&mut buff[offset..], val_size);
        offset += written;

        buff[offset..].copy_from_slice(v.as_bytes());

        Self {
            data: buff.freeze(),
        }
    }

    pub fn from_key(k: String) -> Self {
        Self::from(0, ValueType::KTypeValue, k, String::new())
    }

    pub fn get_key(&self) -> String {
        let mut offset = 0;
        let key_size = utils::decode_varint_32(&self.data[offset..]).unwrap() - 8;
        offset += utils::varint_length(key_size as u64) as usize;
        String::from_utf8_lossy(&self.data[offset..offset + key_size as usize]).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_key() {
        let test_key = "test_key".to_string();
        let kv = InternalKeyValue::from(
            1,
            ValueType::KTypeValue,
            test_key.clone(),
            "test_value".to_string(),
        );
        assert_eq!(kv.get_key(), test_key);
    }
}
