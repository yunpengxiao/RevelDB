pub fn encode_varint_32(dst: &mut [u8], v: u32) -> usize {
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

pub fn decode_varint_32(src: &[u8]) -> Option<u32> {
    const B: u8 = 128;
    let mut v = 0;
    let mut shift = 0;

    for &byte in src {
        if byte < B {
            v |= (byte as u32) << shift;
            return Some(v);
        }
        v |= ((byte & !B) as u32) << shift;
        shift += 7;
    }

    None
}

pub fn varint_length(mut v: u64) -> u32 {
    let mut len = 1;
    while v >= 128 {
        v >>= 7;
        len += 1;
    }
    len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_and_decode_varint_32() {
        let mut buf = [0u8; 5];
        let _ = encode_varint_32(&mut buf, 1);
        assert_eq!(decode_varint_32(&buf), Some(1));

        let mut buf = [0u8; 5];
        let _ = encode_varint_32(&mut buf, 3);
        assert_eq!(decode_varint_32(&buf), Some(3));

        let mut buf = [0u8; 5];
        let _ = encode_varint_32(&mut buf, 10);
        assert_eq!(decode_varint_32(&buf), Some(10));

        let mut buf = [0u8; 5];
        let _ = encode_varint_32(&mut buf, 100);
        assert_eq!(decode_varint_32(&buf), Some(100));

        let mut buf = [0u8; 5];
        let _ = encode_varint_32(&mut buf, 1234);
        assert_eq!(decode_varint_32(&buf), Some(1234));

        let mut buf = [0u8; 5];
        let _ = encode_varint_32(&mut buf, 123456);
        assert_eq!(decode_varint_32(&buf), Some(123456));

        let mut buf = [0u8; 5];
        let _ = encode_varint_32(&mut buf, 3);
        assert_eq!(decode_varint_32(&buf), Some(3));
    }
}
