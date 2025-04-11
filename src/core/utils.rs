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

pub fn varint_length(mut v: u64) -> u32 {
    let mut len = 1;
    while v >= 128 {
        v >>= 7;
        len += 1;
    }
    len
}
