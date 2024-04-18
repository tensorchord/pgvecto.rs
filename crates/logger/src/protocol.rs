use std::io::{self, stderr, Write};

struct PipeProtoHeader {
    nuls: [u8; 2],
    len: u16,
    pid: i32,
    flag: u8,
}

pub fn pipe_msg(msg: &str, flag: u8) -> u32 {
    let mut std_fd = stderr();
    let msg_proto = PipeProtoHeader {
        nuls: [0, 0],
        len: msg.len() as u16,
        pid: 520,
        flag,
    };
    // std_fd.write(&[0, 0]).unwrap();
    // let length: u16 = 13;
    // std_fd.write(length.to_string().as_bytes()).unwrap();
    // let flag: u8 = 0x41;
    // let pid = 520;
    // std_fd.write(pid.to_string().as_bytes()).unwrap();
    // std_fd.write(&[flag]).unwrap();
    std_fd.write_all(&[0, 0, 13, 0, 10, 0, 0, 0, 0x41, 104, 101, 108, 108, 111, 119, 111, 114, 108, 100, 112, 106, 119]).unwrap();
    std_fd.flush().unwrap();
    msg.len() as u32
}