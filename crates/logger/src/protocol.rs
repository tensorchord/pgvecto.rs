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
    std_fd.write(&msg_proto.nuls).unwrap();
    std_fd.write(&msg_proto.len.to_be_bytes()).unwrap();
    std_fd.write(&msg_proto.pid.to_be_bytes()).unwrap();
    std_fd.write(&[msg_proto.flag]).unwrap();
    std_fd.write(&msg.as_bytes()).unwrap();
    std_fd.flush().unwrap();
    msg.len() as u32
}