use super::message::Message;
use std::io::{stderr, Write};

pub fn pipe_msg(msg: &str) -> usize {
    let mut std_fd = stderr();
    if let Ok(buf_size) = std_fd.write(&Message::new(msg).get_chunk()) {
        std_fd.flush().unwrap();
        return buf_size
    }
    0
}
