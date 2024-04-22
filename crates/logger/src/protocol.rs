use super::config::{need_csv, need_json, need_stderr};
use super::message::Message;
use std::io::{stderr, Write};

pub fn pipe_msg(msg: &str) -> usize {
    let mut std_fd = stderr();
    if need_csv() {
        // TODO: CSV
    }
    if need_stderr() {
        if let Ok(buf_size) = std_fd.write(&Message::new(msg).get_stderr_chunk()) {
            std_fd.flush().unwrap();
        }
    }
    if need_json() {
        if let Ok(buf_size) = std_fd.write(&Message::new(msg).get_json_chunk()) {
            std_fd.flush().unwrap();
        }
    }
    0
}
