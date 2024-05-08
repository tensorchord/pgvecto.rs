use super::config::{need, LOG_CSV, LOG_JSON, LOG_ERROR};
use super::message::Message;
use std::io::{stderr, Write};

pub fn pipe_log(msg: &str) {
    let mut std_fd = stderr();
    let message = Message::new(msg);
    if need(LOG_CSV) {
        let _ = std_fd.write(&message.csv_chunk()).unwrap();
    }
    if need(LOG_ERROR) {
        let _ = std_fd.write(&message.stderr_chunk()).unwrap();
    }
    if need(LOG_JSON) {
        let _ = std_fd.write(&message.json_chunk()).unwrap();
    }
}
