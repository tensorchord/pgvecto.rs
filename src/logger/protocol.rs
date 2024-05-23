use super::message::Message;
use pgrx::pg_sys::{
    Log_destination, LOG_DESTINATION_CSVLOG, LOG_DESTINATION_JSONLOG, LOG_DESTINATION_STDERR,
};
use std::io::{stderr, Write};

pub fn pipe_log(msg: &str) {
    let mut std_fd = stderr();
    let message = Message::new(msg);
    unsafe {
        if Log_destination as u32 & LOG_DESTINATION_CSVLOG != 0 {
            let _ = std_fd.write(&message.csv_chunk()).unwrap();
        }
        if Log_destination as u32 & LOG_DESTINATION_STDERR != 0 {
            let _ = std_fd.write(&message.stderr_chunk()).unwrap();
        }
        if Log_destination as u32 & LOG_DESTINATION_JSONLOG != 0 {
            let _ = std_fd.write(&message.json_chunk()).unwrap();
        }
    }
}
