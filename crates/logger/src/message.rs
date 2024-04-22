use super::config::{PIPE_PROTO_DEST_JSONLOG, PIPE_PROTO_DEST_STDERR};
use chrono::Local;
use serde::{Deserialize, Serialize};
use std::io::Write;

// TODO: follow pg log info
#[derive(Serialize, Deserialize)]
pub struct Message {
    timestamp: String,
    pid: u32,
    message: String,
}

impl Message {
    pub fn new(msg: &str) -> Self {
        Self {
            // TODO: follow postgresq.conf log_timezone
            timestamp: Local::now().format("%y-%m-%d %H:%M:%S%.3f").to_string(),
            pid: 520,
            message: String::from(msg),
        }
    }

    pub fn get_stderr_chunk(&self) -> Vec<u8> {
        return get_pipe_proto_chunk(&self.message, PIPE_PROTO_DEST_STDERR);
        vec![0, 0]
    }

    pub fn get_json_chunk(&self) -> Vec<u8> {
        if let Ok(log_str) = serde_json::to_string(self) {
            return get_pipe_proto_chunk(&log_str, PIPE_PROTO_DEST_JSONLOG);
        }
        vec![0, 0]
    }
}

fn get_pipe_proto_chunk(log_msg: &str, flag: u8) -> Vec<u8> {
    let mut data = Vec::from(log_msg);
    writeln!(&mut data).unwrap();
    let data_len = data.len() as u16;
    // TODO: get pid, just test now
    let pid: i32 = 520;
    let mut chunk = Vec::from([0, 0]);
    chunk.extend_from_slice(&data_len.to_le_bytes());
    chunk.extend_from_slice(&pid.to_le_bytes());
    // default last chunk
    chunk.push(flag | 0x01);
    chunk.extend_from_slice(&data);
    chunk
}
