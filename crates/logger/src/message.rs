use chrono::Local;
use std::io::Write;
use serde::{Deserialize, Serialize};

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

    pub fn get_chunk(&self) -> Vec<u8> {
        if let Ok(log_str) = serde_json::to_string(self) {
            return get_pipe_proto_chunk(&log_str);
        }
        vec![0, 0]
    }
}

fn get_pipe_proto_chunk(log_msg: &str) -> Vec<u8> {
    let mut data = Vec::from(log_msg);
    writeln!(&mut data).unwrap();
    let data_len = data.len() as u16;
    // TODO: GET PID, just test now
    let pid: i32 = 520;
    // TODO: get flag, just test now, fiexd 0x41, representing jsonlog and last chunk
    let flag = 0x41;
    let mut chunk = Vec::from([0, 0]);
    chunk.extend_from_slice(&data_len.to_le_bytes());
    chunk.extend_from_slice(&pid.to_le_bytes());
    chunk.push(flag);
    chunk.extend_from_slice(&data);
    chunk
}
