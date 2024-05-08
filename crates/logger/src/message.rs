use super::config::{
    LAST_CHUNK, PIPE_PROTO_DEST_CSVLOG, PIPE_PROTO_DEST_JSONLOG, PIPE_PROTO_DEST_STDERR,
};
use chrono::Local;
use serde::Serialize;
use std::io::Write;
use std::{process, vec};

#[derive(Serialize)]
pub struct Message {
    timestamp: String,
    pid: u32,
    message: String,
}

impl Message {
    pub fn new(msg: &str) -> Self {
        Self {
            timestamp: Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            pid: process::id(),
            message: String::from(msg),
        }
    }

    pub fn csv_chunk(&self) -> Vec<u8> {
        let mut wtr = csv::Writer::from_writer(vec![]);
        wtr.serialize(self).unwrap();
        return get_protocol_chunk(
            &String::from_utf8(wtr.into_inner().unwrap()).unwrap(),
            PIPE_PROTO_DEST_CSVLOG,
        );
    }

    pub fn stderr_chunk(&self) -> Vec<u8> {
        let err = format!("{} [{}] LOG:  {}", self.timestamp, self.pid, self.message);
        return get_protocol_chunk(&err, PIPE_PROTO_DEST_STDERR);
    }

    pub fn json_chunk(&self) -> Vec<u8> {
        let json = serde_json::to_string(self).unwrap();
        return get_protocol_chunk(&json, PIPE_PROTO_DEST_JSONLOG);
    }
}

fn get_protocol_chunk(log_msg: &str, flag: u8) -> Vec<u8> {
    let mut chunk = Vec::from([0, 0]);
    let mut data: Vec<u8> = Vec::new();
    let pid = process::id();
    writeln!(&mut data, "{}", &log_msg).unwrap();
    let data_len = data.len() as u16;
    chunk.extend_from_slice(&data_len.to_le_bytes());
    chunk.extend_from_slice(&pid.to_le_bytes());
    chunk.push(flag | LAST_CHUNK);
    chunk.extend_from_slice(&data);
    chunk
}
