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
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(vec![]);
        wtr.serialize(self).unwrap();
        get_protocol_chunk(wtr.into_inner().unwrap(), PIPE_PROTO_DEST_CSVLOG)
    }

    pub fn stderr_chunk(&self) -> Vec<u8> {
        let err = format!("{} [{}] LOG:  {}", self.timestamp, self.pid, self.message);
        let mut data: Vec<u8> = Vec::new();
        writeln!(&mut data, "{}", &err).unwrap();
        get_protocol_chunk(data, PIPE_PROTO_DEST_STDERR)
    }

    pub fn json_chunk(&self) -> Vec<u8> {
        let json = serde_json::to_string(self).unwrap();
        let mut data: Vec<u8> = Vec::new();
        writeln!(&mut data, "{}", &json).unwrap();
        get_protocol_chunk(data, PIPE_PROTO_DEST_JSONLOG)
    }
}

fn get_protocol_chunk(msg_buf: Vec<u8>, flag: u8) -> Vec<u8> {
    let mut chunk = Vec::from([0, 0]);
    let pid = process::id();
    let data_len = msg_buf.len() as u16;
    chunk.extend_from_slice(&data_len.to_le_bytes());
    chunk.extend_from_slice(&pid.to_le_bytes());
    chunk.push(flag | LAST_CHUNK);
    chunk.extend_from_slice(&msg_buf);
    chunk
}
