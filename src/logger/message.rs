use chrono::Local;
use pgrx::pg_sys::PipeProtoChunk;
use serde::Serialize;
use std::io::{stderr, Write};
use std::{process, vec};

const PIPE_PROTO_DEST_STDERR: u8 = 0x10;
const PIPE_PROTO_DEST_CSVLOG: u8 = 0x20;
const PIPE_PROTO_DEST_JSONLOG: u8 = 0x40;
const PIPE_PROTO_IS_LAST: u8 = 0x01;

const PIPE_CHUNK_SIZE: usize = std::mem::size_of::<PipeProtoChunk>();
const PIPE_HEADER_SIZE: usize = 9;
const PIPE_MAX_PAYLOAD: usize = PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE;

struct PipeProtoHeader {
    nuls: [u8; 2],
    len: u16,
    pid: i32,
    flags: u8,
}

impl PipeProtoHeader {
    fn new(flags: u8) -> Self {
        Self {
            nuls: [0, 0],
            len: 0,
            pid: process::id() as i32,
            flags,
        }
    }

    fn chunk(&self) -> Vec<u8> {
        let mut chunk = Vec::from(&self.nuls);
        chunk.extend_from_slice(&self.len.to_le_bytes());
        chunk.extend_from_slice(&self.pid.to_le_bytes());
        chunk.push(self.flags);
        chunk
    }

    fn set_flag(&mut self, flags: u8) {
        self.flags = flags
    }

    fn set_len(&mut self, len: usize) {
        self.len = len as u16;
    }
}

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

    pub fn csv_chunk(&self) {
        let mut wtr = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(vec![]);
        wtr.serialize(self).unwrap();
        write_pipe_chunks(wtr.into_inner().unwrap(), PIPE_PROTO_DEST_CSVLOG)
    }

    pub fn stderr_chunk(&self) {
        let err = format!("{} [{}] LOG:  {}", self.timestamp, self.pid, self.message);
        let mut data: Vec<u8> = Vec::new();
        writeln!(&mut data, "{}", &err).unwrap();
        write_pipe_chunks(data, PIPE_PROTO_DEST_STDERR)
    }

    pub fn json_chunk(&self) {
        let json = serde_json::to_string(self).unwrap();
        let mut data: Vec<u8> = Vec::new();
        writeln!(&mut data, "{}", &json).unwrap();
        write_pipe_chunks(data, PIPE_PROTO_DEST_JSONLOG)
    }
}

fn write_pipe_chunks(msg_buf: Vec<u8>, flags: u8) {
    let mut len = msg_buf.len();
    let mut header = PipeProtoHeader::new(flags);
    let mut fd = stderr();
    let mut cursor: usize = 0;
    while len > PIPE_MAX_PAYLOAD {
        header.set_len(PIPE_MAX_PAYLOAD);
        let data = &msg_buf[cursor..(cursor + PIPE_MAX_PAYLOAD)];
        let mut chunk = Vec::new();
        chunk.extend_from_slice(&header.chunk());
        chunk.extend_from_slice(data);
        let _ = fd.write(&chunk).unwrap();
        len -= PIPE_MAX_PAYLOAD;
        cursor += PIPE_MAX_PAYLOAD;
    }
    header.set_flag(flags | PIPE_PROTO_IS_LAST);
    header.set_len(len);
    let data = &msg_buf[cursor..(cursor + len)];
    let mut chunk = Vec::new();
    chunk.extend_from_slice(&header.chunk());
    chunk.extend_from_slice(data);
    let _ = fd.write(&chunk).unwrap();
}
