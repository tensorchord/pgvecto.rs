use byteorder::NativeEndian as N;
use crc32fast::hash as crc32;
use std::path::Path;
use std::thread::JoinHandle;

/*
+----------+-----------+---------+
| CRC (4B) | Size (4B) | Payload |
+----------+-----------+---------+
*/

#[derive(Debug, Clone, Copy)]
pub enum WalStatus {
    Read,
    Truncate,
    Write,
    Flush,
}

pub struct Wal {
    file: std::fs::File,
    offset: usize,
    status: WalStatus,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Self {
        use WalStatus::*;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .expect("Failed to open wal.");
        Self {
            file,
            offset: 0,
            status: Read,
        }
    }
    pub fn create(path: impl AsRef<Path>) -> Self {
        use WalStatus::*;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)
            .expect("Failed to create wal.");
        Self {
            file,
            offset: 0,
            status: Write,
        }
    }
    pub fn read(&mut self) -> Option<Vec<u8>> {
        use byteorder::ReadBytesExt;
        use std::io::Read;
        use WalStatus::*;
        let Read = self.status else {
            panic!("Operation not permitted.")
        };
        macro_rules! resolve_eof {
            ($t: expr) => {
                match $t {
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        self.status = WalStatus::Truncate;
                        return None;
                    }
                    Err(e) => panic!("{}", e),
                    Ok(e) => e,
                }
            };
        }
        let crc = resolve_eof!(self.file.read_u32::<N>());
        let len = resolve_eof!(self.file.read_u32::<N>());
        let mut data = vec![0u8; len as usize];
        resolve_eof!(self.file.read_exact(&mut data));
        if crc32(&data) != crc {
            self.status = WalStatus::Truncate;
            return None;
        }
        self.offset += 4 + 4 + data.len();
        Some(data)
    }
    pub fn truncate(&mut self) {
        use WalStatus::*;
        let Truncate = self.status else {
            panic!("Operation not permitted.")
        };
        self.file
            .set_len(self.offset as _)
            .expect("Failed to truncate wal.");
        self.file.sync_all().expect("Failed to flush wal.");
        self.status = WalStatus::Flush;
    }
    pub fn write(&mut self, bytes: &[u8]) {
        use byteorder::WriteBytesExt;
        use std::io::Write;
        use WalStatus::*;
        let (Write | Flush) = self.status else {
            panic!("Operation not permitted.")
        };
        self.file
            .write_u32::<N>(crc32(bytes))
            .expect("Failed to write wal.");
        self.file
            .write_u32::<N>(bytes.len() as _)
            .expect("Failed to write wal.");
        self.file.write_all(bytes).expect("Failed to write wal.");
        self.offset += 4 + 4 + bytes.len();
        self.status = WalStatus::Write;
    }
    pub fn flush(&mut self) {
        use WalStatus::*;
        let (Write | Flush) = self.status else {
            panic!("Operation not permitted.")
        };
        self.file.sync_all().expect("Failed to flush wal.");
        self.status = WalStatus::Flush;
    }
}

pub struct WalWriter {
    #[allow(dead_code)]
    handle: JoinHandle<Wal>,
    tx: crossbeam::channel::Sender<WalWriterMessage>,
}

impl WalWriter {
    pub fn spawn(wal: Wal) -> WalWriter {
        use WalStatus::*;
        let (Write | Flush) = wal.status else {
            panic!("Operation not permitted.")
        };
        let (tx, rx) = crossbeam::channel::bounded(256);
        let handle = std::thread::spawn(move || thread_wal(wal, rx));
        WalWriter { handle, tx }
    }
    pub fn write(&self, data: Vec<u8>) {
        self.tx
            .send(WalWriterMessage::Write(data))
            .expect("Wal thread exited.");
    }
    pub fn flush(&self) {
        let (tx, rx) = crossbeam::channel::bounded::<!>(0);
        self.tx
            .send(WalWriterMessage::Flush(tx))
            .expect("Wal thread exited.");
        let _ = rx.recv();
    }
    pub fn shutdown(&mut self) {
        let (tx, rx) = crossbeam::channel::bounded::<!>(0);
        self.tx
            .send(WalWriterMessage::Shutdown(tx))
            .expect("Wal thread exited.");
        let _ = rx.recv();
    }
}

enum WalWriterMessage {
    Write(Vec<u8>),
    Flush(crossbeam::channel::Sender<!>),
    Shutdown(crossbeam::channel::Sender<!>),
}

fn thread_wal(mut wal: Wal, rx: crossbeam::channel::Receiver<WalWriterMessage>) -> Wal {
    while let Ok(message) = rx.recv() {
        match message {
            WalWriterMessage::Write(data) => {
                wal.write(&data);
            }
            WalWriterMessage::Flush(_callback) => {
                wal.flush();
            }
            WalWriterMessage::Shutdown(_callback) => {
                wal.flush();
                return wal;
            }
        }
    }
    wal.flush();
    wal
}
