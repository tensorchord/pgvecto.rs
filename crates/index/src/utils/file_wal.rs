use byteorder::NativeEndian as N;
use crc32fast::hash as crc32;
use std::path::Path;

/*
+----------+-----------+---------+
| CRC (4B) | Size (4B) | Payload |
+----------+-----------+---------+
*/

pub struct FileWal {
    file: std::fs::File,
    offset: usize,
    status: WalStatus,
}

impl FileWal {
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
    pub fn open(path: impl AsRef<Path>) -> Self {
        use WalStatus::*;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(false)
            .open(path)
            .expect("Failed to open wal.");
        Self {
            file,
            offset: 0,
            status: Read,
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
                        self.status = Truncate;
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
            self.status = Truncate;
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
        self.status = Flush;
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
        self.status = Write;
    }
    pub fn sync_all(&mut self) {
        use WalStatus::*;
        let (Write | Flush) = self.status else {
            panic!("Operation not permitted.")
        };
        self.file.sync_all().expect("Failed to flush wal.");
        self.status = Flush;
    }
}

#[derive(Debug, Clone, Copy)]
enum WalStatus {
    Read,
    Truncate,
    Write,
    Flush,
}
