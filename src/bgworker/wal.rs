use byteorder::NativeEndian as N;
use crc32fast::hash as crc32;
use std::path::Path;

/*
+----------+-----------+---------+
| CRC (4B) | Size (2B) | Payload |
+----------+-----------+---------+
*/

#[derive(Debug, Clone, Copy)]
pub enum WalStatus {
    Read,
    Truncate,
    Write,
    Flush,
}

pub struct WalSync {
    file: std::fs::File,
    offset: usize,
    status: WalStatus,
}

impl WalSync {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        use WalStatus::*;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        Ok(Self {
            file,
            offset: 0,
            status: Read,
        })
    }
    pub fn create(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        use WalStatus::*;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            file,
            offset: 0,
            status: Write,
        })
    }
    pub fn read(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        use byteorder::ReadBytesExt;
        use std::io::Read;
        use std::io::{Error, ErrorKind};
        use WalStatus::*;
        let Read = self.status else { panic!("Operation not permitted.") };
        let maybe_error = (|| -> std::io::Result<Vec<_>> {
            let crc = self.file.read_u32::<N>()?;
            let len = self.file.read_u16::<N>()?;
            let mut data = vec![0u8; len as usize];
            self.file.read_exact(&mut data)?;
            if crc32(&data) == crc {
                self.offset += 4 + 2 + data.len();
                Ok(data)
            } else {
                Err(Error::new(ErrorKind::UnexpectedEof, "Bad crc."))?
            }
        })();
        match maybe_error {
            Ok(data) => Ok(Some(data)),
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                self.status = WalStatus::Truncate;
                Ok(None)
            }
            Err(error) => anyhow::bail!(error),
        }
    }
    pub fn truncate(&mut self) -> anyhow::Result<()> {
        use WalStatus::*;
        let Truncate = self.status else { panic!("Operation not permitted.") };
        self.file.set_len(self.offset as _)?;
        self.file.sync_all()?;
        self.status = WalStatus::Flush;
        Ok(())
    }
    pub fn write(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        use byteorder::WriteBytesExt;
        use std::io::Write;
        use WalStatus::*;
        let (Write | Flush) = self.status else { panic!("Operation not permitted.") };
        self.file.write_u32::<N>(crc32(bytes))?;
        self.file.write_u16::<N>(bytes.len() as _)?;
        self.file.write_all(bytes)?;
        self.offset += 4 + 2 + bytes.len();
        self.status = WalStatus::Write;
        Ok(())
    }
    pub fn flush(&mut self) -> anyhow::Result<()> {
        use WalStatus::*;
        let (Write | Flush) = self.status else { panic!("Operation not permitted.") };
        self.file.sync_all()?;
        self.status = WalStatus::Flush;
        Ok(())
    }
    pub fn into_async(self) -> WalAsync {
        WalAsync {
            file: tokio::fs::File::from_std(self.file),
            offset: self.offset,
            status: self.status,
        }
    }
}

pub struct WalAsync {
    file: tokio::fs::File,
    offset: usize,
    status: WalStatus,
}

impl WalAsync {
    pub async fn write(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        use tokio::io::AsyncWriteExt;
        use WalStatus::*;
        let (Write | Flush) = self.status else { panic!("Operation not permitted.") };
        self.file.write_u32(crc32(bytes)).await?;
        self.file.write_u16(bytes.len() as _).await?;
        self.file.write_all(bytes).await?;
        self.offset += 4 + 2 + bytes.len();
        self.status = WalStatus::Write;
        Ok(())
    }
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        use WalStatus::*;
        let (Write | Flush) = self.status else { panic!("Operation not permitted.") };
        self.file.sync_all().await?;
        self.status = WalStatus::Flush;
        Ok(())
    }
}

enum WalWriterMessage {
    Write(Vec<u8>),
    Flush(tokio::sync::oneshot::Sender<()>),
}

pub struct WalWriter {
    tx: Option<tokio::sync::mpsc::Sender<WalWriterMessage>>,
    handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl WalWriter {
    pub fn spawn(mut wal: WalAsync) -> anyhow::Result<Self> {
        use WalStatus::*;
        anyhow::ensure!(matches!(wal.status, Write | Flush));
        let (tx, mut rx) = tokio::sync::mpsc::channel(4096);
        let handle = tokio::task::spawn(async move {
            while let Some(r) = rx.recv().await {
                use WalWriterMessage::*;
                match r {
                    Write(bytes) => {
                        wal.write(&bytes).await?;
                    }
                    Flush(callback) => {
                        wal.flush().await?;
                        let _ = callback.send(());
                    }
                }
            }
            Ok(())
        });
        Ok(Self {
            tx: Some(tx),
            handle,
        })
    }
    pub async fn write(&self, bytes: Vec<u8>) -> anyhow::Result<()> {
        use WalWriterMessage::*;
        self.tx
            .as_ref()
            .unwrap()
            .send(Write(bytes))
            .await
            .ok()
            .ok_or(anyhow::anyhow!("The WAL thread exited."))?;
        Ok(())
    }
    pub async fn flush(&self) -> anyhow::Result<()> {
        use WalWriterMessage::*;
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .as_ref()
            .unwrap()
            .send(Flush(tx))
            .await
            .ok()
            .ok_or(anyhow::anyhow!("The WAL thread exited."))?;
        rx.await?;
        Ok(())
    }
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        self.tx.take();
        self.handle.await??;
        Ok(())
    }
}
