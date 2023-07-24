use super::wal::WalSync;
use super::wal::WalWriter;
use crate::algorithms::DynAlgorithm;
use crate::algorithms::Vectors;
use crate::memory::given;
use crate::memory::Address;
use crate::memory::Context;
use crate::memory::ContextOptions;
use crate::prelude::*;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::ptr::NonNull;
use std::sync::Arc;
use tokio::io::ErrorKind;
use tokio_stream::StreamExt;

pub struct Index {
    #[allow(dead_code)]
    id: Id,
    #[allow(dead_code)]
    options: Options,
    vectors: Arc<Vectors>,
    algo: DynAlgorithm,
    version: IndexVersion,
    wal: WalWriter,
    #[allow(dead_code)]
    context: Arc<Context>,
}

impl Index {
    pub async fn drop(id: Id) -> anyhow::Result<()> {
        use tokio_stream::wrappers::ReadDirStream;
        let mut stream = ReadDirStream::new(tokio::fs::read_dir(".").await?);
        while let Some(f) = stream.next().await {
            let filename = f?
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("Bad filename."))?;
            if filename.starts_with(&format!("{}_", id.as_u32())) {
                remove_file_if_exists(filename).await?;
            }
        }
        Ok(())
    }
    pub async fn build(
        id: Id,
        options: Options,
        data: async_channel::Receiver<(Box<[Scalar]>, Pointer)>,
    ) -> anyhow::Result<Self> {
        Self::drop(id).await?;
        tokio::task::block_in_place(|| -> anyhow::Result<_> {
            let context = Context::build(ContextOptions {
                block_ram: (options.size_ram, format!("{}_data_ram", id.as_u32())),
                block_disk: (options.size_disk, format!("{}_data_disk", id.as_u32())),
            })?;
            let _given = unsafe { given(NonNull::new_unchecked(Arc::as_ptr(&context).cast_mut())) };
            let vectors = Arc::new(Vectors::build(options.clone())?);
            while let Ok((vector, p)) = data.recv_blocking() {
                let data = p.as_u48() << 16;
                vectors.put(data, &vector)?;
            }
            let algo = DynAlgorithm::build(options.clone(), vectors.clone(), vectors.len())?;
            context.persist()?;
            let version = IndexVersion::new();
            let wal = {
                let path_wal = format!("{}_wal", id.as_u32());
                let mut wal = WalSync::create(path_wal)?;
                let log = LogMeta {
                    options: options.clone(),
                    address_algorithm: algo.address(),
                    address_vectors: vectors.address(),
                };
                wal.write(&log.bincode()?)?;
                WalWriter::spawn(wal.into_async())?
            };
            Ok(Self {
                id,
                options,
                vectors,
                algo,
                version,
                wal,
                context,
            })
        })
    }

    pub async fn load(id: Id) -> anyhow::Result<Self> {
        tokio::task::block_in_place(|| {
            let mut wal = WalSync::open(format!("{}_wal", id.as_u32()))?;
            let LogMeta {
                options,
                address_vectors,
                address_algorithm,
            } = wal
                .read()?
                .ok_or(anyhow::anyhow!("The index is broken."))?
                .deserialize::<LogMeta>()?;
            let context = Context::load(ContextOptions {
                block_ram: (options.size_ram, format!("{}_data_ram", id.as_u32())),
                block_disk: (options.size_disk, format!("{}_data_disk", id.as_u32())),
            })?;
            let _given = unsafe { given(NonNull::new_unchecked(Arc::as_ptr(&context).cast_mut())) };
            let vectors = Arc::new(Vectors::load(options.clone(), address_vectors)?);
            let algo = DynAlgorithm::load(options.clone(), vectors.clone(), address_algorithm)?;
            let version = IndexVersion::new();
            loop {
                let Some(replay) = wal.read()? else { break };
                match replay.deserialize::<LogReplay>()? {
                    LogReplay::Insert { vector, p } => {
                        let data = version.insert(p);
                        let index = vectors.put(data, &vector)?;
                        algo.insert(index)?;
                    }
                    LogReplay::Delete { p } => {
                        version.remove(p);
                    }
                }
            }
            wal.truncate()?;
            wal.flush()?;
            let wal = WalWriter::spawn(wal.into_async())?;
            Ok(Self {
                id,
                options,
                algo,
                version,
                wal,
                vectors,
                context,
            })
        })
    }

    pub async fn insert(&self, (vector, p): (Box<[Scalar]>, Pointer)) -> anyhow::Result<()> {
        tokio::task::block_in_place(|| -> anyhow::Result<()> {
            let _given = unsafe {
                given(NonNull::new_unchecked(
                    Arc::as_ptr(&self.context).cast_mut(),
                ))
            };
            let data = self.version.insert(p);
            let index = self.vectors.put(data, &vector)?;
            self.algo.insert(index)?;
            anyhow::Result::Ok(())
        })?;
        let bytes = LogReplay::Insert { vector, p }.bincode()?;
        self.wal.write(bytes).await?;
        Ok(())
    }

    pub async fn delete(&self, delete: Pointer) -> anyhow::Result<()> {
        self.version.remove(delete);
        let bytes = LogReplay::Delete { p: delete }.bincode()?;
        self.wal.write(bytes).await?;
        Ok(())
    }

    pub async fn search(&self, search: (Box<[Scalar]>, usize)) -> anyhow::Result<Vec<Pointer>> {
        let result = tokio::task::block_in_place(|| -> anyhow::Result<_> {
            let _given = unsafe {
                given(NonNull::new_unchecked(
                    Arc::as_ptr(&self.context).cast_mut(),
                ))
            };
            let result = self.algo.search(search)?;
            let result = result
                .into_iter()
                .filter_map(|(_, x)| self.version.filter(x))
                .collect();
            Ok(result)
        })?;
        Ok(result)
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        self.wal.flush().await?;
        Ok(())
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.wal.shutdown().await?;
        Ok(())
    }
}

struct IndexVersion {
    data: DashMap<Pointer, (u16, bool)>,
}

impl IndexVersion {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }
    pub fn filter(&self, x: u64) -> Option<Pointer> {
        let p = Pointer::from_u48(x >> 16);
        let v = x as u16;
        if let Some(guard) = self.data.get(&p) {
            let (cv, cve) = guard.value();
            debug_assert!(v < *cv || (v == *cv && *cve));
            if v == *cv {
                Some(p)
            } else {
                None
            }
        } else {
            debug_assert!(v == 0);
            Some(p)
        }
    }
    pub fn insert(&self, p: Pointer) -> u64 {
        if let Some(mut guard) = self.data.get_mut(&p) {
            let (cv, cve) = guard.value_mut();
            debug_assert!(*cve == false);
            *cve = true;
            p.as_u48() << 16 | *cv as u64
        } else {
            self.data.insert(p, (0, true));
            p.as_u48() << 16 | 0
        }
    }
    pub fn remove(&self, p: Pointer) {
        if let Some(mut guard) = self.data.get_mut(&p) {
            let (cv, cve) = guard.value_mut();
            if *cve == true {
                *cv = *cv + 1;
                *cve = false;
            }
        } else {
            self.data.insert(p, (1, false));
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogMeta {
    options: Options,
    address_vectors: Address,
    address_algorithm: Address,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum LogReplay {
    Insert { vector: Box<[Scalar]>, p: Pointer },
    Delete { p: Pointer },
}

pub struct Load<T> {
    inner: Option<T>,
}

impl<T> Load<T> {
    pub fn new() -> Self {
        Self { inner: None }
    }
    pub fn get(&self) -> anyhow::Result<&T> {
        self.inner
            .as_ref()
            .ok_or(anyhow::anyhow!("The index is not loaded."))
    }
    #[allow(dead_code)]
    pub fn get_mut(&mut self) -> anyhow::Result<&mut T> {
        self.inner
            .as_mut()
            .ok_or(anyhow::anyhow!("The index is not loaded."))
    }
    pub fn load(&mut self, x: T) {
        assert!(self.inner.is_none());
        self.inner = Some(x);
    }
    pub fn unload(&mut self) -> T {
        assert!(self.inner.is_some());
        self.inner.take().unwrap()
    }
    pub fn is_loaded(&self) -> bool {
        self.inner.is_some()
    }
    pub fn is_unloaded(&self) -> bool {
        self.inner.is_none()
    }
}

async fn remove_file_if_exists(path: impl AsRef<Path>) -> std::io::Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
