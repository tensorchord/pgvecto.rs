use super::wal::Wal;
use super::wal::WalWriter;
use crate::prelude::*;
use dashmap::DashMap;
use std::path::Path;
use tokio::io::ErrorKind;

pub struct Index {
    #[allow(dead_code)]
    id: Id,
    #[allow(dead_code)]
    options: Options,
    algo: Algo1,
    version: IndexVersion,
    wal: WalWriter,
}

impl Index {
    pub async fn drop(id: Id) -> anyhow::Result<()> {
        remove_file_if_exists(format!("{}_wal", id.as_u32())).await?;
        remove_file_if_exists(format!("{}_data", id.as_u32())).await?;
        Ok(())
    }

    pub async fn build(
        id: Id,
        options: Options,
        data: async_channel::Receiver<(Vec<Scalar>, Pointer)>,
    ) -> anyhow::Result<Self> {
        Self::drop(id).await?;
        let mut algo = {
            let algo = Algo0::new(&options.algorithm)?;
            let (tx, rx) = async_channel::bounded(65536);
            tokio::spawn(async move {
                while let Ok((vector, p)) = data.recv().await {
                    let _ = tx.send((vector, p.as_u48() << 16)).await;
                }
            });
            algo.build(options.clone(), rx).await?
        };
        algo.save(format!("{}_data", id.as_u32())).await?;
        let version = IndexVersion::new();
        let wal = {
            let path_wal = format!("{}_wal", id.as_u32());
            let mut wal = Wal::create(path_wal).await?;
            let log = LogMeta {
                options: options.clone(),
            };
            wal.write(&log.serialize()?).await?;
            WalWriter::spawn(wal)?
        };
        Ok(Self {
            id,
            options,
            algo,
            version,
            wal,
        })
    }

    pub async fn load(id: Id) -> anyhow::Result<Self> {
        let mut wal = Wal::open(format!("{}_wal", id.as_u32())).await?;
        let options;
        {
            let log = wal
                .read()
                .await?
                .ok_or(anyhow::anyhow!(Error::IndexIsBroken))?;
            LogMeta { options } = log.deserialize::<LogMeta>()?;
        }
        let algo = Algo0::new(&options.algorithm)?
            .load(options.clone(), format!("{}_data", id.as_u32()))
            .await?;
        let version = IndexVersion::new();
        loop {
            let Some(replay) = wal.read().await? else { break };
            match replay.deserialize::<LogReplay>()? {
                LogReplay::Insert { vector, p } => {
                    algo.insert((vector, version.insert(p))).await?;
                }
                LogReplay::Delete { p } => {
                    version.remove(p);
                }
            }
        }
        wal.truncate().await?;
        wal.flush().await?;
        let wal = WalWriter::spawn(wal)?;
        Ok(Self {
            id,
            options,
            algo,
            version,
            wal,
        })
    }

    pub async fn insert(&self, (vector, p): (Vec<Scalar>, Pointer)) -> anyhow::Result<()> {
        self.algo
            .insert((vector.clone(), self.version.insert(p)))
            .await?;
        let bytes = LogReplay::Insert { vector, p }.serialize()?;
        self.wal.write(bytes).await?;
        Ok(())
    }

    pub async fn delete(&self, delete: Pointer) -> anyhow::Result<()> {
        self.version.remove(delete);
        let bytes = LogReplay::Delete { p: delete }.serialize()?;
        self.wal.write(bytes).await?;
        Ok(())
    }

    pub async fn search(&self, (vector, k): (Vec<Scalar>, usize)) -> anyhow::Result<Vec<Pointer>> {
        let result = self.algo.search((vector, k)).await?;
        Ok(result
            .into_iter()
            .filter_map(|(_, x)| self.version.filter(x))
            .collect())
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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct LogMeta {
    options: Options,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
enum LogReplay {
    Insert { vector: Vec<Scalar>, p: Pointer },
    Delete { p: Pointer },
}

pub struct Load<T> {
    inner: Option<T>,
}

impl<T> Load<T> {
    pub fn new() -> Self {
        Self { inner: None }
    }
    pub fn get(&self) -> Result<&T, Error> {
        self.inner.as_ref().ok_or(Error::IndexIsUnloaded)
    }
    #[allow(dead_code)]
    pub fn get_mut(&mut self) -> Result<&mut T, Error> {
        self.inner.as_mut().ok_or(Error::IndexIsUnloaded)
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
