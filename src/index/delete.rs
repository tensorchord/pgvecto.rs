use crate::prelude::*;
use crate::utils::file_wal::FileWal;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;

pub struct Delete {
    version: DashMap<Pointer, u16>,
    wal: Mutex<FileWal>,
}

impl Delete {
    pub fn create(path: PathBuf) -> Arc<Self> {
        let wal = FileWal::create(path);
        let version = DashMap::new();
        Arc::new(Self {
            version,
            wal: wal.into(),
        })
    }
    pub fn open(path: PathBuf) -> Arc<Self> {
        let mut wal = FileWal::open(path);
        let version = DashMap::<Pointer, u16>::new();
        while let Some(log) = wal.read() {
            let log = bincode::deserialize::<Log>(&log).unwrap();
            let key = log.key;
            match version.entry(key) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() += 1;
                }
                Entry::Vacant(e) => {
                    e.insert(1);
                }
            }
        }
        wal.truncate();
        Arc::new(Self {
            version,
            wal: wal.into(),
        })
    }
    pub fn check(&self, data: u64) -> Option<Pointer> {
        let pointer = Pointer::from_u48(data >> 16);
        match self.version.get(&pointer) {
            Some(e) => {
                if (data as u16) == *e {
                    Some(pointer)
                } else {
                    None
                }
            }
            None => Some(pointer),
        }
    }
    pub fn delete(&self, key: Pointer) {
        match self.version.entry(key) {
            Entry::Occupied(mut e) => {
                *e.get_mut() += 1;
                let mut wal = self.wal.lock();
                wal.write(&bincode::serialize(&Log { key }).unwrap());
            }
            Entry::Vacant(e) => {
                e.insert(1);
                let mut wal = self.wal.lock();
                wal.write(&bincode::serialize(&Log { key }).unwrap());
            }
        }
    }
    pub fn version(&self, key: Pointer) -> u16 {
        match self.version.get(&key) {
            Some(e) => *e,
            None => 0,
        }
    }
    pub fn flush(&self) {
        self.wal.lock().sync_all();
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Log {
    key: Pointer,
}
