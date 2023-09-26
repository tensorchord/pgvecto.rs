use super::filter_delete::FilterDelete;
use super::storage::Storage;
use super::storage::StoragePreallocator;
use super::vectors::Vectors;
use super::wal::Wal;
use super::wal::WalWriter;
use crate::algorithms::Algorithm;
use crate::algorithms::AlgorithmError;
use crate::algorithms::AlgorithmOptions;
use crate::bgworker::vectors::VectorsOptions;
use crate::ipc::server::Build;
use crate::ipc::server::Search;
use crate::ipc::ServerIpcError;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use validator::Validate;

#[derive(Debug, Clone, Serialize, Deserialize, Error)]
pub enum IndexError {
    #[error("Algorithm {0}")]
    Algorithm(#[from] AlgorithmError),
    #[error("Ipc {0}")]
    Ipc(#[from] ServerIpcError),
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct IndexOptions {
    #[validate(range(min = 1))]
    pub dims: u16,
    #[serde(rename = "distance")]
    pub d: Distance,
    #[validate(range(min = 1))]
    pub capacity: usize,
    pub vectors: VectorsOptions,
    pub algorithm: AlgorithmOptions,
}

pub struct Index {
    #[allow(dead_code)]
    id: Id,
    #[allow(dead_code)]
    options: IndexOptions,
    vectors: Arc<Vectors>,
    algo: Algorithm,
    filter_delete: FilterDelete,
    wal: WalWriter,
    #[allow(dead_code)]
    storage: Storage,
}

impl Index {
    pub fn clean(id: Id) {
        for f in std::fs::read_dir(".").expect("Failed to clean.") {
            let f = f.unwrap();
            if let Some(filename) = f.file_name().to_str() {
                if filename.starts_with(&format!("{}_", id)) {
                    remove_file_if_exists(filename).expect("Failed to delete.");
                }
            }
        }
    }
    pub fn prebuild(options: IndexOptions) -> Result<StoragePreallocator, IndexError> {
        let mut storage = StoragePreallocator::new();
        Vectors::prebuild(&mut storage, options.clone());
        Algorithm::prebuild(&mut storage, options.clone())?;
        Ok(storage)
    }
    pub fn build(
        id: Id,
        options: IndexOptions,
        server_build: &mut Build,
    ) -> Result<Self, IndexError> {
        Self::clean(id);
        let storage_preallocator = Self::prebuild(options.clone())?;
        let mut storage = Storage::build(id, storage_preallocator);
        let vectors = Arc::new(Vectors::build(&mut storage, options.clone()));
        while let Some((vector, p)) = server_build.next().expect("IPC error.") {
            let data = p.as_u48() << 16;
            vectors.put(data, &vector);
        }
        let algo = Algorithm::build(
            &mut storage,
            options.clone(),
            vectors.clone(),
            vectors.len(),
        )?;
        storage.persist();
        let filter_delete = FilterDelete::new();
        let wal = {
            let path_wal = format!("{}_wal", id);
            let mut wal = Wal::create(path_wal);
            let log = LogFirst {
                options: options.clone(),
            };
            wal.write(&log.bincode());
            wal
        };
        Ok(Self {
            id,
            options,
            vectors,
            algo,
            filter_delete,
            wal: WalWriter::spawn(wal),
            storage,
        })
    }

    pub fn load(id: Id) -> Self {
        let mut storage = Storage::load(id);
        let mut wal = Wal::open(format!("{}_wal", id));
        let LogFirst { options } = wal
            .read()
            .expect("The index is broken.")
            .deserialize::<LogFirst>();
        let vectors = Arc::new(Vectors::load(&mut storage, options.clone()));
        let algo = Algorithm::load(&mut storage, options.clone(), vectors.clone())
            .expect("Failed to load the algorithm.");
        let filter_delete = FilterDelete::new();
        loop {
            let Some(replay) = wal.read() else { break };
            match replay.deserialize::<LogFollowing>() {
                LogFollowing::Insert { vector, p } => {
                    let data = filter_delete.on_inserting(p);
                    let index = vectors.put(data, &vector);
                    algo.insert(index).expect("Failed to reinsert.");
                }
                LogFollowing::Delete { p } => {
                    filter_delete.on_deleting(p);
                }
            }
        }
        wal.truncate();
        wal.flush();
        Self {
            id,
            options,
            algo,
            filter_delete,
            wal: WalWriter::spawn(wal),
            vectors,
            storage,
        }
    }

    pub fn insert(&self, (vector, p): (Box<[Scalar]>, Pointer)) -> Result<(), IndexError> {
        let data = self.filter_delete.on_inserting(p);
        let index = self.vectors.put(data, &vector);
        self.algo.insert(index)?;
        let bytes = LogFollowing::Insert { vector, p }.bincode();
        self.wal.write(bytes);
        Ok(())
    }

    pub fn delete(&self, delete: Pointer) -> Result<(), IndexError> {
        self.filter_delete.on_deleting(delete);
        let bytes = LogFollowing::Delete { p: delete }.bincode();
        self.wal.write(bytes);
        Ok(())
    }

    pub fn search(
        &self,
        target: Box<[Scalar]>,
        k: usize,
        server_search: &mut Search,
    ) -> Result<Vec<Pointer>, IndexError> {
        let filter = |p| {
            if let Some(p) = self.filter_delete.filter(p) {
                server_search.check(p).expect("IPC error.")
            } else {
                false
            }
        };
        let result = self.algo.search(target, k, filter)?;
        let result = result
            .into_iter()
            .filter_map(|(_, x)| self.filter_delete.filter(x))
            .collect();
        Ok(result)
    }

    pub fn flush(&self) {
        self.wal.flush();
    }

    pub fn shutdown(&mut self) {
        self.wal.shutdown();
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LogFirst {
    options: IndexOptions,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
enum LogFollowing {
    Insert { vector: Box<[Scalar]>, p: Pointer },
    Delete { p: Pointer },
}

fn remove_file_if_exists(path: impl AsRef<Path>) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

trait BincodeDeserialize {
    fn deserialize<'a, T: Deserialize<'a>>(&'a self) -> T;
}

impl BincodeDeserialize for [u8] {
    fn deserialize<'a, T: Deserialize<'a>>(&'a self) -> T {
        bincode::deserialize::<T>(self).expect("Failed to deserialize.")
    }
}

trait Bincode: Sized {
    fn bincode(&self) -> Vec<u8>;
}

impl<T: Serialize> Bincode for T {
    fn bincode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize.")
    }
}
