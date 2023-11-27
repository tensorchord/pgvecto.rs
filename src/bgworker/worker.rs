use crate::index::Index;
use crate::index::IndexInsertError;
use crate::index::IndexOptions;
use crate::index::IndexSearchError;
use crate::prelude::*;
use crate::utils::clean::clean;
use crate::utils::dir_ops::sync_dir;
use crate::utils::file_atomic::FileAtomic;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use serde_with::DisplayFromStr;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

fn magic() -> &'static [u8] {
    &[1, 4, 53, 23, 34, 92, 34, 23]
}

fn check(data: &[u8]) -> bool {
    magic() == data
}

pub struct Worker {
    path: PathBuf,
    protect: Mutex<WorkerProtect>,
    view: ArcSwap<WorkerView>,
}

impl Worker {
    pub fn create(path: PathBuf) -> Arc<Self> {
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("magic"), magic()).unwrap();
        std::fs::create_dir(path.join("indexes")).unwrap();
        let startup = FileAtomic::create(path.join("startup"), WorkerStartup::new());
        let indexes = HashMap::new();
        let view = Arc::new(WorkerView {
            indexes: indexes.clone(),
        });
        let delay_reindex = HashSet::new();
        let protect = WorkerProtect {
            startup,
            indexes,
            delay_reindex,
        };
        sync_dir(&path);
        Arc::new(Worker {
            path,
            protect: Mutex::new(protect),
            view: ArcSwap::new(view),
        })
    }
    pub fn open(path: PathBuf) -> Arc<Self> {
        let startup = FileAtomic::<WorkerStartup>::open(path.join("startup"));
        if !check(&std::fs::read(path.join("magic")).unwrap_or_default()) {
            panic!("Please delete the directory pg_vectors in Postgresql data folder. The files are created by older versions of postgresql or broken.");
        }
        clean(
            path.join("indexes"),
            startup.get().indexes.keys().map(|s| s.to_string()),
        );
        let mut indexes = HashMap::new();
        for (&id, options) in startup.get().indexes.iter() {
            let path = path.join("indexes").join(id.to_string());
            let index = Index::open(path, options.clone());
            indexes.insert(id, index);
        }
        let view = Arc::new(WorkerView {
            indexes: indexes.clone(),
        });
        let delay_reindex = HashSet::new();
        let protect = WorkerProtect {
            startup,
            indexes,
            delay_reindex,
        };
        Arc::new(Worker {
            path,
            protect: Mutex::new(protect),
            view: ArcSwap::new(view),
        })
    }
    pub fn call_create(&self, id: Id, options: IndexOptions) {
        log::debug!("create index {}", id);
        let mut protect = self.protect.lock();
        match protect.indexes.entry(id) {
            // This happens when calling reindex without concurrent parameters.
            Entry::Occupied(x) => {
                let old = x.remove();
                // This maintenance operation is used to ensure that the reference count of the index is able to decrement to 0 in order to perform deletion.
                protect.maintain(&self.view);
                let index = Index::recreate(old, options);
                protect.indexes.insert(id, index);
                protect.maintain(&self.view);
                protect.delay_reindex.insert(id);
            }
            Entry::Vacant(x) => {
                let index = Index::create(self.path.join("indexes").join(id.to_string()), options);
                x.insert(index);
                protect.maintain(&self.view);
            }
        }
    }
    pub fn call_search<F>(
        &self,
        id: Id,
        search: (Vec<Scalar>, usize),
        filter: F,
    ) -> Result<Vec<Pointer>, FriendlyError>
    where
        F: FnMut(Pointer) -> bool,
    {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::Index404)?;
        let view = index.view();
        match view.search(search.1, &search.0, filter) {
            Ok(x) => Ok(x),
            Err(IndexSearchError::InvalidVector(x)) => Err(FriendlyError::BadVector(x)),
        }
    }
    pub fn call_insert(&self, id: Id, insert: (Vec<Scalar>, Pointer)) -> Result<(), FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::Index404)?;
        loop {
            let view = index.view();
            match view.insert(insert.0.clone(), insert.1) {
                Ok(()) => break Ok(()),
                Err(IndexInsertError::InvalidVector(x)) => break Err(FriendlyError::BadVector(x)),
                Err(IndexInsertError::OutdatedView(_)) => index.refresh(),
            }
        }
    }
    pub fn call_delete<F>(&self, id: Id, f: F) -> Result<(), FriendlyError>
    where
        F: FnMut(Pointer) -> bool,
    {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::Index404)?;
        let view = index.view();
        view.delete(f);
        Ok(())
    }
    pub fn call_flush(&self, id: Id) -> Result<(), FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::Index404)?;
        let view = index.view();
        view.flush().unwrap();
        Ok(())
    }
    pub fn call_destory(&self, ids: Vec<Id>) {
        let mut updated = false;
        let mut protect = self.protect.lock();
        for id in ids {
            if protect.delay_reindex.remove(&id) {
                log::debug!("delay reindex {}", id);
                continue;
            }
            if protect.indexes.remove(&id).is_some() {
                updated = true;
                log::debug!("destory index {}", id);
            }
        }
        if updated {
            protect.maintain(&self.view);
        }
    }
    pub fn call_stat(&self, id: Id) -> Result<VectorIndexInfo, FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::Index404)?;
        let view = index.view();
        let idx_sealed_len = view.sealed_len();
        let idx_growing_len = view.growing_len();
        let idx_write = view.write_len();
        let res = VectorIndexInfo {
            indexing: index.indexing(),
            idx_tuples: (idx_write + idx_sealed_len + idx_growing_len)
                .try_into()
                .unwrap(),
            idx_sealed_len: idx_sealed_len.try_into().unwrap(),
            idx_growing_len: idx_growing_len.try_into().unwrap(),
            idx_write: idx_write.try_into().unwrap(),
            idx_sealed: view
                .sealed_len_vec()
                .into_iter()
                .map(|x| x.try_into().unwrap())
                .collect(),
            idx_growing: view
                .growing_len_vec()
                .into_iter()
                .map(|x| x.try_into().unwrap())
                .collect(),
            idx_config: serde_json::to_string(index.options()).unwrap(),
        };
        Ok(res)
    }
}

struct WorkerView {
    indexes: HashMap<Id, Arc<Index>>,
}

struct WorkerProtect {
    startup: FileAtomic<WorkerStartup>,
    indexes: HashMap<Id, Arc<Index>>,
    // For reindex operation without concurrent param, postgres will call extra destory, so we need to record the all reindexed indexes.
    delay_reindex: HashSet<Id>,
}

impl WorkerProtect {
    fn maintain(&mut self, swap: &ArcSwap<WorkerView>) {
        let indexes = self
            .indexes
            .iter()
            .map(|(&k, v)| (k, v.options().clone()))
            .collect();
        self.startup.set(WorkerStartup { indexes });
        swap.swap(Arc::new(WorkerView {
            indexes: self.indexes.clone(),
        }));
    }
}

#[serde_with::serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct WorkerStartup {
    #[serde_as(as = "HashMap<DisplayFromStr, _>")]
    indexes: HashMap<Id, IndexOptions>,
}

impl WorkerStartup {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }
}
