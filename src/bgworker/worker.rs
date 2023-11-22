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
use std::collections::HashMap;
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
        let protect = WorkerProtect { startup, indexes };
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
        let protect = WorkerProtect { startup, indexes };
        Arc::new(Worker {
            path,
            protect: Mutex::new(protect),
            view: ArcSwap::new(view),
        })
    }
    pub fn call_create(&self, id: Id, options: IndexOptions) {
        let mut protect = self.protect.lock();
        let index = Index::create(self.path.join("indexes").join(id.to_string()), options);
        protect.indexes.insert(id, index);
        protect.maintain(&self.view);
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
    pub fn call_destory(&self, id: Id) {
        let mut protect = self.protect.lock();
        protect.indexes.remove(&id);
        protect.maintain(&self.view);
    }
    pub fn call_stat_indexing(&self, id: Id) -> Result<bool, FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::Index404)?;
        let indexing = index.indexing();
        Ok(indexing)
    }
    pub fn call_stat_tuples(&self, id: Id) -> Result<u32, FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::Index404)?;
        let len = index.view().len();
        Ok(len)
    }
    pub fn call_stat_tuples_done(&self, id: Id) -> Result<u32, FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::Index404)?;
        let len = index.view().sealed_len();
        Ok(len)
    }
    pub fn call_stat_config(&self, id: Id) -> Result<String, FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::Index404)?;
        let config = serde_json::to_string(index.options()).unwrap();
        Ok(config)
    }
}

struct WorkerView {
    indexes: HashMap<Id, Arc<Index>>,
}

struct WorkerProtect {
    startup: FileAtomic<WorkerStartup>,
    indexes: HashMap<Id, Arc<Index>>,
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
