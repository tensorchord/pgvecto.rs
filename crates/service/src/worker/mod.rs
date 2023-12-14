pub mod instance;

use self::instance::Instance;
use crate::index::IndexOptions;
use crate::index::IndexStat;
use crate::index::OutdatedError;
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
            let index = Instance::open(path, options.clone());
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
        let index = Instance::create(self.path.join("indexes").join(id.to_string()), options);
        if protect.indexes.insert(id, index).is_some() {
            panic!("index {} already exists", id)
        }
        protect.maintain(&self.view);
    }
    pub fn call_search<F>(
        &self,
        id: Id,
        search: (DynamicVector, usize),
        filter: F,
    ) -> Result<Vec<Pointer>, FriendlyError>
    where
        F: FnMut(Pointer) -> bool,
    {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::UnknownIndex)?;
        let view = index.view();
        view.search(search.1, search.0, filter)
    }
    pub fn call_insert(
        &self,
        id: Id,
        insert: (DynamicVector, Pointer),
    ) -> Result<(), FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::UnknownIndex)?;
        loop {
            let view = index.view();
            match view.insert(insert.0.clone(), insert.1)? {
                Ok(()) => break Ok(()),
                Err(OutdatedError(_)) => index.refresh(),
            }
        }
    }
    pub fn call_delete<F>(&self, id: Id, f: F) -> Result<(), FriendlyError>
    where
        F: FnMut(Pointer) -> bool,
    {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::UnknownIndex)?;
        let view = index.view();
        view.delete(f);
        Ok(())
    }
    pub fn call_flush(&self, id: Id) -> Result<(), FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::UnknownIndex)?;
        let view = index.view();
        view.flush();
        Ok(())
    }
    pub fn call_destory(&self, ids: Vec<Id>) {
        let mut updated = false;
        let mut protect = self.protect.lock();
        for id in ids {
            updated |= protect.indexes.remove(&id).is_some();
        }
        if updated {
            protect.maintain(&self.view);
        }
    }
    pub fn call_stat(&self, id: Id) -> Result<IndexStat, FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::UnknownIndex)?;
        Ok(index.stat())
    }
    pub fn get_instance(&self, id: Id) -> Result<Instance, FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).ok_or(FriendlyError::UnknownIndex)?;
        Ok(index.clone())
    }
}

struct WorkerView {
    indexes: HashMap<Id, Instance>,
}

struct WorkerProtect {
    startup: FileAtomic<WorkerStartup>,
    indexes: HashMap<Id, Instance>,
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
