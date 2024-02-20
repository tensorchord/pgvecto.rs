pub mod metadata;

use crate::instance::*;
use crate::prelude::*;
use crate::utils::clean::clean;
use crate::utils::dir_ops::sync_dir;
use crate::utils::file_atomic::FileAtomic;
use arc_swap::ArcSwap;
use base::worker::*;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

pub struct Worker {
    path: PathBuf,
    protect: Mutex<WorkerProtect>,
    view: ArcSwap<WorkerView>,
}

impl Worker {
    pub fn create(path: PathBuf) -> Arc<Self> {
        std::fs::create_dir(&path).unwrap();
        std::fs::create_dir(path.join("indexes")).unwrap();
        let startup = FileAtomic::create(path.join("startup"), WorkerStartup::new());
        let indexes = HashMap::new();
        let view = Arc::new(WorkerView {
            indexes: indexes.clone(),
        });
        let protect = WorkerProtect { startup, indexes };
        sync_dir(&path);
        self::metadata::Metadata::write(path.join("metadata"));
        Arc::new(Worker {
            path,
            protect: Mutex::new(protect),
            view: ArcSwap::new(view),
        })
    }
    pub fn check(path: PathBuf) -> bool {
        self::metadata::Metadata::read(path.join("metadata")).is_ok()
    }
    pub fn open(path: PathBuf) -> Arc<Self> {
        let startup = FileAtomic::<WorkerStartup>::open(path.join("startup"));
        clean(
            path.join("indexes"),
            startup.get().indexes.iter().map(|s| s.to_string()),
        );
        let mut indexes = HashMap::new();
        for &id in startup.get().indexes.iter() {
            let path = path.join("indexes").join(id.to_string());
            let index = Instance::open(path);
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
    fn view(&self) -> Arc<WorkerView> {
        self.view.load_full()
    }
}

impl WorkerOperations for Worker {
    fn create(&self, handle: Handle, options: IndexOptions) -> Result<(), CreateError> {
        use std::collections::hash_map::Entry;
        let mut protect = self.protect.lock();
        match protect.indexes.entry(handle) {
            Entry::Vacant(o) => {
                let index =
                    Instance::create(self.path.join("indexes").join(handle.to_string()), options)?;
                o.insert(index);
                protect.maintain(&self.view);
                Ok(())
            }
            Entry::Occupied(_) => Err(CreateError::Exist),
        }
    }
    fn drop(&self, handle: Handle) -> Result<(), DropError> {
        let mut protect = self.protect.lock();
        if protect.indexes.remove(&handle).is_some() {
            protect.maintain(&self.view);
            Ok(())
        } else {
            Err(DropError::NotExist)
        }
    }
    fn flush(&self, handle: Handle) -> Result<(), FlushError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(FlushError::NotExist)?;
        let view = instance.view().ok_or(FlushError::Upgrade)?;
        view.flush()?;
        Ok(())
    }
    fn insert(
        &self,
        handle: Handle,
        vector: OwnedVector,
        pointer: Pointer,
    ) -> Result<(), InsertError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(InsertError::NotExist)?;
        loop {
            let view = instance.view().ok_or(InsertError::Upgrade)?;
            match view.insert(vector.clone(), pointer)? {
                Ok(()) => break,
                Err(_) => instance.refresh(),
            }
        }
        Ok(())
    }
    fn delete(&self, handle: Handle, pointer: Pointer) -> Result<(), DeleteError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(DeleteError::NotExist)?;
        let view = instance.view().ok_or(DeleteError::Upgrade)?;
        view.delete(pointer)?;
        Ok(())
    }
    fn view_basic(&self, handle: Handle) -> Result<impl ViewBasicOperations, BasicError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(BasicError::NotExist)?;
        instance.view().ok_or(BasicError::Upgrade)
    }
    fn view_vbase(&self, handle: Handle) -> Result<impl ViewVbaseOperations, VbaseError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(VbaseError::NotExist)?;
        instance.view().ok_or(VbaseError::Upgrade)
    }
    fn view_list(&self, handle: Handle) -> Result<impl ViewListOperations, ListError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(ListError::NotExist)?;
        instance.view().ok_or(ListError::Upgrade)
    }
    fn stat(&self, handle: Handle) -> Result<IndexStat, StatError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(StatError::NotExist)?;
        let stat = instance.stat().ok_or(StatError::Upgrade)?;
        Ok(stat)
    }
}

pub struct WorkerView {
    indexes: HashMap<Handle, Instance>,
}

impl WorkerView {
    pub fn get(&self, handle: Handle) -> Option<&Instance> {
        self.indexes.get(&handle)
    }
}

struct WorkerProtect {
    startup: FileAtomic<WorkerStartup>,
    indexes: HashMap<Handle, Instance>,
}

impl WorkerProtect {
    fn maintain(&mut self, swap: &ArcSwap<WorkerView>) {
        let indexes = self.indexes.keys().copied().collect();
        self.startup.set(WorkerStartup { indexes });
        swap.swap(Arc::new(WorkerView {
            indexes: self.indexes.clone(),
        }));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkerStartup {
    indexes: HashSet<Handle>,
}

impl WorkerStartup {
    pub fn new() -> Self {
        Self {
            indexes: HashSet::new(),
        }
    }
}
