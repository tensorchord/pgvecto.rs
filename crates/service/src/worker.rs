use crate::instance::*;
use arc_swap::ArcSwap;
use base::index::*;
use base::search::*;
use base::vector::*;
use base::worker::*;
use common::clean::clean;
use common::dir_ops::sync_dir;
use common::file_atomic::FileAtomic;
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
        Arc::new(Worker {
            path,
            protect: Mutex::new(protect),
            view: ArcSwap::new(view),
        })
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
                index.start();
                o.insert(index);
                protect.maintain(&self.view);
                Ok(())
            }
            // reindex
            Entry::Occupied(o) => {
                {
                    let index = o.remove();
                    protect.maintain(&self.view);
                    index.stop();
                    let tracker = index.wait();
                    drop(index);
                    loop {
                        if Arc::strong_count(&tracker) == 1 {
                            break;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    drop(tracker);
                }
                {
                    let index = Instance::create(
                        self.path.join("indexes").join(handle.to_string()),
                        options,
                    )?;
                    index.start();
                    protect.indexes.insert(handle, index);
                    protect.maintain(&self.view);
                }
                Ok(())
            }
        }
    }
    fn drop(&self, handle: Handle) -> Result<(), DropError> {
        let mut protect = self.protect.lock();
        if let Some(index) = protect.indexes.remove(&handle) {
            protect.maintain(&self.view);
            index.stop();
            let tracker = index.wait();
            drop(index);
            loop {
                if Arc::strong_count(&tracker) == 1 {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            drop(tracker);
            Ok(())
        } else {
            Err(DropError::NotExist)
        }
    }
    fn flush(&self, handle: Handle) -> Result<(), FlushError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(FlushError::NotExist)?;
        let view = instance.view();
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
            let view = instance.view();
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
        let view = instance.view();
        view.delete(pointer)?;
        Ok(())
    }
    fn view_basic(&self, handle: Handle) -> Result<impl ViewBasicOperations, BasicError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(BasicError::NotExist)?;
        Ok(instance.view())
    }
    fn view_vbase(&self, handle: Handle) -> Result<impl ViewVbaseOperations, VbaseError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(VbaseError::NotExist)?;
        Ok(instance.view())
    }
    fn view_list(&self, handle: Handle) -> Result<impl ViewListOperations, ListError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(ListError::NotExist)?;
        Ok(instance.view())
    }
    fn stat(&self, handle: Handle) -> Result<IndexStat, StatError> {
        let view = self.view();
        let instance = view.get(handle).ok_or(StatError::NotExist)?;
        let stat = instance.stat();
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
