pub mod metadata;
use crate::index::IndexOptions;
#[double]
use crate::instance::Instance;
use crate::prelude::*;
use crate::utils::clean::clean;
use crate::utils::dir_ops::sync_dir;
use crate::utils::file_atomic::FileAtomic;
use arc_swap::ArcSwap;
use mockall_double::double;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(test)]
use mockall::automock;

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
        let protect = WorkerProtect::create(startup, indexes);
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
        let protect = WorkerProtect::create(startup, indexes);
        Arc::new(Worker {
            path,
            protect: Mutex::new(protect),
            view: ArcSwap::new(view),
        })
    }
    pub fn view(&self) -> Arc<WorkerView> {
        self.view.load_full()
    }
    pub fn index_create(&self, handle: Handle, options: IndexOptions) -> Result<(), ServiceError> {
        use std::collections::hash_map::Entry;
        let mut protect = self.protect.lock();
        match protect.indexes.entry(handle) {
            Entry::Vacant(o) => {
                let path = self.path.join("indexes").join(handle.to_string());
                let index = Instance::create(path, options)?;
                o.insert(index);
                protect.maintain(&self.view);
                Ok(())
            }
            Entry::Occupied(_) => Err(ServiceError::KnownIndex),
        }
    }
    pub fn index_destroy(&self, handle: Handle) {
        let mut protect = self.protect.lock();
        if protect.indexes.remove(&handle).is_some() {
            protect.maintain(&self.view);
        }
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

#[cfg_attr(test, automock)]
impl WorkerProtect {
    fn create(startup: FileAtomic<WorkerStartup>, indexes: HashMap<Handle, Instance>) -> Self {
        Self { startup, indexes }
    }
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

#[cfg(test)]
mod tests {
    use super::Handle;
    use super::MockWorkerProtect;
    use super::Worker;
    use super::WorkerProtect;
    use crate::index::indexing::IndexingOptions;
    use crate::index::optimizing::OptimizingOptions;
    use crate::index::segments::SegmentsOptions;
    use crate::index::IndexOptions;
    use crate::index::VectorOptions;
    use crate::instance::MockInstance;
    use crate::prelude::Distance;
    use crate::prelude::Kind;
    use crate::utils::file_atomic::FileAtomic;
    use crate::worker::WorkerStartup;
    use crate::worker::WorkerView;
    use arc_swap::ArcSwap;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::sync::Arc;
    use tempfile::tempdir;

    const DEFAULT_HANDLE: Handle = Handle { newtype: 1 };

    #[test]
    fn init_test() {
        let path = tempdir().unwrap().into_path().join("init");
        Worker::create(path.clone());
        let dirs: HashSet<String> = std::fs::read_dir(&path)
            .unwrap()
            .map(|a| a.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(dirs.contains("indexes"), true);
        assert_eq!(dirs.contains("startup"), true);
        assert_eq!(dirs.contains("indexes"), true);
        assert_eq!(dirs.contains("metadata"), true);
        assert_eq!(Worker::check(path.clone()), true);
        Worker::open(path);
    }

    #[test]
    fn insert_test() {
        let instance_ctx = MockInstance::create_context();
        instance_ctx.expect().returning(|_, _| {
            let mut mock = MockInstance::new();
            mock.expect_clone().returning(|| MockInstance::default());
            Ok(mock)
        });

        let protect_ctx = MockWorkerProtect::create_context();
        protect_ctx.expect().returning(|_, _| {
            let mut protect = MockWorkerProtect::new();
            protect.expect_maintain().return_const(());
            protect
        });

        let opts: IndexOptions = IndexOptions {
            vector: VectorOptions {
                dims: 1,
                d: Distance::Dot,
                k: Kind::F32,
            },
            segment: SegmentsOptions::default(),
            optimizing: OptimizingOptions::default(),
            indexing: IndexingOptions::default(),
        };

        let path = tempdir().unwrap().into_path().join("insert");
        let worker = Worker::create(path.clone());

        let success = worker.index_create(DEFAULT_HANDLE, opts).is_ok();
        assert!(success);
        let view = worker.view();
        let ret = view.get(DEFAULT_HANDLE);
        assert!(ret.is_some());

        worker.index_destroy(DEFAULT_HANDLE);
        let view = worker.view();
        let ret = view.get(DEFAULT_HANDLE);
        assert!(ret.is_none());
    }

    #[test]
    fn maintain_test() {
        let mut mock = MockInstance::new();
        mock.expect_clone().returning(|| MockInstance::default());

        let path = tempdir().unwrap().into_path().join("maintain");
        let startup = FileAtomic::create(path, WorkerStartup::new());
        let mut protect = WorkerProtect::create(startup, HashMap::new());
        let protect_item = protect.startup.get().indexes.get(&DEFAULT_HANDLE);
        assert!(protect_item.is_none());

        let view = Arc::new(WorkerView {
            indexes: HashMap::new(),
        });

        protect.indexes.insert(DEFAULT_HANDLE, mock);
        let swap = ArcSwap::new(view);
        protect.maintain(&swap);

        let inner = swap.load_full();
        let view_item = inner.get(DEFAULT_HANDLE);
        assert!(view_item.is_some());
        let protect_item = protect.startup.get().indexes.get(&DEFAULT_HANDLE);
        assert!(protect_item.is_some());
    }
}
