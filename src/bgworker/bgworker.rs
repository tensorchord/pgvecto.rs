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

pub struct Bgworker {
    path: PathBuf,
    protect: Mutex<BgworkerProtect>,
    view: ArcSwap<BgworkerView>,
}

impl Bgworker {
    pub fn create(path: PathBuf) -> Arc<Self> {
        std::fs::create_dir(&path).unwrap();
        std::fs::create_dir(path.join("indexes")).unwrap();
        let startup = FileAtomic::create(path.join("startup"), BgworkerStartup::new());
        let indexes = HashMap::new();
        let view = Arc::new(BgworkerView {
            indexes: indexes.clone(),
        });
        let protect = BgworkerProtect { startup, indexes };
        sync_dir(&path);
        Arc::new(Bgworker {
            path,
            protect: Mutex::new(protect),
            view: ArcSwap::new(view),
        })
    }
    pub fn open(path: PathBuf) -> Arc<Self> {
        let startup = FileAtomic::<BgworkerStartup>::open(path.join("startup"));
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
        let view = Arc::new(BgworkerView {
            indexes: indexes.clone(),
        });
        let protect = BgworkerProtect { startup, indexes };
        Arc::new(Bgworker {
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
        mut f: F,
    ) -> Result<Vec<Pointer>, FriendlyError>
    where
        F: FnMut(Pointer) -> bool,
    {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).expect("Not exists.");
        let view = index.view();
        match view.search(search.1, &search.0, |p| f(p)) {
            Ok(x) => Ok(x),
            Err(IndexSearchError::InvalidVector(x)) => Err(FriendlyError::InvalidVector(x)),
        }
    }
    pub fn call_insert(&self, id: Id, insert: (Vec<Scalar>, Pointer)) -> Result<(), FriendlyError> {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).expect("Not exists.");
        loop {
            let view = index.view();
            match view.insert(insert.0.clone(), insert.1) {
                Ok(()) => break Ok(()),
                Err(IndexInsertError::InvalidVector(x)) => {
                    break Err(FriendlyError::InvalidVector(x))
                }
                Err(IndexInsertError::OutdatedView(_)) => index.refresh(),
            }
        }
    }
    pub fn call_delete<F>(&self, id: Id, f: F)
    where
        F: FnMut(Pointer) -> bool,
    {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).expect("Not exists.");
        let view = index.view();
        view.delete(f);
    }
    pub fn call_flush(&self, id: Id) {
        let view = self.view.load_full();
        let index = view.indexes.get(&id).expect("Not exists.");
        let view = index.view();
        view.flush().unwrap();
    }
    pub fn call_destory(&self, id: Id) {
        let mut protect = self.protect.lock();
        protect.indexes.remove(&id);
        protect.maintain(&self.view);
    }
}

struct BgworkerView {
    indexes: HashMap<Id, Arc<Index>>,
}

struct BgworkerProtect {
    startup: FileAtomic<BgworkerStartup>,
    indexes: HashMap<Id, Arc<Index>>,
}

impl BgworkerProtect {
    fn maintain(&mut self, swap: &ArcSwap<BgworkerView>) {
        let indexes = self
            .indexes
            .iter()
            .map(|(&k, v)| (k, v.options().clone()))
            .collect();
        self.startup.set(BgworkerStartup { indexes });
        swap.swap(Arc::new(BgworkerView {
            indexes: self.indexes.clone(),
        }));
    }
}

#[serde_with::serde_as]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BgworkerStartup {
    #[serde_as(as = "HashMap<DisplayFromStr, _>")]
    indexes: HashMap<Id, IndexOptions>,
}

impl BgworkerStartup {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
        }
    }
}
