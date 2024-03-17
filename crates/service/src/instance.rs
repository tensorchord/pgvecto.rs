use base::distance::*;
use base::index::*;
use base::operator::*;
use base::search::*;
use base::vector::*;
use base::worker::*;
use index::Index;
use index::IndexTracker;
use index::IndexView;
use index::OutdatedError;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub enum Instance {
    Vecf32Cos(Arc<Index<Vecf32Cos>>),
    Vecf32Dot(Arc<Index<Vecf32Dot>>),
    Vecf32L2(Arc<Index<Vecf32L2>>),
    Vecf16Cos(Arc<Index<Vecf16Cos>>),
    Vecf16Dot(Arc<Index<Vecf16Dot>>),
    Vecf16L2(Arc<Index<Vecf16L2>>),
    SVecf32Cos(Arc<Index<SVecf32Cos>>),
    SVecf32Dot(Arc<Index<SVecf32Dot>>),
    SVecf32L2(Arc<Index<SVecf32L2>>),
    BVecf32Cos(Arc<Index<BVecf32Cos>>),
    BVecf32Dot(Arc<Index<BVecf32Dot>>),
    BVecf32L2(Arc<Index<BVecf32L2>>),
    BVecf32Jaccard(Arc<Index<BVecf32Jaccard>>),
    Veci8L2(Arc<Index<Veci8L2>>),
    Veci8Cos(Arc<Index<Veci8Cos>>),
    Veci8Dot(Arc<Index<Veci8Dot>>),
}

impl Instance {
    pub fn create(path: PathBuf, options: IndexOptions) -> Result<Self, CreateError> {
        macro_rules! match_create {
            ($(($distance:ident, $vector:ident, $instance:ident)),* ,) => {
                match (options.vector.d, options.vector.v) {
                    $((DistanceKind::$distance, VectorKind::$vector) => Ok(Self::$instance(Index::create(path, options)?)),)*
                    (DistanceKind::Jaccard, _) => Err(CreateError::InvalidIndexOptions {
                        reason: "Jaccard distance is only supported for BVecf32 vectors".to_string(),
                    }),
                }
            };
        }
        match_create! {
            (Cos, Vecf32, Vecf32Cos),
            (Dot, Vecf32, Vecf32Dot),
            (L2, Vecf32, Vecf32L2),
            (Cos, Vecf16, Vecf16Cos),
            (Dot, Vecf16, Vecf16Dot),
            (L2, Vecf16, Vecf16L2),
            (Cos, SVecf32, SVecf32Cos),
            (Dot, SVecf32, SVecf32Dot),
            (L2, SVecf32, SVecf32L2),
            (Cos, BVecf32, BVecf32Cos),
            (Dot, BVecf32, BVecf32Dot),
            (L2, BVecf32, BVecf32L2),
            (Jaccard, BVecf32, BVecf32Jaccard),
            (Cos, Veci8, Veci8Cos),
            (Dot, Veci8, Veci8Dot),
            (L2, Veci8, Veci8L2),
        }
    }
    pub fn open(path: PathBuf) -> Self {
        let options =
            serde_json::from_slice::<IndexOptions>(&std::fs::read(path.join("options")).unwrap())
                .unwrap();
        macro_rules! match_open {
            ($(($distance:ident, $vector:ident, $instance:ident)),* ,) => {
                match (options.vector.d, options.vector.v) {
                    $((DistanceKind::$distance, VectorKind::$vector) => Self::$instance(Index::open(path)),)*
                    _ => unreachable!(),
                }
            };
        }
        match_open! {
            (Cos, Vecf32, Vecf32Cos),
            (Dot, Vecf32, Vecf32Dot),
            (L2, Vecf32, Vecf32L2),
            (Cos, Vecf16, Vecf16Cos),
            (Dot, Vecf16, Vecf16Dot),
            (L2, Vecf16, Vecf16L2),
            (Cos, SVecf32, SVecf32Cos),
            (Dot, SVecf32, SVecf32Dot),
            (L2, SVecf32, SVecf32L2),
            (Cos, BVecf32, BVecf32Cos),
            (Dot, BVecf32, BVecf32Dot),
            (L2, BVecf32, BVecf32L2),
            (Jaccard, BVecf32, BVecf32Jaccard),
            (Cos, Veci8, Veci8Cos),
            (Dot, Veci8, Veci8Dot),
            (L2, Veci8, Veci8L2),
        }
    }
    pub fn refresh(&self) {
        macro_rules! match_refresh {
            ($($instance:ident),* ,) => {
                match self {
                    $(
                        Instance::$instance(x) => x.refresh(),
                    )*
                }
            };
        }
        match_refresh! {
            Vecf32Cos,
            Vecf32Dot,
            Vecf32L2,
            Vecf16Cos,
            Vecf16Dot,
            Vecf16L2,
            SVecf32Cos,
            SVecf32Dot,
            SVecf32L2,
            BVecf32Cos,
            BVecf32Dot,
            BVecf32L2,
            BVecf32Jaccard,
            Veci8Cos,
            Veci8Dot,
            Veci8L2,
        }
    }
    pub fn view(&self) -> InstanceView {
        macro_rules! match_view {
            ($($instance:ident),* ,) => {
                match self {
                    $(
                        Instance::$instance(x) => InstanceView::$instance(x.view()),
                    )*
                }
            };
        }
        match_view! {
            Vecf32Cos,
            Vecf32Dot,
            Vecf32L2,
            Vecf16Cos,
            Vecf16Dot,
            Vecf16L2,
            SVecf32Cos,
            SVecf32Dot,
            SVecf32L2,
            BVecf32Cos,
            BVecf32Dot,
            BVecf32L2,
            BVecf32Jaccard,
            Veci8Cos,
            Veci8Dot,
            Veci8L2,
        }
    }
    pub fn stat(&self) -> IndexStat {
        macro_rules! match_stat {
            ($($instance:ident),* ,) => {
                match self {
                    $(
                        Instance::$instance(x) => x.stat(),
                    )*
                }
            };
        }
        match_stat! {
            Vecf32Cos,
            Vecf32Dot,
            Vecf32L2,
            Vecf16Cos,
            Vecf16Dot,
            Vecf16L2,
            SVecf32Cos,
            SVecf32Dot,
            SVecf32L2,
            BVecf32Cos,
            BVecf32Dot,
            BVecf32L2,
            BVecf32Jaccard,
            Veci8Cos,
            Veci8Dot,
            Veci8L2,
        }
    }
    pub fn start(&self) {
        macro_rules! match_start {
            ($($instance:ident),* ,) => {
                match self {
                    $(
                        Instance::$instance(x) => x.start(),
                    )*
                }
            };
        }
        match_start! {
            Vecf32Cos,
            Vecf32Dot,
            Vecf32L2,
            Vecf16Cos,
            Vecf16Dot,
            Vecf16L2,
            SVecf32Cos,
            SVecf32Dot,
            SVecf32L2,
            BVecf32Cos,
            BVecf32Dot,
            BVecf32L2,
            BVecf32Jaccard,
            Veci8Cos,
            Veci8Dot,
            Veci8L2,
        }
    }
    pub fn stop(&self) {
        macro_rules! match_stop {
            ($($instance:ident),* ,) => {
                match self {
                    $(
                        Instance::$instance(x) => x.stop(),
                    )*
                }
            };
        }
        match_stop! {
            Vecf32Cos,
            Vecf32Dot,
            Vecf32L2,
            Vecf16Cos,
            Vecf16Dot,
            Vecf16L2,
            SVecf32Cos,
            SVecf32Dot,
            SVecf32L2,
            BVecf32Cos,
            BVecf32Dot,
            BVecf32L2,
            BVecf32Jaccard,
            Veci8Cos,
            Veci8Dot,
            Veci8L2,
        }
    }
    pub fn wait(&self) -> Arc<IndexTracker> {
        macro_rules! match_wait {
            ($($instance:ident),* ,) => {
                match self {
                    $(
                        Instance::$instance(x) => x.wait(),
                    )*
                }
            };
        }
        match_wait! {
            Vecf32Cos,
            Vecf32Dot,
            Vecf32L2,
            Vecf16Cos,
            Vecf16Dot,
            Vecf16L2,
            SVecf32Cos,
            SVecf32Dot,
            SVecf32L2,
            BVecf32Cos,
            BVecf32Dot,
            BVecf32L2,
            BVecf32Jaccard,
            Veci8Cos,
            Veci8Dot,
            Veci8L2,
        }
    }
}

pub enum InstanceView {
    Vecf32Cos(Arc<IndexView<Vecf32Cos>>),
    Vecf32Dot(Arc<IndexView<Vecf32Dot>>),
    Vecf32L2(Arc<IndexView<Vecf32L2>>),
    Vecf16Cos(Arc<IndexView<Vecf16Cos>>),
    Vecf16Dot(Arc<IndexView<Vecf16Dot>>),
    Vecf16L2(Arc<IndexView<Vecf16L2>>),
    SVecf32Cos(Arc<IndexView<SVecf32Cos>>),
    SVecf32Dot(Arc<IndexView<SVecf32Dot>>),
    SVecf32L2(Arc<IndexView<SVecf32L2>>),
    BVecf32Cos(Arc<IndexView<BVecf32Cos>>),
    BVecf32Dot(Arc<IndexView<BVecf32Dot>>),
    BVecf32L2(Arc<IndexView<BVecf32L2>>),
    BVecf32Jaccard(Arc<IndexView<BVecf32Jaccard>>),
    Veci8Cos(Arc<IndexView<Veci8Cos>>),
    Veci8Dot(Arc<IndexView<Veci8Dot>>),
    Veci8L2(Arc<IndexView<Veci8L2>>),
}

impl ViewBasicOperations for InstanceView {
    fn basic<'a, F: Fn(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: &'a OwnedVector,
        opts: &'a SearchOptions,
        filter: F,
    ) -> Result<Box<dyn Iterator<Item = Pointer> + 'a>, BasicError> {
        macro_rules! match_basic {
            ($(($instance:ident, $vector:ident)),* ,) => {
                match (self, vector) {
                    $(
                        (InstanceView::$instance(x), OwnedVector::$vector(vector)) => Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?)),
                    )*
                    _ => Err(BasicError::InvalidVector),
                }
            };
        }
        match_basic! {
            (Vecf32Cos, Vecf32),
            (Vecf32Dot, Vecf32),
            (Vecf32L2, Vecf32),
            (Vecf16Cos, Vecf16),
            (Vecf16Dot, Vecf16),
            (Vecf16L2, Vecf16),
            (SVecf32Cos, SVecf32),
            (SVecf32Dot, SVecf32),
            (SVecf32L2, SVecf32),
            (BVecf32Cos, BVecf32),
            (BVecf32Dot, BVecf32),
            (BVecf32L2, BVecf32),
            (BVecf32Jaccard, BVecf32),
            (Veci8Cos, Veci8),
            (Veci8Dot, Veci8),
            (Veci8L2, Veci8),
        }
    }
}

impl ViewVbaseOperations for InstanceView {
    fn vbase<'a, F: FnMut(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: &'a OwnedVector,
        opts: &'a SearchOptions,
        filter: F,
    ) -> Result<Box<dyn Iterator<Item = Pointer> + 'a>, VbaseError> {
        macro_rules! match_vbase {
            ($(($instance:ident, $vector:ident)),* ,) => {
                match (self, vector) {
                    $(
                        (InstanceView::$instance(x), OwnedVector::$vector(vector)) => Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?)),
                    )*
                    _ => Err(VbaseError::InvalidVector),
                }
            };
        }
        match_vbase! {
            (Vecf32Cos, Vecf32),
            (Vecf32Dot, Vecf32),
            (Vecf32L2, Vecf32),
            (Vecf16Cos, Vecf16),
            (Vecf16Dot, Vecf16),
            (Vecf16L2, Vecf16),
            (SVecf32Cos, SVecf32),
            (SVecf32Dot, SVecf32),
            (SVecf32L2, SVecf32),
            (BVecf32Cos, BVecf32),
            (BVecf32Dot, BVecf32),
            (BVecf32L2, BVecf32),
            (BVecf32Jaccard, BVecf32),
            (Veci8Cos, Veci8),
            (Veci8Dot, Veci8),
            (Veci8L2, Veci8),
        }
    }
}

impl ViewListOperations for InstanceView {
    fn list(&self) -> Result<Box<dyn Iterator<Item = Pointer> + '_>, ListError> {
        macro_rules! match_list {
            ($($instance:ident),* ,) => {
                match self {
                    $(
                        InstanceView::$instance(x) => Ok(Box::new(x.list()?)),
                    )*
                }
            };
        }
        match_list! {
            Vecf32Cos,
            Vecf32Dot,
            Vecf32L2,
            Vecf16Cos,
            Vecf16Dot,
            Vecf16L2,
            SVecf32Cos,
            SVecf32Dot,
            SVecf32L2,
            BVecf32Cos,
            BVecf32Dot,
            BVecf32L2,
            BVecf32Jaccard,
            Veci8Cos,
            Veci8Dot,
            Veci8L2,
        }
    }
}

impl InstanceView {
    pub fn insert(
        &self,
        vector: OwnedVector,
        pointer: Pointer,
        multicolumn_data: i64,
    ) -> Result<Result<(), OutdatedError>, InsertError> {
        macro_rules! match_insert {
            ($(($instance:ident, $vector:ident)),* ,) => {
                match (self, vector) {
                    $(
                        (InstanceView::$instance(x), OwnedVector::$vector(vector)) => x.insert(vector, pointer, multicolumn_data),
                    )*
                    _ => Err(InsertError::InvalidVector),
                }
            };
        }
        match_insert! {
            (Vecf32Cos, Vecf32),
            (Vecf32Dot, Vecf32),
            (Vecf32L2, Vecf32),
            (Vecf16Cos, Vecf16),
            (Vecf16Dot, Vecf16),
            (Vecf16L2, Vecf16),
            (SVecf32Cos, SVecf32),
            (SVecf32Dot, SVecf32),
            (SVecf32L2, SVecf32),
            (BVecf32Cos, BVecf32),
            (BVecf32Dot, BVecf32),
            (BVecf32L2, BVecf32),
            (BVecf32Jaccard, BVecf32),
            (Veci8Cos, Veci8),
            (Veci8Dot, Veci8),
            (Veci8L2, Veci8),
        }
    }
    pub fn delete(&self, pointer: Pointer) -> Result<(), DeleteError> {
        macro_rules! match_delete {
            ($($instance:ident),* ,) => {
                match self {
                    $(
                        InstanceView::$instance(x) => x.delete(pointer),
                    )*
                }
            };
        }
        match_delete! {
            Vecf32Cos,
            Vecf32Dot,
            Vecf32L2,
            Vecf16Cos,
            Vecf16Dot,
            Vecf16L2,
            SVecf32Cos,
            SVecf32Dot,
            SVecf32L2,
            BVecf32Cos,
            BVecf32Dot,
            BVecf32L2,
            BVecf32Jaccard,
            Veci8Cos,
            Veci8Dot,
            Veci8L2,
        }
    }
    pub fn flush(&self) -> Result<(), FlushError> {
        macro_rules! match_flush {
            ($($instance:ident),* ,) => {
                match self {
                    $(
                        InstanceView::$instance(x) => x.flush(),
                    )*
                }
            };
        }
        match_flush! {
            Vecf32Cos,
            Vecf32Dot,
            Vecf32L2,
            Vecf16Cos,
            Vecf16Dot,
            Vecf16L2,
            SVecf32Cos,
            SVecf32Dot,
            SVecf32L2,
            BVecf32Cos,
            BVecf32Dot,
            BVecf32L2,
            BVecf32Jaccard,
            Veci8Cos,
            Veci8Dot,
            Veci8L2,
        }
    }
}
