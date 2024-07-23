use base::distance::*;
use base::index::*;
use base::operator::*;
use base::scalar::F32;
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
}

impl Instance {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        alterable_options: IndexAlterableOptions,
    ) -> Result<Self, CreateError> {
        match (options.vector.d, options.vector.v) {
            (DistanceKind::Cos, VectorKind::Vecf32) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::Vecf32Cos(index))
            }
            (DistanceKind::Dot, VectorKind::Vecf32) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::Vecf32Dot(index))
            }
            (DistanceKind::L2, VectorKind::Vecf32) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::Vecf32L2(index))
            }
            (DistanceKind::Cos, VectorKind::Vecf16) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::Vecf16Cos(index))
            }
            (DistanceKind::Dot, VectorKind::Vecf16) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::Vecf16Dot(index))
            }
            (DistanceKind::L2, VectorKind::Vecf16) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::Vecf16L2(index))
            }
            (DistanceKind::Cos, VectorKind::SVecf32) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::SVecf32Cos(index))
            }
            (DistanceKind::Dot, VectorKind::SVecf32) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::SVecf32Dot(index))
            }
            (DistanceKind::L2, VectorKind::SVecf32) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::SVecf32L2(index))
            }
            (DistanceKind::Cos, VectorKind::BVecf32) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::BVecf32Cos(index))
            }
            (DistanceKind::Dot, VectorKind::BVecf32) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::BVecf32Dot(index))
            }
            (DistanceKind::L2, VectorKind::BVecf32) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::BVecf32L2(index))
            }
            (DistanceKind::Jaccard, VectorKind::BVecf32) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::BVecf32Jaccard(index))
            }
            (DistanceKind::Jaccard, _) => Err(CreateError::InvalidIndexOptions {
                reason: "Jaccard distance is only supported for BVecf32 vectors".to_string(),
            }),
        }
    }
    pub fn open(path: PathBuf) -> Self {
        let options =
            serde_json::from_slice::<IndexOptions>(&std::fs::read(path.join("options")).unwrap())
                .unwrap();
        match (options.vector.d, options.vector.v) {
            (DistanceKind::Cos, VectorKind::Vecf32) => Self::Vecf32Cos(Index::open(path)),
            (DistanceKind::Dot, VectorKind::Vecf32) => Self::Vecf32Dot(Index::open(path)),
            (DistanceKind::L2, VectorKind::Vecf32) => Self::Vecf32L2(Index::open(path)),
            (DistanceKind::Cos, VectorKind::Vecf16) => Self::Vecf16Cos(Index::open(path)),
            (DistanceKind::Dot, VectorKind::Vecf16) => Self::Vecf16Dot(Index::open(path)),
            (DistanceKind::L2, VectorKind::Vecf16) => Self::Vecf16L2(Index::open(path)),
            (DistanceKind::Cos, VectorKind::SVecf32) => Self::SVecf32Cos(Index::open(path)),
            (DistanceKind::Dot, VectorKind::SVecf32) => Self::SVecf32Dot(Index::open(path)),
            (DistanceKind::L2, VectorKind::SVecf32) => Self::SVecf32L2(Index::open(path)),
            (DistanceKind::Cos, VectorKind::BVecf32) => Self::BVecf32Cos(Index::open(path)),
            (DistanceKind::Dot, VectorKind::BVecf32) => Self::BVecf32Dot(Index::open(path)),
            (DistanceKind::L2, VectorKind::BVecf32) => Self::BVecf32L2(Index::open(path)),
            (DistanceKind::Jaccard, VectorKind::BVecf32) => Self::BVecf32Jaccard(Index::open(path)),
            _ => unreachable!(),
        }
    }
    pub fn refresh(&self) {
        match self {
            Instance::Vecf32Cos(x) => x.refresh(),
            Instance::Vecf32Dot(x) => x.refresh(),
            Instance::Vecf32L2(x) => x.refresh(),
            Instance::Vecf16Cos(x) => x.refresh(),
            Instance::Vecf16Dot(x) => x.refresh(),
            Instance::Vecf16L2(x) => x.refresh(),
            Instance::SVecf32Cos(x) => x.refresh(),
            Instance::SVecf32Dot(x) => x.refresh(),
            Instance::SVecf32L2(x) => x.refresh(),
            Instance::BVecf32Cos(x) => x.refresh(),
            Instance::BVecf32Dot(x) => x.refresh(),
            Instance::BVecf32L2(x) => x.refresh(),
            Instance::BVecf32Jaccard(x) => x.refresh(),
        }
    }
    pub fn view(&self) -> InstanceView {
        match self {
            Instance::Vecf32Cos(x) => InstanceView::Vecf32Cos(x.view()),
            Instance::Vecf32Dot(x) => InstanceView::Vecf32Dot(x.view()),
            Instance::Vecf32L2(x) => InstanceView::Vecf32L2(x.view()),
            Instance::Vecf16Cos(x) => InstanceView::Vecf16Cos(x.view()),
            Instance::Vecf16Dot(x) => InstanceView::Vecf16Dot(x.view()),
            Instance::Vecf16L2(x) => InstanceView::Vecf16L2(x.view()),
            Instance::SVecf32Cos(x) => InstanceView::SVecf32Cos(x.view()),
            Instance::SVecf32Dot(x) => InstanceView::SVecf32Dot(x.view()),
            Instance::SVecf32L2(x) => InstanceView::SVecf32L2(x.view()),
            Instance::BVecf32Cos(x) => InstanceView::BVecf32Cos(x.view()),
            Instance::BVecf32Dot(x) => InstanceView::BVecf32Dot(x.view()),
            Instance::BVecf32L2(x) => InstanceView::BVecf32L2(x.view()),
            Instance::BVecf32Jaccard(x) => InstanceView::BVecf32Jaccard(x.view()),
        }
    }
    pub fn stat(&self) -> IndexStat {
        match self {
            Instance::Vecf32Cos(x) => x.stat(),
            Instance::Vecf32Dot(x) => x.stat(),
            Instance::Vecf32L2(x) => x.stat(),
            Instance::Vecf16Cos(x) => x.stat(),
            Instance::Vecf16Dot(x) => x.stat(),
            Instance::Vecf16L2(x) => x.stat(),
            Instance::SVecf32Cos(x) => x.stat(),
            Instance::SVecf32Dot(x) => x.stat(),
            Instance::SVecf32L2(x) => x.stat(),
            Instance::BVecf32Cos(x) => x.stat(),
            Instance::BVecf32Dot(x) => x.stat(),
            Instance::BVecf32L2(x) => x.stat(),
            Instance::BVecf32Jaccard(x) => x.stat(),
        }
    }
    pub fn alter(&self, key: &str, value: &str) -> Result<(), AlterError> {
        match self {
            Instance::Vecf32Cos(x) => x.alter(key, value),
            Instance::Vecf32Dot(x) => x.alter(key, value),
            Instance::Vecf32L2(x) => x.alter(key, value),
            Instance::Vecf16Cos(x) => x.alter(key, value),
            Instance::Vecf16Dot(x) => x.alter(key, value),
            Instance::Vecf16L2(x) => x.alter(key, value),
            Instance::SVecf32Cos(x) => x.alter(key, value),
            Instance::SVecf32Dot(x) => x.alter(key, value),
            Instance::SVecf32L2(x) => x.alter(key, value),
            Instance::BVecf32Cos(x) => x.alter(key, value),
            Instance::BVecf32Dot(x) => x.alter(key, value),
            Instance::BVecf32L2(x) => x.alter(key, value),
            Instance::BVecf32Jaccard(x) => x.alter(key, value),
        }
    }
    pub fn delete(&self, pointer: Pointer) -> Result<(), DeleteError> {
        match self {
            Instance::Vecf32Cos(x) => x.delete(pointer),
            Instance::Vecf32Dot(x) => x.delete(pointer),
            Instance::Vecf32L2(x) => x.delete(pointer),
            Instance::Vecf16Cos(x) => x.delete(pointer),
            Instance::Vecf16Dot(x) => x.delete(pointer),
            Instance::Vecf16L2(x) => x.delete(pointer),
            Instance::SVecf32Cos(x) => x.delete(pointer),
            Instance::SVecf32Dot(x) => x.delete(pointer),
            Instance::SVecf32L2(x) => x.delete(pointer),
            Instance::BVecf32Cos(x) => x.delete(pointer),
            Instance::BVecf32Dot(x) => x.delete(pointer),
            Instance::BVecf32L2(x) => x.delete(pointer),
            Instance::BVecf32Jaccard(x) => x.delete(pointer),
        }
    }
    pub fn start(&self) {
        match self {
            Instance::Vecf32Cos(x) => x.start(),
            Instance::Vecf32Dot(x) => x.start(),
            Instance::Vecf32L2(x) => x.start(),
            Instance::Vecf16Cos(x) => x.start(),
            Instance::Vecf16Dot(x) => x.start(),
            Instance::Vecf16L2(x) => x.start(),
            Instance::SVecf32Cos(x) => x.start(),
            Instance::SVecf32Dot(x) => x.start(),
            Instance::SVecf32L2(x) => x.start(),
            Instance::BVecf32Cos(x) => x.start(),
            Instance::BVecf32Dot(x) => x.start(),
            Instance::BVecf32L2(x) => x.start(),
            Instance::BVecf32Jaccard(x) => x.start(),
        }
    }
    pub fn stop(&self) {
        match self {
            Instance::Vecf32Cos(x) => x.stop(),
            Instance::Vecf32Dot(x) => x.stop(),
            Instance::Vecf32L2(x) => x.stop(),
            Instance::Vecf16Cos(x) => x.stop(),
            Instance::Vecf16Dot(x) => x.stop(),
            Instance::Vecf16L2(x) => x.stop(),
            Instance::SVecf32Cos(x) => x.stop(),
            Instance::SVecf32Dot(x) => x.stop(),
            Instance::SVecf32L2(x) => x.stop(),
            Instance::BVecf32Cos(x) => x.stop(),
            Instance::BVecf32Dot(x) => x.stop(),
            Instance::BVecf32L2(x) => x.stop(),
            Instance::BVecf32Jaccard(x) => x.stop(),
        }
    }
    pub fn wait(&self) -> Arc<IndexTracker> {
        match self {
            Instance::Vecf32Cos(x) => x.wait(),
            Instance::Vecf32Dot(x) => x.wait(),
            Instance::Vecf32L2(x) => x.wait(),
            Instance::Vecf16Cos(x) => x.wait(),
            Instance::Vecf16Dot(x) => x.wait(),
            Instance::Vecf16L2(x) => x.wait(),
            Instance::SVecf32Cos(x) => x.wait(),
            Instance::SVecf32Dot(x) => x.wait(),
            Instance::SVecf32L2(x) => x.wait(),
            Instance::BVecf32Cos(x) => x.wait(),
            Instance::BVecf32Dot(x) => x.wait(),
            Instance::BVecf32L2(x) => x.wait(),
            Instance::BVecf32Jaccard(x) => x.wait(),
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
}

impl ViewVbaseOperations for InstanceView {
    fn vbase<'a>(
        &'a self,
        vector: &'a OwnedVector,
        opts: &'a SearchOptions,
    ) -> Result<Box<dyn Iterator<Item = (F32, Pointer)> + 'a>, VbaseError> {
        match (self, vector) {
            (InstanceView::Vecf32Cos(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?)
                    as Box<dyn Iterator<Item = (F32, Pointer)>>)
            }
            (InstanceView::Vecf32Dot(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::Vecf32L2(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::Vecf16Cos(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::Vecf16Dot(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::Vecf16L2(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::SVecf32Cos(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::SVecf32Dot(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::SVecf32L2(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::BVecf32Cos(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::BVecf32Dot(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::BVecf32L2(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::BVecf32Jaccard(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            _ => Err(VbaseError::InvalidVector),
        }
    }
}

impl ViewListOperations for InstanceView {
    fn list(&self) -> Result<Box<dyn Iterator<Item = Pointer> + '_>, ListError> {
        match self {
            InstanceView::Vecf32Cos(x) => {
                Ok(Box::new(x.list()?) as Box<dyn Iterator<Item = Pointer>>)
            }
            InstanceView::Vecf32Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::Vecf32L2(x) => Ok(Box::new(x.list()?)),
            InstanceView::Vecf16Cos(x) => Ok(Box::new(x.list()?)),
            InstanceView::Vecf16Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::Vecf16L2(x) => Ok(Box::new(x.list()?)),
            InstanceView::SVecf32Cos(x) => Ok(Box::new(x.list()?)),
            InstanceView::SVecf32Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::SVecf32L2(x) => Ok(Box::new(x.list()?)),
            InstanceView::BVecf32Cos(x) => Ok(Box::new(x.list()?)),
            InstanceView::BVecf32Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::BVecf32L2(x) => Ok(Box::new(x.list()?)),
            InstanceView::BVecf32Jaccard(x) => Ok(Box::new(x.list()?)),
        }
    }
}

impl InstanceView {
    pub fn insert(
        &self,
        vector: OwnedVector,
        pointer: Pointer,
    ) -> Result<Result<(), OutdatedError>, InsertError> {
        match (self, vector) {
            (InstanceView::Vecf32Cos(x), OwnedVector::Vecf32(vector)) => x.insert(vector, pointer),
            (InstanceView::Vecf32Dot(x), OwnedVector::Vecf32(vector)) => x.insert(vector, pointer),
            (InstanceView::Vecf32L2(x), OwnedVector::Vecf32(vector)) => x.insert(vector, pointer),
            (InstanceView::Vecf16Cos(x), OwnedVector::Vecf16(vector)) => x.insert(vector, pointer),
            (InstanceView::Vecf16Dot(x), OwnedVector::Vecf16(vector)) => x.insert(vector, pointer),
            (InstanceView::Vecf16L2(x), OwnedVector::Vecf16(vector)) => x.insert(vector, pointer),
            (InstanceView::SVecf32Cos(x), OwnedVector::SVecf32(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::SVecf32Dot(x), OwnedVector::SVecf32(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::SVecf32L2(x), OwnedVector::SVecf32(vector)) => x.insert(vector, pointer),
            (InstanceView::BVecf32Cos(x), OwnedVector::BVecf32(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::BVecf32Dot(x), OwnedVector::BVecf32(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::BVecf32L2(x), OwnedVector::BVecf32(vector)) => x.insert(vector, pointer),
            (InstanceView::BVecf32Jaccard(x), OwnedVector::BVecf32(vector)) => {
                x.insert(vector, pointer)
            }
            _ => Err(InsertError::InvalidVector),
        }
    }
    pub fn flush(&self) -> Result<(), FlushError> {
        match self {
            InstanceView::Vecf32Cos(x) => x.flush(),
            InstanceView::Vecf32Dot(x) => x.flush(),
            InstanceView::Vecf32L2(x) => x.flush(),
            InstanceView::Vecf16Cos(x) => x.flush(),
            InstanceView::Vecf16Dot(x) => x.flush(),
            InstanceView::Vecf16L2(x) => x.flush(),
            InstanceView::SVecf32Cos(x) => x.flush(),
            InstanceView::SVecf32Dot(x) => x.flush(),
            InstanceView::SVecf32L2(x) => x.flush(),
            InstanceView::BVecf32Cos(x) => x.flush(),
            InstanceView::BVecf32Dot(x) => x.flush(),
            InstanceView::BVecf32L2(x) => x.flush(),
            InstanceView::BVecf32Jaccard(x) => x.flush(),
        }
    }
}
