pub mod metadata;

use crate::index::Index;
use crate::index::IndexView;
use crate::index::OutdatedError;
use crate::prelude::*;
use base::worker::*;
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
    Upgrade,
}

impl Instance {
    pub fn create(path: PathBuf, options: IndexOptions) -> Result<Self, CreateError> {
        match (options.vector.d, options.vector.v) {
            (DistanceKind::Cos, VectorKind::Vecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::Vecf32Cos(index))
            }
            (DistanceKind::Dot, VectorKind::Vecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::Vecf32Dot(index))
            }
            (DistanceKind::L2, VectorKind::Vecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::Vecf32L2(index))
            }
            (DistanceKind::Cos, VectorKind::Vecf16) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::Vecf16Cos(index))
            }
            (DistanceKind::Dot, VectorKind::Vecf16) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::Vecf16Dot(index))
            }
            (DistanceKind::L2, VectorKind::Vecf16) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::Vecf16L2(index))
            }
            (DistanceKind::Cos, VectorKind::SVecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::SVecf32Cos(index))
            }
            (DistanceKind::Dot, VectorKind::SVecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::SVecf32Dot(index))
            }
            (DistanceKind::L2, VectorKind::SVecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::SVecf32L2(index))
            }
            (DistanceKind::Cos, VectorKind::BVecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::BVecf32Cos(index))
            }
            (DistanceKind::Dot, VectorKind::BVecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::BVecf32Dot(index))
            }
            (DistanceKind::L2, VectorKind::BVecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::BVecf32L2(index))
            }
            (DistanceKind::Jaccard, VectorKind::BVecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::BVecf32Jaccard(index))
            }
            (DistanceKind::L2, VectorKind::Veci8) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::Veci8L2(index))
            }
            (DistanceKind::Cos, VectorKind::Veci8) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::Veci8Cos(index))
            }
            (DistanceKind::Dot, VectorKind::Veci8) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::Veci8Dot(index))
            }
            (DistanceKind::Jaccard, _) => Err(CreateError::InvalidIndexOptions {
                reason: "Jaccard distance is only supported for BVecf32 vectors".to_string(),
            }),
        }
    }
    pub fn open(path: PathBuf) -> Self {
        if self::metadata::Metadata::read(path.join("metadata")).is_err() {
            return Self::Upgrade;
        }
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
            (DistanceKind::L2, VectorKind::Veci8) => Self::Veci8L2(Index::open(path)),
            (DistanceKind::Cos, VectorKind::Veci8) => Self::Veci8Cos(Index::open(path)),
            (DistanceKind::Dot, VectorKind::Veci8) => Self::Veci8Dot(Index::open(path)),
            (DistanceKind::Jaccard, _) => unreachable!(),
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
            Instance::Veci8L2(x) => x.refresh(),
            Instance::Veci8Cos(x) => x.refresh(),
            Instance::Veci8Dot(x) => x.refresh(),
            Instance::Upgrade => (),
        }
    }
    pub fn view(&self) -> Option<InstanceView> {
        match self {
            Instance::Vecf32Cos(x) => Some(InstanceView::Vecf32Cos(x.view())),
            Instance::Vecf32Dot(x) => Some(InstanceView::Vecf32Dot(x.view())),
            Instance::Vecf32L2(x) => Some(InstanceView::Vecf32L2(x.view())),
            Instance::Vecf16Cos(x) => Some(InstanceView::Vecf16Cos(x.view())),
            Instance::Vecf16Dot(x) => Some(InstanceView::Vecf16Dot(x.view())),
            Instance::Vecf16L2(x) => Some(InstanceView::Vecf16L2(x.view())),
            Instance::SVecf32Cos(x) => Some(InstanceView::SVecf32Cos(x.view())),
            Instance::SVecf32Dot(x) => Some(InstanceView::SVecf32Dot(x.view())),
            Instance::SVecf32L2(x) => Some(InstanceView::SVecf32L2(x.view())),
            Instance::BVecf32Cos(x) => Some(InstanceView::BVecf32Cos(x.view())),
            Instance::BVecf32Dot(x) => Some(InstanceView::BVecf32Dot(x.view())),
            Instance::BVecf32L2(x) => Some(InstanceView::BVecf32L2(x.view())),
            Instance::BVecf32Jaccard(x) => Some(InstanceView::BVecf32Jaccard(x.view())),
            Instance::Veci8L2(x) => Some(InstanceView::Veci8L2(x.view())),
            Instance::Veci8Cos(x) => Some(InstanceView::Veci8Cos(x.view())),
            Instance::Veci8Dot(x) => Some(InstanceView::Veci8Dot(x.view())),
            Instance::Upgrade => None,
        }
    }
    pub fn stat(&self) -> Option<IndexStat> {
        match self {
            Instance::Vecf32Cos(x) => Some(x.stat()),
            Instance::Vecf32Dot(x) => Some(x.stat()),
            Instance::Vecf32L2(x) => Some(x.stat()),
            Instance::Vecf16Cos(x) => Some(x.stat()),
            Instance::Vecf16Dot(x) => Some(x.stat()),
            Instance::Vecf16L2(x) => Some(x.stat()),
            Instance::SVecf32Cos(x) => Some(x.stat()),
            Instance::SVecf32Dot(x) => Some(x.stat()),
            Instance::SVecf32L2(x) => Some(x.stat()),
            Instance::BVecf32Cos(x) => Some(x.stat()),
            Instance::BVecf32Dot(x) => Some(x.stat()),
            Instance::BVecf32L2(x) => Some(x.stat()),
            Instance::BVecf32Jaccard(x) => Some(x.stat()),
            Instance::Veci8L2(x) => Some(x.stat()),
            Instance::Veci8Cos(x) => Some(x.stat()),
            Instance::Veci8Dot(x) => Some(x.stat()),
            Instance::Upgrade => None,
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
        match (self, vector) {
            (InstanceView::Vecf32Cos(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?)
                    as Box<dyn Iterator<Item = Pointer>>)
            }
            (InstanceView::Vecf32Dot(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::Vecf32L2(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::Vecf16Cos(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::Vecf16Dot(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::Vecf16L2(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::SVecf32Cos(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::SVecf32Dot(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::SVecf32L2(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::BVecf32Cos(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::BVecf32Dot(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::BVecf32L2(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::BVecf32Jaccard(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::Veci8Cos(x), OwnedVector::Veci8(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            (InstanceView::Veci8Dot(x), OwnedVector::Veci8(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            (InstanceView::Veci8L2(x), OwnedVector::Veci8(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            _ => Err(BasicError::InvalidVector),
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
        match (self, vector) {
            (InstanceView::Vecf32Cos(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?)
                    as Box<dyn Iterator<Item = Pointer>>)
            }
            (InstanceView::Vecf32Dot(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::Vecf32L2(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::Vecf16Cos(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::Vecf16Dot(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::Vecf16L2(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::SVecf32Cos(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::SVecf32Dot(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::SVecf32L2(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::BVecf32Cos(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::BVecf32Dot(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::BVecf32L2(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::BVecf32Jaccard(x), OwnedVector::BVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::Veci8Cos(x), OwnedVector::Veci8(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
            }
            (InstanceView::Veci8Dot(x), OwnedVector::Veci8(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
            }
            (InstanceView::Veci8L2(x), OwnedVector::Veci8(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
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
            InstanceView::Veci8Cos(x) => Ok(Box::new(x.list()?)),
            InstanceView::Veci8Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::Veci8L2(x) => Ok(Box::new(x.list()?)),
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
            (InstanceView::Veci8Cos(x), OwnedVector::Veci8(vector)) => x.insert(vector, pointer),
            (InstanceView::Veci8Dot(x), OwnedVector::Veci8(vector)) => x.insert(vector, pointer),
            (InstanceView::Veci8L2(x), OwnedVector::Veci8(vector)) => x.insert(vector, pointer),
            _ => Err(InsertError::InvalidVector),
        }
    }
    pub fn delete(&self, pointer: Pointer) -> Result<(), DeleteError> {
        match self {
            InstanceView::Vecf32Cos(x) => x.delete(pointer),
            InstanceView::Vecf32Dot(x) => x.delete(pointer),
            InstanceView::Vecf32L2(x) => x.delete(pointer),
            InstanceView::Vecf16Cos(x) => x.delete(pointer),
            InstanceView::Vecf16Dot(x) => x.delete(pointer),
            InstanceView::Vecf16L2(x) => x.delete(pointer),
            InstanceView::SVecf32Cos(x) => x.delete(pointer),
            InstanceView::SVecf32Dot(x) => x.delete(pointer),
            InstanceView::SVecf32L2(x) => x.delete(pointer),
            InstanceView::BVecf32Cos(x) => x.delete(pointer),
            InstanceView::BVecf32Dot(x) => x.delete(pointer),
            InstanceView::BVecf32L2(x) => x.delete(pointer),
            InstanceView::BVecf32Jaccard(x) => x.delete(pointer),
            InstanceView::Veci8Cos(x) => x.delete(pointer),
            InstanceView::Veci8Dot(x) => x.delete(pointer),
            InstanceView::Veci8L2(x) => x.delete(pointer),
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
            InstanceView::Veci8Cos(x) => x.flush(),
            InstanceView::Veci8Dot(x) => x.flush(),
            InstanceView::Veci8L2(x) => x.flush(),
        }
    }
}
