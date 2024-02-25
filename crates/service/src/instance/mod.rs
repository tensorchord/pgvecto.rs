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
    SVecf32L2(Arc<Index<SVecf32L2>>),
    SVecf32Cos(Arc<Index<SVecf32Cos>>),
    SVecf32Dot(Arc<Index<SVecf32Dot>>),
    I8L2(Arc<Index<I8L2>>),
    I8Cos(Arc<Index<I8Cos>>),
    I8Dot(Arc<Index<I8Dot>>),
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
            (DistanceKind::L2, VectorKind::SVecf32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::SVecf32L2(index))
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
            (Distance::L2, Kind::I8) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::I8L2(index))
            }
            (Distance::Cos, Kind::I8) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::I8Cos(index))
            }
            (Distance::Dot, Kind::I8) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::I8Dot(index))
            }
            (Distance::L2, Kind::I8) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::I8L2(index))
            }
            (Distance::Cos, Kind::I8) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::I8Cos(index))
            }
            (Distance::Dot, Kind::I8) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::I8Dot(index))
            }
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
            (DistanceKind::L2, VectorKind::SVecf32) => Self::SVecf32L2(Index::open(path)),
            (DistanceKind::Cos, VectorKind::SVecf32) => Self::SVecf32Cos(Index::open(path)),
            (DistanceKind::Dot, VectorKind::SVecf32) => Self::SVecf32Dot(Index::open(path)),
            (Distance::L2, Kind::I8) => Self::I8L2(Index::open(path)),
            (Distance::Cos, Kind::I8) => Self::I8Cos(Index::open(path)),
            (Distance::Dot, Kind::I8) => Self::I8Dot(Index::open(path)),
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
            Instance::SVecf32L2(x) => x.refresh(),
            Instance::SVecf32Cos(x) => x.refresh(),
            Instance::SVecf32Dot(x) => x.refresh(),
            Instance::I8L2(x) => x.refresh(),
            Instance::I8Cos(x) => x.refresh(),
            Instance::I8Dot(x) => x.refresh(),
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
            Instance::SVecf32L2(x) => Some(InstanceView::SVecf32L2(x.view())),
            Instance::SVecf32Cos(x) => Some(InstanceView::SVecf32Cos(x.view())),
            Instance::SVecf32Dot(x) => Some(InstanceView::SVecf32Dot(x.view())),
            Instance::I8L2(x) => Some(InstanceView::I8L2(x.view())),
            Instance::I8Cos(x) => Some(InstanceView::I8Cos(x.view())),
            Instance::I8Dot(x) => Some(InstanceView::I8Dot(x.view())),
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
            Instance::SVecf32L2(x) => Some(x.stat()),
            Instance::SVecf32Cos(x) => Some(x.stat()),
            Instance::SVecf32Dot(x) => Some(x.stat()),
            Instance::I8L2(x) => Some(x.stat()),
            Instance::I8Cos(x) => Some(x.stat()),
            Instance::I8Dot(x) => Some(x.stat()),
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
    I8Cos(Arc<IndexView<I8Cos>>),
    I8Dot(Arc<IndexView<I8Dot>>),
    I8L2(Arc<IndexView<I8L2>>),
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
            (InstanceView::SVecf32Cos(x), OwnedVector::SVecF32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::SVecf32Dot(x), OwnedVector::SVecF32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::SVecf32L2(x), OwnedVector::SVecF32(vector)) => {
                Ok(Box::new(x.basic(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::I8Cos(x), DynamicVector::I8(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            (InstanceView::I8Dot(x), DynamicVector::I8(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            (InstanceView::I8L2(x), DynamicVector::I8(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            (InstanceView::I8Cos(x), DynamicVector::I8(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            (InstanceView::I8Dot(x), DynamicVector::I8(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            (InstanceView::I8L2(x), DynamicVector::I8(vector)) => {
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
            (InstanceView::SVecf32Cos(x), OwnedVector::SVecF32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::SVecf32Dot(x), OwnedVector::SVecF32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::SVecf32L2(x), OwnedVector::SVecF32(vector)) => {
                Ok(Box::new(x.vbase(vector.for_borrow(), opts, filter)?))
            }
            (InstanceView::I8Cos(x), DynamicVector::I8(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
            }
            (InstanceView::I8Dot(x), DynamicVector::I8(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
            }
            (InstanceView::I8L2(x), DynamicVector::I8(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
            }
            (InstanceView::I8Cos(x), DynamicVector::I8(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
            }
            (InstanceView::I8Dot(x), DynamicVector::I8(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
            }
            (InstanceView::I8L2(x), DynamicVector::I8(vector)) => {
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
            InstanceView::I8Cos(x) => Ok(Box::new(x.list()?)),
            InstanceView::I8Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::I8L2(x) => Ok(Box::new(x.list()?)),
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
            (InstanceView::SVecf32Cos(x), OwnedVector::SVecF32(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::SVecf32Dot(x), OwnedVector::SVecF32(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::SVecf32L2(x), OwnedVector::SVecF32(vector)) => x.insert(vector, pointer),
            (InstanceView::I8Cos(x), DynamicVector::I8(vector)) => x.insert(vector, pointer),
            (InstanceView::I8Dot(x), DynamicVector::I8(vector)) => x.insert(vector, pointer),
            (InstanceView::I8L2(x), DynamicVector::I8(vector)) => x.insert(vector, pointer),
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
            InstanceView::I8Cos(x) => x.delete(pointer),
            InstanceView::I8Dot(x) => x.delete(pointer),
            InstanceView::I8L2(x) => x.delete(pointer),
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
            InstanceView::I8Cos(x) => x.flush(),
            InstanceView::I8Dot(x) => x.flush(),
            InstanceView::I8L2(x) => x.flush(),
        }
    }
}
