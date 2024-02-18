pub mod metadata;

use crate::index::Index;
use crate::index::IndexOptions;
use crate::index::IndexStat;
use crate::index::IndexView;
use crate::index::OutdatedError;
use crate::index::SearchOptions;
use crate::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub enum Instance {
    F32Cos(Arc<Index<F32Cos>>),
    F32Dot(Arc<Index<F32Dot>>),
    F32L2(Arc<Index<F32L2>>),
    F16Cos(Arc<Index<F16Cos>>),
    F16Dot(Arc<Index<F16Dot>>),
    F16L2(Arc<Index<F16L2>>),
    SparseF32L2(Arc<Index<SparseF32L2>>),
    SparseF32Cos(Arc<Index<SparseF32Cos>>),
    SparseF32Dot(Arc<Index<SparseF32Dot>>),
    Upgrade,
}

impl Instance {
    pub fn create(path: PathBuf, options: IndexOptions) -> Result<Self, CreateError> {
        match (options.vector.d, options.vector.k) {
            (Distance::Cos, Kind::F32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::F32Cos(index))
            }
            (Distance::Dot, Kind::F32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::F32Dot(index))
            }
            (Distance::L2, Kind::F32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::F32L2(index))
            }
            (Distance::Cos, Kind::F16) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::F16Cos(index))
            }
            (Distance::Dot, Kind::F16) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::F16Dot(index))
            }
            (Distance::L2, Kind::F16) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::F16L2(index))
            }
            (Distance::L2, Kind::SparseF32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::SparseF32L2(index))
            }
            (Distance::Cos, Kind::SparseF32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::SparseF32Cos(index))
            }
            (Distance::Dot, Kind::SparseF32) => {
                let index = Index::create(path.clone(), options)?;
                self::metadata::Metadata::write(path.join("metadata"));
                Ok(Self::SparseF32Dot(index))
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
        match (options.vector.d, options.vector.k) {
            (Distance::Cos, Kind::F32) => Self::F32Cos(Index::open(path)),
            (Distance::Dot, Kind::F32) => Self::F32Dot(Index::open(path)),
            (Distance::L2, Kind::F32) => Self::F32L2(Index::open(path)),
            (Distance::Cos, Kind::F16) => Self::F16Cos(Index::open(path)),
            (Distance::Dot, Kind::F16) => Self::F16Dot(Index::open(path)),
            (Distance::L2, Kind::F16) => Self::F16L2(Index::open(path)),
            (Distance::L2, Kind::SparseF32) => Self::SparseF32L2(Index::open(path)),
            (Distance::Cos, Kind::SparseF32) => Self::SparseF32Cos(Index::open(path)),
            (Distance::Dot, Kind::SparseF32) => Self::SparseF32Dot(Index::open(path)),
        }
    }
    pub fn refresh(&self) {
        match self {
            Instance::F32Cos(x) => x.refresh(),
            Instance::F32Dot(x) => x.refresh(),
            Instance::F32L2(x) => x.refresh(),
            Instance::F16Cos(x) => x.refresh(),
            Instance::F16Dot(x) => x.refresh(),
            Instance::F16L2(x) => x.refresh(),
            Instance::SparseF32L2(x) => x.refresh(),
            Instance::SparseF32Cos(x) => x.refresh(),
            Instance::SparseF32Dot(x) => x.refresh(),
            Instance::Upgrade => (),
        }
    }
    pub fn view(&self) -> Option<InstanceView> {
        match self {
            Instance::F32Cos(x) => Some(InstanceView::F32Cos(x.view())),
            Instance::F32Dot(x) => Some(InstanceView::F32Dot(x.view())),
            Instance::F32L2(x) => Some(InstanceView::F32L2(x.view())),
            Instance::F16Cos(x) => Some(InstanceView::F16Cos(x.view())),
            Instance::F16Dot(x) => Some(InstanceView::F16Dot(x.view())),
            Instance::F16L2(x) => Some(InstanceView::F16L2(x.view())),
            Instance::SparseF32L2(x) => Some(InstanceView::SparseF32L2(x.view())),
            Instance::SparseF32Cos(x) => Some(InstanceView::SparseF32Cos(x.view())),
            Instance::SparseF32Dot(x) => Some(InstanceView::SparseF32Dot(x.view())),
            Instance::Upgrade => None,
        }
    }
    pub fn stat(&self) -> Option<IndexStat> {
        match self {
            Instance::F32Cos(x) => Some(x.stat()),
            Instance::F32Dot(x) => Some(x.stat()),
            Instance::F32L2(x) => Some(x.stat()),
            Instance::F16Cos(x) => Some(x.stat()),
            Instance::F16Dot(x) => Some(x.stat()),
            Instance::F16L2(x) => Some(x.stat()),
            Instance::SparseF32L2(x) => Some(x.stat()),
            Instance::SparseF32Cos(x) => Some(x.stat()),
            Instance::SparseF32Dot(x) => Some(x.stat()),
            Instance::Upgrade => None,
        }
    }
}

pub enum InstanceView {
    F32Cos(Arc<IndexView<F32Cos>>),
    F32Dot(Arc<IndexView<F32Dot>>),
    F32L2(Arc<IndexView<F32L2>>),
    F16Cos(Arc<IndexView<F16Cos>>),
    F16Dot(Arc<IndexView<F16Dot>>),
    F16L2(Arc<IndexView<F16L2>>),
    SparseF32Cos(Arc<IndexView<SparseF32Cos>>),
    SparseF32Dot(Arc<IndexView<SparseF32Dot>>),
    SparseF32L2(Arc<IndexView<SparseF32L2>>),
}

impl InstanceView {
    pub fn _basic<'a, F: Fn(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: &'a DynamicVector,
        opts: &'a SearchOptions,
        filter: F,
    ) -> Result<Box<dyn Iterator<Item = Pointer> + 'a>, BasicError> {
        match (self, vector) {
            (InstanceView::F32Cos(x), DynamicVector::F32(vector)) => {
                Ok(Box::new(x.basic(vector, opts, filter)?) as Box<dyn Iterator<Item = Pointer>>)
            }
            (InstanceView::F32Dot(x), DynamicVector::F32(vector)) => {
                Ok(Box::new(x.basic(vector, opts, filter)?))
            }
            (InstanceView::F32L2(x), DynamicVector::F32(vector)) => {
                Ok(Box::new(x.basic(vector, opts, filter)?))
            }
            (InstanceView::F16Cos(x), DynamicVector::F16(vector)) => {
                Ok(Box::new(x.basic(vector, opts, filter)?))
            }
            (InstanceView::F16Dot(x), DynamicVector::F16(vector)) => {
                Ok(Box::new(x.basic(vector, opts, filter)?))
            }
            (InstanceView::F16L2(x), DynamicVector::F16(vector)) => {
                Ok(Box::new(x.basic(vector, opts, filter)?))
            }
            (InstanceView::SparseF32Cos(x), DynamicVector::SparseF32(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            (InstanceView::SparseF32Dot(x), DynamicVector::SparseF32(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            (InstanceView::SparseF32L2(x), DynamicVector::SparseF32(vector)) => {
                Ok(Box::new(x.basic(vector.into(), opts, filter)?))
            }
            _ => Err(BasicError::InvalidVector),
        }
    }
    pub fn _vbase<'a, F: FnMut(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: &'a DynamicVector,
        opts: &'a SearchOptions,
        filter: F,
    ) -> Result<Box<dyn Iterator<Item = Pointer> + 'a>, VbaseError> {
        match (self, vector) {
            (InstanceView::F32Cos(x), DynamicVector::F32(vector)) => {
                Ok(Box::new(x.vbase(vector, opts, filter)?) as Box<dyn Iterator<Item = Pointer>>)
            }
            (InstanceView::F32Dot(x), DynamicVector::F32(vector)) => {
                Ok(Box::new(x.vbase(vector, opts, filter)?))
            }
            (InstanceView::F32L2(x), DynamicVector::F32(vector)) => {
                Ok(Box::new(x.vbase(vector, opts, filter)?))
            }
            (InstanceView::F16Cos(x), DynamicVector::F16(vector)) => {
                Ok(Box::new(x.vbase(vector, opts, filter)?))
            }
            (InstanceView::F16Dot(x), DynamicVector::F16(vector)) => {
                Ok(Box::new(x.vbase(vector, opts, filter)?))
            }
            (InstanceView::F16L2(x), DynamicVector::F16(vector)) => {
                Ok(Box::new(x.vbase(vector, opts, filter)?))
            }
            (InstanceView::SparseF32Cos(x), DynamicVector::SparseF32(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
            }
            (InstanceView::SparseF32Dot(x), DynamicVector::SparseF32(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
            }
            (InstanceView::SparseF32L2(x), DynamicVector::SparseF32(vector)) => {
                Ok(Box::new(x.vbase(vector.into(), opts, filter)?))
            }
            _ => Err(VbaseError::InvalidVector),
        }
    }
    pub fn _list(&self) -> Result<Box<dyn Iterator<Item = Pointer> + '_>, ListError> {
        match self {
            InstanceView::F32Cos(x) => Ok(Box::new(x.list()?) as Box<dyn Iterator<Item = Pointer>>),
            InstanceView::F32Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::F32L2(x) => Ok(Box::new(x.list()?)),
            InstanceView::F16Cos(x) => Ok(Box::new(x.list()?)),
            InstanceView::F16Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::F16L2(x) => Ok(Box::new(x.list()?)),
            InstanceView::SparseF32Cos(x) => Ok(Box::new(x.list()?)),
            InstanceView::SparseF32Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::SparseF32L2(x) => Ok(Box::new(x.list()?)),
        }
    }
    pub fn insert(
        &self,
        vector: DynamicVector,
        pointer: Pointer,
    ) -> Result<Result<(), OutdatedError>, InsertError> {
        match (self, vector) {
            (InstanceView::F32Cos(x), DynamicVector::F32(vector)) => x.insert(vector, pointer),
            (InstanceView::F32Dot(x), DynamicVector::F32(vector)) => x.insert(vector, pointer),
            (InstanceView::F32L2(x), DynamicVector::F32(vector)) => x.insert(vector, pointer),
            (InstanceView::F16Cos(x), DynamicVector::F16(vector)) => x.insert(vector, pointer),
            (InstanceView::F16Dot(x), DynamicVector::F16(vector)) => x.insert(vector, pointer),
            (InstanceView::F16L2(x), DynamicVector::F16(vector)) => x.insert(vector, pointer),
            (InstanceView::SparseF32Cos(x), DynamicVector::SparseF32(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::SparseF32Dot(x), DynamicVector::SparseF32(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::SparseF32L2(x), DynamicVector::SparseF32(vector)) => {
                x.insert(vector, pointer)
            }
            _ => Err(InsertError::InvalidVector),
        }
    }
    pub fn delete(&self, pointer: Pointer) -> Result<(), DeleteError> {
        match self {
            InstanceView::F32Cos(x) => x.delete(pointer),
            InstanceView::F32Dot(x) => x.delete(pointer),
            InstanceView::F32L2(x) => x.delete(pointer),
            InstanceView::F16Cos(x) => x.delete(pointer),
            InstanceView::F16Dot(x) => x.delete(pointer),
            InstanceView::F16L2(x) => x.delete(pointer),
            InstanceView::SparseF32Cos(x) => x.delete(pointer),
            InstanceView::SparseF32Dot(x) => x.delete(pointer),
            InstanceView::SparseF32L2(x) => x.delete(pointer),
        }
    }
    pub fn flush(&self) -> Result<(), FlushError> {
        match self {
            InstanceView::F32Cos(x) => x.flush(),
            InstanceView::F32Dot(x) => x.flush(),
            InstanceView::F32L2(x) => x.flush(),
            InstanceView::F16Cos(x) => x.flush(),
            InstanceView::F16Dot(x) => x.flush(),
            InstanceView::F16L2(x) => x.flush(),
            InstanceView::SparseF32Cos(x) => x.flush(),
            InstanceView::SparseF32Dot(x) => x.flush(),
            InstanceView::SparseF32L2(x) => x.flush(),
        }
    }
}
