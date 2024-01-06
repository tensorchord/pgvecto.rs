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
    Upgrade,
}

impl Instance {
    pub fn create(path: PathBuf, options: IndexOptions) -> Self {
        match (options.vector.d, options.vector.k) {
            (Distance::Cos, Kind::F32) => {
                let index = Index::create(path.clone(), options);
                self::metadata::Metadata::write(path.join("metadata"));
                Self::F32Cos(index)
            }
            (Distance::Dot, Kind::F32) => {
                let index = Index::create(path.clone(), options);
                self::metadata::Metadata::write(path.join("metadata"));
                Self::F32Dot(index)
            }
            (Distance::L2, Kind::F32) => {
                let index = Index::create(path.clone(), options);
                self::metadata::Metadata::write(path.join("metadata"));
                Self::F32L2(index)
            }
            (Distance::Cos, Kind::F16) => {
                let index = Index::create(path.clone(), options);
                self::metadata::Metadata::write(path.join("metadata"));
                Self::F16Cos(index)
            }
            (Distance::Dot, Kind::F16) => {
                let index = Index::create(path.clone(), options);
                self::metadata::Metadata::write(path.join("metadata"));
                Self::F16Dot(index)
            }
            (Distance::L2, Kind::F16) => {
                let index = Index::create(path.clone(), options);
                self::metadata::Metadata::write(path.join("metadata"));
                Self::F16L2(index)
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
        }
    }
    pub fn options(&self) -> Result<&IndexOptions, FriendlyError> {
        match self {
            Instance::F32Cos(x) => Ok(x.options()),
            Instance::F32Dot(x) => Ok(x.options()),
            Instance::F32L2(x) => Ok(x.options()),
            Instance::F16Cos(x) => Ok(x.options()),
            Instance::F16Dot(x) => Ok(x.options()),
            Instance::F16L2(x) => Ok(x.options()),
            Instance::Upgrade => Err(FriendlyError::Upgrade2),
        }
    }
    pub fn refresh(&self) -> Result<(), FriendlyError> {
        match self {
            Instance::F32Cos(x) => {
                x.refresh();
                Ok(())
            }
            Instance::F32Dot(x) => {
                x.refresh();
                Ok(())
            }
            Instance::F32L2(x) => {
                x.refresh();
                Ok(())
            }
            Instance::F16Cos(x) => {
                x.refresh();
                Ok(())
            }
            Instance::F16Dot(x) => {
                x.refresh();
                Ok(())
            }
            Instance::F16L2(x) => {
                x.refresh();
                Ok(())
            }
            Instance::Upgrade => Err(FriendlyError::Upgrade2),
        }
    }
    pub fn view(&self) -> Result<InstanceView, FriendlyError> {
        match self {
            Instance::F32Cos(x) => Ok(InstanceView::F32Cos(x.view())),
            Instance::F32Dot(x) => Ok(InstanceView::F32Dot(x.view())),
            Instance::F32L2(x) => Ok(InstanceView::F32L2(x.view())),
            Instance::F16Cos(x) => Ok(InstanceView::F16Cos(x.view())),
            Instance::F16Dot(x) => Ok(InstanceView::F16Dot(x.view())),
            Instance::F16L2(x) => Ok(InstanceView::F16L2(x.view())),
            Instance::Upgrade => Err(FriendlyError::Upgrade2),
        }
    }
    pub fn stat(&self) -> Result<IndexStat, FriendlyError> {
        match self {
            Instance::F32Cos(x) => Ok(x.stat()),
            Instance::F32Dot(x) => Ok(x.stat()),
            Instance::F32L2(x) => Ok(x.stat()),
            Instance::F16Cos(x) => Ok(x.stat()),
            Instance::F16Dot(x) => Ok(x.stat()),
            Instance::F16L2(x) => Ok(x.stat()),
            Instance::Upgrade => Ok(IndexStat::Upgrade),
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
}

impl InstanceView {
    pub fn search<F: FnMut(Pointer) -> bool>(
        &self,
        vector: &DynamicVector,
        opts: &SearchOptions,
        filter: F,
    ) -> Result<Vec<Pointer>, FriendlyError> {
        match (self, vector) {
            (InstanceView::F32Cos(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(vector, opts, filter))
            }
            (InstanceView::F32Dot(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(vector, opts, filter))
            }
            (InstanceView::F32L2(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(vector, opts, filter))
            }
            (InstanceView::F16Cos(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(vector, opts, filter))
            }
            (InstanceView::F16Dot(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(vector, opts, filter))
            }
            (InstanceView::F16L2(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(vector, opts, filter))
            }
            _ => Err(FriendlyError::Unmatched2),
        }
    }
    pub fn vbase<'a>(
        &'a self,
        vector: &'a DynamicVector,
        opts: &'a SearchOptions,
    ) -> Result<impl Iterator<Item = Pointer> + '_, FriendlyError> {
        match (self, vector) {
            (InstanceView::F32Cos(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(vector, opts)) as Box<dyn Iterator<Item = Pointer>>)
            }
            (InstanceView::F32Dot(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(vector, opts)))
            }
            (InstanceView::F32L2(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(vector, opts)))
            }
            (InstanceView::F16Cos(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(vector, opts)))
            }
            (InstanceView::F16Dot(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(vector, opts)))
            }
            (InstanceView::F16L2(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(vector, opts)))
            }
            _ => Err(FriendlyError::Unmatched2),
        }
    }
    pub fn insert(
        &self,
        vector: DynamicVector,
        pointer: Pointer,
    ) -> Result<Result<(), OutdatedError>, FriendlyError> {
        match (self, vector) {
            (InstanceView::F32Cos(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.insert(vector, pointer))
            }
            (InstanceView::F32Dot(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.insert(vector, pointer))
            }
            (InstanceView::F32L2(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.insert(vector, pointer))
            }
            (InstanceView::F16Cos(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.insert(vector, pointer))
            }
            (InstanceView::F16Dot(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.insert(vector, pointer))
            }
            (InstanceView::F16L2(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.insert(vector, pointer))
            }
            _ => Err(FriendlyError::Unmatched2),
        }
    }
    pub fn delete<F: FnMut(Pointer) -> bool>(&self, f: F) {
        match self {
            InstanceView::F32Cos(x) => x.delete(f),
            InstanceView::F32Dot(x) => x.delete(f),
            InstanceView::F32L2(x) => x.delete(f),
            InstanceView::F16Cos(x) => x.delete(f),
            InstanceView::F16Dot(x) => x.delete(f),
            InstanceView::F16L2(x) => x.delete(f),
        }
    }
    pub fn flush(&self) {
        match self {
            InstanceView::F32Cos(x) => x.flush(),
            InstanceView::F32Dot(x) => x.flush(),
            InstanceView::F32L2(x) => x.flush(),
            InstanceView::F16Cos(x) => x.flush(),
            InstanceView::F16Dot(x) => x.flush(),
            InstanceView::F16L2(x) => x.flush(),
        }
    }
}
