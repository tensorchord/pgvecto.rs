use crate::index::Index;
use crate::index::IndexOptions;
use crate::index::IndexStat;
use crate::index::IndexView;
use crate::index::OutdatedError;
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
}

impl Instance {
    pub fn create(path: PathBuf, options: IndexOptions) -> Self {
        match (options.vector.d, options.vector.k) {
            (Distance::Cos, Kind::F32) => Self::F32Cos(Index::create(path, options)),
            (Distance::Dot, Kind::F32) => Self::F32Dot(Index::create(path, options)),
            (Distance::L2, Kind::F32) => Self::F32L2(Index::create(path, options)),
            (Distance::Cos, Kind::F16) => Self::F16Cos(Index::create(path, options)),
            (Distance::Dot, Kind::F16) => Self::F16Dot(Index::create(path, options)),
            (Distance::L2, Kind::F16) => Self::F16L2(Index::create(path, options)),
        }
    }
    pub fn open(path: PathBuf, options: IndexOptions) -> Self {
        match (options.vector.d, options.vector.k) {
            (Distance::Cos, Kind::F32) => Self::F32Cos(Index::open(path, options)),
            (Distance::Dot, Kind::F32) => Self::F32Dot(Index::open(path, options)),
            (Distance::L2, Kind::F32) => Self::F32L2(Index::open(path, options)),
            (Distance::Cos, Kind::F16) => Self::F16Cos(Index::open(path, options)),
            (Distance::Dot, Kind::F16) => Self::F16Dot(Index::open(path, options)),
            (Distance::L2, Kind::F16) => Self::F16L2(Index::open(path, options)),
        }
    }
    pub fn options(&self) -> &IndexOptions {
        match self {
            Instance::F32Cos(x) => x.options(),
            Instance::F32Dot(x) => x.options(),
            Instance::F32L2(x) => x.options(),
            Instance::F16Cos(x) => x.options(),
            Instance::F16Dot(x) => x.options(),
            Instance::F16L2(x) => x.options(),
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
        }
    }
    pub fn view(&self) -> InstanceView {
        match self {
            Instance::F32Cos(x) => InstanceView::F32Cos(x.view()),
            Instance::F32Dot(x) => InstanceView::F32Dot(x.view()),
            Instance::F32L2(x) => InstanceView::F32L2(x.view()),
            Instance::F16Cos(x) => InstanceView::F16Cos(x.view()),
            Instance::F16Dot(x) => InstanceView::F16Dot(x.view()),
            Instance::F16L2(x) => InstanceView::F16L2(x.view()),
        }
    }
    pub fn stat(&self) -> IndexStat {
        match self {
            Instance::F32Cos(x) => x.stat(),
            Instance::F32Dot(x) => x.stat(),
            Instance::F32L2(x) => x.stat(),
            Instance::F16Cos(x) => x.stat(),
            Instance::F16Dot(x) => x.stat(),
            Instance::F16L2(x) => x.stat(),
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
        k: usize,
        vector: DynamicVector,
        filter: F,
    ) -> Result<Vec<Pointer>, FriendlyError> {
        match (self, vector) {
            (InstanceView::F32Cos(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(k, &vector, filter))
            }
            (InstanceView::F32Dot(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(k, &vector, filter))
            }
            (InstanceView::F32L2(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(k, &vector, filter))
            }
            (InstanceView::F16Cos(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(k, &vector, filter))
            }
            (InstanceView::F16Dot(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(k, &vector, filter))
            }
            (InstanceView::F16L2(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(x.search(k, &vector, filter))
            }
            _ => Err(FriendlyError::Unmatched2),
        }
    }
    pub fn vbase(
        &self,
        vector: DynamicVector,
    ) -> Result<impl Iterator<Item = Pointer> + '_, FriendlyError> {
        match (self, vector) {
            (InstanceView::F32Cos(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(&vector)) as Box<dyn Iterator<Item = Pointer>>)
            }
            (InstanceView::F32Dot(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(&vector)))
            }
            (InstanceView::F32L2(x), DynamicVector::F32(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(&vector)))
            }
            (InstanceView::F16Cos(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(&vector)))
            }
            (InstanceView::F16Dot(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(&vector)))
            }
            (InstanceView::F16L2(x), DynamicVector::F16(vector)) => {
                if x.options.vector.dims as usize != vector.len() {
                    return Err(FriendlyError::Unmatched2);
                }
                Ok(Box::new(x.vbase(&vector)))
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
