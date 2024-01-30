pub mod metadata;

use crate::index::Index;
use crate::index::IndexOptions;
use crate::index::IndexStat;
use crate::index::IndexView;
use crate::index::SearchOptions;
use crate::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

#[cfg(test)]
use mockall::mock;

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

// #[derive(Clone)] is not supported by automock
#[cfg(test)]
mock! {
    pub Instance {
        pub fn create(path: PathBuf, options: IndexOptions) -> Result<Self, ServiceError>;
        pub fn open(path: PathBuf) -> Self;
        pub fn refresh(&self);
        pub fn view(&self) -> Option<InstanceView>;
        pub fn stat(&self) -> IndexStat;
    }
    impl Clone for Instance {
        fn clone(&self) -> Self;
    }
}

impl Instance {
    pub fn create(path: PathBuf, options: IndexOptions) -> Result<Self, ServiceError> {
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
    pub fn refresh(&self) {
        match self {
            Instance::F32Cos(x) => x.refresh(),
            Instance::F32Dot(x) => x.refresh(),
            Instance::F32L2(x) => x.refresh(),
            Instance::F16Cos(x) => x.refresh(),
            Instance::F16Dot(x) => x.refresh(),
            Instance::F16L2(x) => x.refresh(),
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
            Instance::Upgrade => None,
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
            Instance::Upgrade => IndexStat::Upgrade,
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

// `generic lifetime as part of the return type` is not supported by mockall
// rewrite its lifetime for test
#[cfg(test)]
mock! {
    pub InstanceView {
        pub fn basic<F: Fn(Pointer) -> bool + Clone + 'static>(
            &self,
            vector: &DynamicVector,
            opts: &SearchOptions,
            filter: F,
        ) -> Result<Box<dyn Iterator<Item = Pointer>>, ServiceError>;
        pub fn vbase<F: FnMut(Pointer) -> bool + Clone + 'static>(
            &self,
            vector: &DynamicVector,
            opts: &SearchOptions,
            filter: F,
        ) -> Result<Box<dyn Iterator<Item = Pointer>>, ServiceError>;
        pub fn insert(&self, vector: DynamicVector, pointer: Pointer) -> Result<(), ServiceError>;
        pub fn delete<F: FnMut(Pointer) -> bool + 'static>(&self, f: F);
        pub fn flush(&self);
    }
}

impl InstanceView {
    pub fn basic<'a, F: Fn(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: &'a DynamicVector,
        opts: &'a SearchOptions,
        filter: F,
    ) -> Result<Box<dyn Iterator<Item = Pointer> + 'a>, ServiceError> {
        match (self, vector) {
            (InstanceView::F32Cos(x), DynamicVector::F32(vector)) => {
                Ok(Box::new(x.basic(vector, opts, filter)?))
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
            _ => Err(ServiceError::Unmatched),
        }
    }
    pub fn vbase<'a, F: FnMut(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: &'a DynamicVector,
        opts: &'a SearchOptions,
        filter: F,
    ) -> Result<Box<dyn Iterator<Item = Pointer> + 'a>, ServiceError> {
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
            _ => Err(ServiceError::Unmatched),
        }
    }
    pub fn insert(&self, vector: DynamicVector, pointer: Pointer) -> Result<(), ServiceError> {
        match (self, vector) {
            (InstanceView::F32Cos(x), DynamicVector::F32(vector)) => x.insert(vector, pointer),
            (InstanceView::F32Dot(x), DynamicVector::F32(vector)) => x.insert(vector, pointer),
            (InstanceView::F32L2(x), DynamicVector::F32(vector)) => x.insert(vector, pointer),
            (InstanceView::F16Cos(x), DynamicVector::F16(vector)) => x.insert(vector, pointer),
            (InstanceView::F16Dot(x), DynamicVector::F16(vector)) => x.insert(vector, pointer),
            (InstanceView::F16L2(x), DynamicVector::F16(vector)) => x.insert(vector, pointer),
            _ => Err(ServiceError::Unmatched),
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
