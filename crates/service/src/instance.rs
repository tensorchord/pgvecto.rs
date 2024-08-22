use base::distance::*;
use base::index::*;
use base::operator::*;
use base::search::*;
use base::vector::*;
use base::worker::*;
use half::f16;
use index::Index;
use index::IndexTracker;
use index::IndexView;
use index::OutdatedError;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub enum Instance {
    Vecf32Dot(Arc<Index<VectDot<f32>>>),
    Vecf32L2(Arc<Index<VectL2<f32>>>),
    Vecf16Dot(Arc<Index<VectDot<f16>>>),
    Vecf16L2(Arc<Index<VectL2<f16>>>),
    SVecf32Dot(Arc<Index<SVectDot<f32>>>),
    SVecf32L2(Arc<Index<SVectL2<f32>>>),
    BVectorDot(Arc<Index<BVectorDot>>),
    BVectorHamming(Arc<Index<BVectorHamming>>),
    BVectorJaccard(Arc<Index<BVectorJaccard>>),
}

impl Instance {
    pub fn create(
        path: PathBuf,
        options: IndexOptions,
        alterable_options: IndexAlterableOptions,
    ) -> Result<Self, CreateError> {
        match (options.vector.v, options.vector.d) {
            (VectorKind::Vecf32, DistanceKind::Dot) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::Vecf32Dot(index))
            }
            (VectorKind::Vecf32, DistanceKind::L2) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::Vecf32L2(index))
            }
            (VectorKind::Vecf16, DistanceKind::Dot) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::Vecf16Dot(index))
            }
            (VectorKind::Vecf16, DistanceKind::L2) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::Vecf16L2(index))
            }
            (VectorKind::SVecf32, DistanceKind::Dot) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::SVecf32Dot(index))
            }
            (VectorKind::SVecf32, DistanceKind::L2) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::SVecf32L2(index))
            }
            (VectorKind::BVector, DistanceKind::Dot) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::BVectorDot(index))
            }
            (VectorKind::BVector, DistanceKind::Hamming) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::BVectorHamming(index))
            }
            (VectorKind::BVector, DistanceKind::Jaccard) => {
                let index = Index::create(path.clone(), options, alterable_options)?;
                Ok(Self::BVectorJaccard(index))
            }
            _ => Err(CreateError::InvalidIndexOptions {
                reason: "vector index config is not supported".to_string(),
            }),
        }
    }
    pub fn open(path: PathBuf) -> Self {
        let options =
            serde_json::from_slice::<IndexOptions>(&std::fs::read(path.join("options")).unwrap())
                .unwrap();
        match (options.vector.v, options.vector.d) {
            (VectorKind::Vecf32, DistanceKind::Dot) => Self::Vecf32Dot(Index::open(path)),
            (VectorKind::Vecf32, DistanceKind::L2) => Self::Vecf32L2(Index::open(path)),
            (VectorKind::Vecf16, DistanceKind::Dot) => Self::Vecf16Dot(Index::open(path)),
            (VectorKind::Vecf16, DistanceKind::L2) => Self::Vecf16L2(Index::open(path)),
            (VectorKind::SVecf32, DistanceKind::Dot) => Self::SVecf32Dot(Index::open(path)),
            (VectorKind::SVecf32, DistanceKind::L2) => Self::SVecf32L2(Index::open(path)),
            (VectorKind::BVector, DistanceKind::Dot) => Self::BVectorDot(Index::open(path)),
            (VectorKind::BVector, DistanceKind::Hamming) => Self::BVectorHamming(Index::open(path)),
            (VectorKind::BVector, DistanceKind::Jaccard) => Self::BVectorJaccard(Index::open(path)),
            _ => unreachable!(),
        }
    }
    pub fn refresh(&self) {
        match self {
            Instance::Vecf32Dot(x) => x.refresh(),
            Instance::Vecf32L2(x) => x.refresh(),
            Instance::Vecf16Dot(x) => x.refresh(),
            Instance::Vecf16L2(x) => x.refresh(),
            Instance::SVecf32Dot(x) => x.refresh(),
            Instance::SVecf32L2(x) => x.refresh(),
            Instance::BVectorDot(x) => x.refresh(),
            Instance::BVectorHamming(x) => x.refresh(),
            Instance::BVectorJaccard(x) => x.refresh(),
        }
    }
    pub fn view(&self) -> InstanceView {
        match self {
            Instance::Vecf32Dot(x) => InstanceView::Vecf32Dot(x.view()),
            Instance::Vecf32L2(x) => InstanceView::Vecf32L2(x.view()),
            Instance::Vecf16Dot(x) => InstanceView::Vecf16Dot(x.view()),
            Instance::Vecf16L2(x) => InstanceView::Vecf16L2(x.view()),
            Instance::SVecf32Dot(x) => InstanceView::SVecf32Dot(x.view()),
            Instance::SVecf32L2(x) => InstanceView::SVecf32L2(x.view()),
            Instance::BVectorDot(x) => InstanceView::BVectorDot(x.view()),
            Instance::BVectorHamming(x) => InstanceView::BVectorHamming(x.view()),
            Instance::BVectorJaccard(x) => InstanceView::BVectorJaccard(x.view()),
        }
    }
    pub fn stat(&self) -> IndexStat {
        match self {
            Instance::Vecf32Dot(x) => x.stat(),
            Instance::Vecf32L2(x) => x.stat(),
            Instance::Vecf16Dot(x) => x.stat(),
            Instance::Vecf16L2(x) => x.stat(),
            Instance::SVecf32Dot(x) => x.stat(),
            Instance::SVecf32L2(x) => x.stat(),
            Instance::BVectorDot(x) => x.stat(),
            Instance::BVectorHamming(x) => x.stat(),
            Instance::BVectorJaccard(x) => x.stat(),
        }
    }
    pub fn alter(&self, key: &str, value: &str) -> Result<(), AlterError> {
        match self {
            Instance::Vecf32Dot(x) => x.alter(key, value),
            Instance::Vecf32L2(x) => x.alter(key, value),
            Instance::Vecf16Dot(x) => x.alter(key, value),
            Instance::Vecf16L2(x) => x.alter(key, value),
            Instance::SVecf32Dot(x) => x.alter(key, value),
            Instance::SVecf32L2(x) => x.alter(key, value),
            Instance::BVectorDot(x) => x.alter(key, value),
            Instance::BVectorHamming(x) => x.alter(key, value),
            Instance::BVectorJaccard(x) => x.alter(key, value),
        }
    }
    pub fn delete(&self, pointer: Pointer) -> Result<(), DeleteError> {
        match self {
            Instance::Vecf32Dot(x) => x.delete(pointer),
            Instance::Vecf32L2(x) => x.delete(pointer),
            Instance::Vecf16Dot(x) => x.delete(pointer),
            Instance::Vecf16L2(x) => x.delete(pointer),
            Instance::SVecf32Dot(x) => x.delete(pointer),
            Instance::SVecf32L2(x) => x.delete(pointer),
            Instance::BVectorDot(x) => x.delete(pointer),
            Instance::BVectorHamming(x) => x.delete(pointer),
            Instance::BVectorJaccard(x) => x.delete(pointer),
        }
    }
    pub fn start(&self) {
        match self {
            Instance::Vecf32Dot(x) => x.start(),
            Instance::Vecf32L2(x) => x.start(),
            Instance::Vecf16Dot(x) => x.start(),
            Instance::Vecf16L2(x) => x.start(),
            Instance::SVecf32Dot(x) => x.start(),
            Instance::SVecf32L2(x) => x.start(),
            Instance::BVectorDot(x) => x.start(),
            Instance::BVectorHamming(x) => x.start(),
            Instance::BVectorJaccard(x) => x.start(),
        }
    }
    pub fn stop(&self) {
        match self {
            Instance::Vecf32Dot(x) => x.stop(),
            Instance::Vecf32L2(x) => x.stop(),
            Instance::Vecf16Dot(x) => x.stop(),
            Instance::Vecf16L2(x) => x.stop(),
            Instance::SVecf32Dot(x) => x.stop(),
            Instance::SVecf32L2(x) => x.stop(),
            Instance::BVectorDot(x) => x.stop(),
            Instance::BVectorHamming(x) => x.stop(),
            Instance::BVectorJaccard(x) => x.stop(),
        }
    }
    pub fn wait(&self) -> Arc<IndexTracker> {
        match self {
            Instance::Vecf32Dot(x) => x.wait(),
            Instance::Vecf32L2(x) => x.wait(),
            Instance::Vecf16Dot(x) => x.wait(),
            Instance::Vecf16L2(x) => x.wait(),
            Instance::SVecf32Dot(x) => x.wait(),
            Instance::SVecf32L2(x) => x.wait(),
            Instance::BVectorDot(x) => x.wait(),
            Instance::BVectorHamming(x) => x.wait(),
            Instance::BVectorJaccard(x) => x.wait(),
        }
    }
}

pub enum InstanceView {
    Vecf32Dot(Arc<IndexView<VectDot<f32>>>),
    Vecf32L2(Arc<IndexView<VectL2<f32>>>),
    Vecf16Dot(Arc<IndexView<VectDot<f16>>>),
    Vecf16L2(Arc<IndexView<VectL2<f16>>>),
    SVecf32Dot(Arc<IndexView<SVectDot<f32>>>),
    SVecf32L2(Arc<IndexView<SVectL2<f32>>>),
    BVectorDot(Arc<IndexView<BVectorDot>>),
    BVectorHamming(Arc<IndexView<BVectorHamming>>),
    BVectorJaccard(Arc<IndexView<BVectorJaccard>>),
}

impl ViewVbaseOperations for InstanceView {
    fn vbase<'a>(
        &'a self,
        vector: &'a OwnedVector,
        opts: &'a SearchOptions,
    ) -> Result<Box<dyn Iterator<Item = (Distance, Pointer)> + 'a>, VbaseError> {
        match (self, vector) {
            (InstanceView::Vecf32Dot(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::Vecf32L2(x), OwnedVector::Vecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::Vecf16Dot(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::Vecf16L2(x), OwnedVector::Vecf16(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::SVecf32Dot(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::SVecf32L2(x), OwnedVector::SVecf32(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::BVectorDot(x), OwnedVector::BVector(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::BVectorHamming(x), OwnedVector::BVector(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            (InstanceView::BVectorJaccard(x), OwnedVector::BVector(vector)) => {
                Ok(Box::new(x.vbase(vector.as_borrowed(), opts)?))
            }
            _ => Err(VbaseError::InvalidVector),
        }
    }
}

impl ViewListOperations for InstanceView {
    fn list(&self) -> Result<Box<dyn Iterator<Item = Pointer> + '_>, ListError> {
        match self {
            InstanceView::Vecf32Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::Vecf32L2(x) => Ok(Box::new(x.list()?)),
            InstanceView::Vecf16Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::Vecf16L2(x) => Ok(Box::new(x.list()?)),
            InstanceView::SVecf32Dot(x) => Ok(Box::new(x.list()?)),
            InstanceView::SVecf32L2(x) => Ok(Box::new(x.list()?)),
            InstanceView::BVectorDot(x) => Ok(Box::new(x.list()?)),
            InstanceView::BVectorHamming(x) => Ok(Box::new(x.list()?)),
            InstanceView::BVectorJaccard(x) => Ok(Box::new(x.list()?)),
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
            (InstanceView::Vecf32Dot(x), OwnedVector::Vecf32(vector)) => x.insert(vector, pointer),
            (InstanceView::Vecf32L2(x), OwnedVector::Vecf32(vector)) => x.insert(vector, pointer),
            (InstanceView::Vecf16Dot(x), OwnedVector::Vecf16(vector)) => x.insert(vector, pointer),
            (InstanceView::Vecf16L2(x), OwnedVector::Vecf16(vector)) => x.insert(vector, pointer),
            (InstanceView::SVecf32Dot(x), OwnedVector::SVecf32(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::SVecf32L2(x), OwnedVector::SVecf32(vector)) => x.insert(vector, pointer),
            (InstanceView::BVectorDot(x), OwnedVector::BVector(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::BVectorHamming(x), OwnedVector::BVector(vector)) => {
                x.insert(vector, pointer)
            }
            (InstanceView::BVectorJaccard(x), OwnedVector::BVector(vector)) => {
                x.insert(vector, pointer)
            }
            _ => Err(InsertError::InvalidVector),
        }
    }
    pub fn flush(&self) -> Result<(), FlushError> {
        match self {
            InstanceView::Vecf32Dot(x) => x.flush(),
            InstanceView::Vecf32L2(x) => x.flush(),
            InstanceView::Vecf16Dot(x) => x.flush(),
            InstanceView::Vecf16L2(x) => x.flush(),
            InstanceView::SVecf32Dot(x) => x.flush(),
            InstanceView::SVecf32L2(x) => x.flush(),
            InstanceView::BVectorDot(x) => x.flush(),
            InstanceView::BVectorHamming(x) => x.flush(),
            InstanceView::BVectorJaccard(x) => x.flush(),
        }
    }
}
