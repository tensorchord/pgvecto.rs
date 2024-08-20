use base::distance::DistanceKind;
use base::index::{IndexOptions, SearchOptions};
use base::operator::*;
use base::scalar::F32;
use base::search::{Collection, Element, Pointer, Source, Vectors};
use base::vector::*;
use std::path::Path;

#[allow(dead_code)]
pub enum Indexing {
    Vecf32Dot(indexing::SealedIndexing<Vecf32Dot>),
    Vecf32L2(indexing::SealedIndexing<Vecf32L2>),
    Vecf16Dot(indexing::SealedIndexing<Vecf16Dot>),
    Vecf16L2(indexing::SealedIndexing<Vecf16L2>),
    BVectorDot(indexing::SealedIndexing<BVectorDot>),
    BVectorHamming(indexing::SealedIndexing<BVectorHamming>),
    BVectorJaccard(indexing::SealedIndexing<BVectorJaccard>),
    SVecf32Dot(indexing::SealedIndexing<SVecf32Dot>),
    SVecf32L2(indexing::SealedIndexing<SVecf32L2>),
}

impl Indexing {
    pub fn create(
        path: impl AsRef<Path>,
        index_options: IndexOptions,
        source: impl Vectors<Vecf32Owned> + Collection + Source + Sync,
    ) -> Self {
        let path = path.as_ref();
        match (index_options.vector.v, index_options.vector.d) {
            (VectorKind::Vecf32, DistanceKind::L2) => Self::Vecf32L2(
                stoppable_rayon::ThreadPoolBuilder::new()
                    .build_scoped(|pool| {
                        pool.install(|| {
                            let x = indexing::SealedIndexing::create(
                                &path,
                                index_options.clone(),
                                &source,
                            );
                            // write options
                            std::fs::write(
                                path.join(".index_options"),
                                serde_json::to_string(&index_options).unwrap(),
                            )
                            .unwrap();
                            x
                        })
                    })
                    .unwrap()
                    .unwrap(),
            ),
            (VectorKind::Vecf32, DistanceKind::Dot) => Self::Vecf32Dot(
                stoppable_rayon::ThreadPoolBuilder::new()
                    .build_scoped(|pool| {
                        pool.install(|| {
                            let x = indexing::SealedIndexing::create(
                                &path,
                                index_options.clone(),
                                &source,
                            );
                            // write options
                            std::fs::write(
                                path.join(".index_options"),
                                serde_json::to_string(&index_options).unwrap(),
                            )
                            .unwrap();
                            x
                        })
                    })
                    .unwrap()
                    .unwrap(),
            ),
            _ => unimplemented!(),
        }
    }
    pub fn open(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        // read options
        let index_options: IndexOptions =
            serde_json::from_slice(&std::fs::read(path.join(".index_options")).unwrap()).unwrap();
        match (index_options.vector.v, index_options.vector.d) {
            (VectorKind::Vecf32, DistanceKind::L2) => {
                Self::Vecf32L2(indexing::SealedIndexing::open(path, index_options))
            }
            (VectorKind::Vecf32, DistanceKind::Dot) => {
                Self::Vecf32Dot(indexing::SealedIndexing::open(path, index_options))
            }
            _ => unimplemented!(),
        }
    }
    pub fn vbase<'a>(
        &'a self,
        vector: BorrowedVector<'a>,
        opts: &'a SearchOptions,
    ) -> impl Iterator<Item = (F32, Pointer)> + 'a {
        match (self, vector) {
            (Self::Vecf32L2(x), BorrowedVector::Vecf32(vector)) => x.vbase(vector, opts),
            (Self::Vecf32Dot(x), BorrowedVector::Vecf32(vector)) => x.vbase(vector, opts),
            (Self::Vecf16Dot(x), BorrowedVector::Vecf16(vector)) => x.vbase(vector, opts),
            (Self::Vecf16L2(x), BorrowedVector::Vecf16(vector)) => x.vbase(vector, opts),
            (Self::BVectorDot(x), BorrowedVector::BVector(vector)) => x.vbase(vector, opts),
            (Self::BVectorHamming(x), BorrowedVector::BVector(vector)) => x.vbase(vector, opts),
            (Self::BVectorJaccard(x), BorrowedVector::BVector(vector)) => x.vbase(vector, opts),
            (Self::SVecf32Dot(x), BorrowedVector::SVecf32(vector)) => x.vbase(vector, opts),
            (Self::SVecf32L2(x), BorrowedVector::SVecf32(vector)) => x.vbase(vector, opts),
            _ => panic!("invalid vector type"),
        }
        .map(|Element { distance, payload }| (distance, payload.0.pointer()))
    }
    pub fn dims(&self) -> u32 {
        match self {
            Indexing::Vecf32Dot(x) => x.dims(),
            Indexing::Vecf32L2(x) => x.dims(),
            Indexing::Vecf16Dot(x) => x.dims(),
            Indexing::Vecf16L2(x) => x.dims(),
            Indexing::BVectorDot(x) => x.dims(),
            Indexing::BVectorHamming(x) => x.dims(),
            Indexing::BVectorJaccard(x) => x.dims(),
            Indexing::SVecf32Dot(x) => x.dims(),
            Indexing::SVecf32L2(x) => x.dims(),
        }
    }
}
