use std::path::Path;
use base::index::{HnswIndexingOptions, IndexOptions, OptimizingOptions, SegmentsOptions, VectorOptions};

fn main() {
    make().unwrap();
}

fn make() -> Result<(), std::io::Error> {
    let path = Path::new("/home/yanqi/stand-alone-test/data/hnsw");
    let options = IndexOptions {
        vector: VectorOptions {
            dims: 128,
            v: base::vector::VectorKind::Vecf32,
            d: base::distance::DistanceKind::L2,
        },
        segment: SegmentsOptions::default(),
        optimizing: OptimizingOptions::default(),
        indexing: base::index::IndexingOptions::Hnsw(HnswIndexingOptions {
            m: 16,
            ef_construction: 300,
            quantization: Default::default(),
        }),
    };
    hnsw::mock_create(path, options);
    Ok(())
}