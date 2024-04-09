pub mod hnsw;
pub mod utils;

use base::index::*;
use base::scalar::F32;
use base::search::{Filter, Payload, Pointer};
use base::vector::Vecf32Borrowed;
use common::mmap_array::MmapArray;
use std::path::Path;
use utils::read_vecs_file;

pub fn prepare_dataset(data_file: &str, output_dir: &str) {
    let vecs = read_vecs_file::<f32>(data_file).unwrap();
    println!("vecs len: {}", vecs.len());
    println!("vecs dim: {}", vecs.get_d());
    let vecs_iter = (0..vecs.len()).flat_map(|i| vecs.get_vector(i).unwrap().iter().copied());
    MmapArray::create(&Path::new(output_dir).join("vectors"), vecs_iter);
    MmapArray::create(
        &Path::new(output_dir).join("payload"),
        (0..vecs.len()).map(|i| Payload::new(Pointer::new(i as u64), i as u64)),
    );
}

pub fn make_hnsw(data_dir: &str, dims: u32, m: u32, ef_construction: u32, output_dir: &str) {
    let path = Path::new(output_dir);
    let data_path = Path::new(data_dir);
    let options = IndexOptions {
        vector: VectorOptions {
            dims,
            v: base::vector::VectorKind::Vecf32,
            d: base::distance::DistanceKind::L2,
        },
        segment: SegmentsOptions::default(),
        indexing: IndexingOptions::Hnsw(HnswIndexingOptions {
            m,
            ef_construction,
            quantization: Default::default(),
        }),
    };
    hnsw::mock_create(path, data_path, options);
}

pub fn search_hnsw(
    data_dir: &str,
    dims: u32,
    hnsw_dir: &str,
    query_file: &str,
    gt_file: &str,
    ef: u32,
) {
    let path = Path::new(hnsw_dir);
    let data_path = Path::new(data_dir);
    let options = IndexOptions {
        vector: VectorOptions {
            dims,
            v: base::vector::VectorKind::Vecf32,
            d: base::distance::DistanceKind::L2,
        },
        segment: SegmentsOptions::default(),
        indexing: IndexingOptions::default(),
    };
    let hnsw = hnsw::mock_open(path, data_path, options);
    let queries = read_vecs_file::<f32>(query_file).unwrap();
    let nq = queries.len();
    let queries = (0..nq)
        .flat_map(|i| queries.get_vector(i).unwrap().iter().map(|&x| F32(x)))
        .collect::<Vec<F32>>();
    let gt = read_vecs_file::<i32>(gt_file).unwrap();
    let mut results = Vec::new();
    #[derive(Clone)]
    struct FilterStruct {}
    impl Filter for FilterStruct {
        fn check(&mut self, _: Payload) -> bool {
            true
        }
    }
    let filter = FilterStruct {};
    let opts = SearchOptions {
        prefilter_enable: false,
        hnsw_ef_search: ef,
        ivf_nprobe: 0,
    };
    let k = 10;
    let start = std::time::Instant::now();
    for i in 0..nq {
        let vector = Vecf32Borrowed::new(&queries[i * dims as usize..(i + 1) * dims as usize]);
        let mut heap = hnsw.basic(vector, &opts, filter.clone());
        let mut result = Vec::new();
        while result.len() < k {
            if let Some(e) = heap.pop() {
                result.push(e.0.payload.time());
            } else {
                break;
            }
        }
        results.push(result);
    }
    let end = std::time::Instant::now();
    println!("Time: {:.4}s", (end - start).as_secs_f64());
    let mut recall = 0.0;
    for (i, res) in results.iter().enumerate() {
        let mut correct = 0;
        for &id in res {
            for l in 0..k {
                if gt.get_vector(i).unwrap()[l] as u64 == id {
                    correct += 1;
                    break;
                }
            }
        }
        recall += correct as f64 / k as f64;
    }
    println!("Recall: {:.4}", recall / nq as f64);
}
