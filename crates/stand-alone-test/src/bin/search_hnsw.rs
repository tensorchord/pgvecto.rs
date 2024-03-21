use std::path::Path;
use base::{index::{IndexOptions, OptimizingOptions, SearchOptions, SegmentsOptions, VectorOptions}, scalar::F32, search::{Filter, Payload}, vector::Vecf32Borrowed};
use stand_alone_test::utils::read_vecs_file;

#[derive(Clone)]
struct FilterStruct {}
impl Filter for FilterStruct {
    fn check(&mut self, _: Payload) -> bool {
        true
    }
}

fn main() {
    let dims = 128;
    let path = Path::new("data/hnsw");
    let options = IndexOptions {
        vector: VectorOptions {
            dims,
            v: base::vector::VectorKind::Vecf32,
            d: base::distance::DistanceKind::L2,
        },
        segment: SegmentsOptions::default(),
        optimizing: OptimizingOptions::default(),
        indexing: base::index::IndexingOptions::default(),
    };
    let hnsw = hnsw::mock_open(path, options);
    let queries = read_vecs_file::<f32>("data/sift_query.fvecs").unwrap();
    let nq = queries.len();
    let queries = (0..nq).flat_map(|i| {
        queries.get_vector(i).unwrap().iter().map(|&x| F32(x))
    }).collect::<Vec<F32>>();
    let gt = read_vecs_file::<i32>("data/sift_groundtruth.ivecs").unwrap();
    let mut results = Vec::new();
    let filter = FilterStruct {};
    let opts = SearchOptions {
        prefilter_enable: false,
        hnsw_ef_search: 29,
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
    for i in 0..nq {
        let mut correct = 0;
        for j in 0..results[i].len() {
            for l in 0..k {
                if gt.get_vector(i).unwrap()[l] as u64 == results[i][j] {
                    correct += 1;
                    break;
                }
            }
        }
        recall += correct as f64 / k as f64;
    }
    println!("Recall: {:.4}", recall / nq as f64);
}