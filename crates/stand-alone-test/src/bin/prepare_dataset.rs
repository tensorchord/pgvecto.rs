use std::path::Path;
use base::search::{Payload, Pointer};
use stand_alone_test::utils::read_vecs_file;
use common::mmap_array::MmapArray;

fn main() {
    let vecs = read_vecs_file::<f32>("data/sift_base.fvecs").unwrap();
    println!("vecs len: {}", vecs.len());
    println!("vecs dim: {}", vecs.get_d());
    let vecs_iter = (0..vecs.len()).flat_map(|i| {
        vecs.get_vector(i).unwrap().to_vec().into_iter()
    });
    MmapArray::create(Path::new("data/sift_vectors"), vecs_iter);
    MmapArray::create(Path::new("data/sift_payload"), (0..vecs.len()).map(|i| {
        Payload::new(Pointer::new(i as u64), i as u64)
    }));
}