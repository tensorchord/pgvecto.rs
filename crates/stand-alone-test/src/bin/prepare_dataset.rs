use base::search::{Payload, Pointer};
use common::mmap_array::MmapArray;
use stand_alone_test::utils::read_vecs_file;
use std::path::Path;

fn main() {
    let vecs = read_vecs_file::<f32>("/home/yanqi/stand-alone-test/data/sift_base.fvecs").unwrap();
    println!("vecs len: {}", vecs.len());
    println!("vecs dim: {}", vecs.get_d());
    let vecs_iter = (0..vecs.len()).flat_map(|i| vecs.get_vector(i).unwrap().to_vec().into_iter());
    MmapArray::create(
        Path::new("/home/yanqi/stand-alone-test/data/sift_vectors"),
        vecs_iter,
    );
    MmapArray::create(
        Path::new("/home/yanqi/stand-alone-test/data/sift_payload"),
        (0..vecs.len()).map(|i| Payload::new(Pointer::new(i as u64), i as u64)),
    );
}
