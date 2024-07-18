use crate::vec2::Vec2;
use base::operator::{Borrowed, Operator, Owned, Scalar};
use base::search::Vectors;
use base::vector::VectorBorrowed;
use base::vector::VectorOwned;

const SAMPLES: usize = 65536;

pub fn sample<O: Operator>(vectors: &impl Vectors<O>) -> Vec2<Scalar<O>> {
    let n = vectors.len();
    let m = std::cmp::min(SAMPLES as u32, n);
    let f = super::rand::sample_u32(&mut rand::thread_rng(), n, m);
    let mut samples = Vec2::new(vectors.dims(), m as usize);
    for i in 0..m {
        let v = vectors.vector(f[i as usize] as u32).to_vec();
        samples[i as usize].copy_from_slice(&v);
    }
    samples
}

pub fn sample_subvector_transform<O: Operator>(
    vectors: &impl Vectors<O>,
    s: usize,
    e: usize,
    transform: impl Fn(Borrowed<'_, O>) -> Owned<O>,
) -> Vec2<Scalar<O>> {
    let n = vectors.len();
    let m = std::cmp::min(SAMPLES as u32, n);
    let f = super::rand::sample_u32(&mut rand::thread_rng(), n, m);
    let mut samples = Vec2::new((e - s) as u32, m as usize);
    for i in 0..m {
        let v = transform(vectors.vector(f[i as usize] as u32))
            .as_borrowed()
            .to_vec();
        samples[i as usize].copy_from_slice(&v[s..e]);
    }
    samples
}
