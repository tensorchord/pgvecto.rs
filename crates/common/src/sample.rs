use crate::vec2::Vec2;
use base::simd::ScalarLike;

pub fn sample<S: ScalarLike, R: AsRef<[S]>>(
    n: u32,
    m: u32,
    d: u32,
    g: impl Fn(u32) -> R,
) -> Vec2<S> {
    let m = std::cmp::min(n, m);
    let f = base::rand::sample_u32(&mut rand::thread_rng(), n, m);
    let mut samples = Vec2::zeros((m as usize, d as usize));
    for i in 0..m {
        samples[(i as usize,)].copy_from_slice(g(f[i as usize]).as_ref());
    }
    samples
}
