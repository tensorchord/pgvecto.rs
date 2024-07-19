use base::scalar::ScalarLike;
use common::vec2::Vec2;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub fn quick_centers<S: ScalarLike>(c: usize, samples: Vec2<S>) -> Vec2<S> {
    let n = samples.shape_0();
    let dims = samples.shape_1();
    assert!(c >= n);
    let mut rand = StdRng::from_entropy();
    let mut centroids = Vec2::zeros((c, dims));
    centroids
        .as_mut_slice()
        .fill_with(|| S::from_f32(rand.gen_range(0.0..1.0f32)));
    let index = {
        let mut index = (0..n).collect::<Vec<_>>();
        index.sort_by_key(|&i| &samples[(i,)]);
        index.dedup_by_key(|&mut i| &samples[(i,)]);
        index
    };
    for i in index {
        centroids[(i,)].copy_from_slice(&samples[(i,)]);
    }
    centroids
}
