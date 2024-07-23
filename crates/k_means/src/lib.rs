#![allow(clippy::needless_range_loop)]

pub mod elkan;
pub mod kmeans1d;
pub mod quick_centers;

use base::scalar::*;
use common::vec2::Vec2;
use kmeans1d::kmeans1d;
use num_traits::Float;
use stoppable_rayon as rayon;

const ITERATIONS: usize = 400;

pub fn k_means<S: ScalarLike, F: FnMut(&mut [S])>(
    c: usize,
    mut samples: Vec2<S>,
    mut spherical: F,
) -> Vec2<S> {
    assert!(c > 0);
    let n = samples.shape_0();
    let dims = samples.shape_1();
    assert!(dims > 0);
    if dims > 1 {
        for i in 0..n {
            spherical(&mut samples[(i,)]);
        }
    }
    if n <= c {
        return quick_centers::quick_centers(c, samples);
    }
    if dims == 1 {
        return Vec2::from_vec((c, 1), kmeans1d(c, samples.as_slice()));
    }
    let mut elkan_k_means = elkan::ElkanKMeans::<S, _>::new(c, samples, spherical);
    for _ in 0..ITERATIONS {
        rayon::check();
        if elkan_k_means.iterate() {
            break;
        }
    }
    elkan_k_means.finish()
}

pub fn k_means_lookup<S: ScalarLike>(vector: &[S], centroids: &Vec2<S>) -> usize {
    assert_ne!(centroids.shape_0(), 0);
    let mut result = (F32::infinity(), 0);
    for i in 0..centroids.shape_0() {
        let dis = S::euclid_distance(vector, &centroids[(i,)]);
        result = std::cmp::min(result, (dis, i));
    }
    result.1
}

pub fn k_means_lookup_many<S: ScalarLike>(vector: &[S], centroids: &Vec2<S>) -> Vec<(F32, usize)> {
    assert_ne!(centroids.shape_0(), 0);
    let mut seq = Vec::new();
    for i in 0..centroids.shape_0() {
        let dis = S::euclid_distance(vector, &centroids[(i,)]);
        seq.push((dis, i));
    }
    seq
}
