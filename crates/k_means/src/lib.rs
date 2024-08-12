#![allow(clippy::needless_range_loop)]

pub mod elkan;
pub mod kmeans1d;
pub mod lloyd;
pub mod quick_centers;

use base::scalar::*;
use common::vec2::Vec2;
use kmeans1d::kmeans1d;
use num_traits::Float;
use stoppable_rayon as rayon;

pub fn k_means<S: ScalarLike>(c: usize, mut samples: Vec2<S>, is_spherical: bool) -> Vec2<S> {
    assert!(c > 0);
    let n = samples.shape_0();
    let dims = samples.shape_1();
    let spherical = if is_spherical { spherical } else { dummy };
    assert!(dims > 0);
    if dims > 1 && is_spherical {
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
    if dims < 16 || samples.shape_0() < 1024 || rayon::current_num_threads() <= 1 {
        let mut elkan_k_means = elkan::ElkanKMeans::<S, _>::new(c, samples, spherical);
        for _ in 0..400 {
            rayon::check();
            if elkan_k_means.iterate() {
                break;
            }
        }
        return elkan_k_means.finish();
    }
    let mut lloyd_k_means = lloyd::LloydKMeans::<S, _>::new(c, samples, spherical);
    for _ in 0..800 {
        rayon::check();
        if lloyd_k_means.iterate() {
            break;
        }
    }
    lloyd_k_means.finish()
}

pub fn k_means_lookup<S: ScalarLike>(vector: &[S], centroids: &Vec2<S>) -> usize {
    assert_ne!(centroids.shape_0(), 0);
    let mut result = (F32::infinity(), 0);
    for i in 0..centroids.shape_0() {
        let dis = S::impl_l2(vector, &centroids[(i,)]);
        result = std::cmp::min(result, (dis, i));
    }
    result.1
}

pub fn k_means_lookup_many<S: ScalarLike>(vector: &[S], centroids: &Vec2<S>) -> Vec<(F32, usize)> {
    assert_ne!(centroids.shape_0(), 0);
    let mut seq = Vec::new();
    for i in 0..centroids.shape_0() {
        let dis = S::impl_l2(vector, &centroids[(i,)]);
        seq.push((dis, i));
    }
    seq
}

fn spherical<S: ScalarLike>(vector: &mut [S]) {
    let n = vector.len();
    let mut dot = F32(0.0);
    for i in 0..n {
        dot += vector[i].to_f() * vector[i].to_f();
    }
    let l = dot.sqrt();
    for i in 0..n {
        vector[i] /= S::from_f(l);
    }
}

fn dummy<S: ScalarLike>(_: &mut [S]) {}
