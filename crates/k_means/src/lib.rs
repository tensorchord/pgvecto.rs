#![allow(clippy::needless_range_loop)]

pub mod elkan;
pub mod kmeans1d;
pub mod lloyd;
pub mod quick_centers;

use base::scalar::*;
use common::vec2::Vec2;
use elkan::ElkanKMeans;
use kmeans1d::kmeans1d;
use lloyd::LloydKMeans;
use stoppable_rayon as rayon;

pub fn k_means<S: ScalarLike>(
    c: usize,
    mut samples: Vec2<S>,
    prefer_multithreading: bool,
    is_spherical: bool,
) -> Vec2<S> {
    assert!(c > 0);
    let n = samples.shape_0();
    let dims = samples.shape_1();
    assert!(dims > 0);
    if is_spherical {
        for i in 0..n {
            let sample = &mut samples[(i,)];
            let l = S::reduce_sum_of_x2(sample).sqrt();
            S::vector_mul_scalar_inplace(sample, 1.0 / l);
        }
    }
    if n <= c {
        return quick_centers::quick_centers(c, samples);
    }
    if dims == 1 {
        let samples = S::vector_to_f32(samples.as_slice());
        let centroids = S::vector_from_f32(&kmeans1d(c, samples.as_slice()));
        return Vec2::from_vec((c, 1), centroids);
    }
    if prefer_multithreading {
        let mut lloyd_k_means = LloydKMeans::new(c, samples, is_spherical);
        for _ in 0..25 {
            rayon::check();
            if lloyd_k_means.iterate() {
                break;
            }
        }
        lloyd_k_means.finish()
    } else {
        let mut elkan_k_means = ElkanKMeans::new(c, samples, is_spherical);
        for _ in 0..100 {
            rayon::check();
            if elkan_k_means.iterate() {
                break;
            }
        }
        elkan_k_means.finish()
    }
}

pub fn k_means_lookup<S: ScalarLike>(vector: &[S], centroids: &Vec2<S>) -> usize {
    assert_ne!(centroids.shape_0(), 0);
    let mut result = (f32::INFINITY, 0);
    for i in 0..centroids.shape_0() {
        let dis = S::reduce_sum_of_d2(vector, &centroids[(i,)]);
        if dis <= result.0 {
            result = (dis, i);
        }
    }
    result.1
}

pub fn k_means_lookup_many<S: ScalarLike>(vector: &[S], centroids: &Vec2<S>) -> Vec<(f32, usize)> {
    assert_ne!(centroids.shape_0(), 0);
    let mut seq = Vec::new();
    for i in 0..centroids.shape_0() {
        let dis = S::reduce_sum_of_d2(vector, &centroids[(i,)]);
        seq.push((dis, i));
    }
    seq
}
