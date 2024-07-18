#![allow(clippy::needless_range_loop)]

pub mod elkan;

use base::scalar::*;
use common::vec2::Vec2;
use num_traits::Float;
use stoppable_rayon as rayon;

const ITERATIONS: usize = 400;

pub fn k_means<S: ScalarLike>(c: usize, samples: Vec2<S>) -> Vec2<S> {
    assert!(c > 0);
    let mut elkan_k_means = elkan::ElkanKMeans::<S>::new(c, samples);
    for _ in 0..ITERATIONS {
        rayon::check();
        if elkan_k_means.iterate() {
            break;
        }
    }
    elkan_k_means.finish()
}

pub fn k_means_lookup<S: ScalarLike>(vector: &[S], centroids: &Vec2<S>) -> usize {
    assert!(!centroids.is_empty());
    let mut result = (F32::infinity(), 0);
    for i in 0..centroids.len() {
        let dis = S::euclid_distance(vector, &centroids[i]);
        result = std::cmp::min(result, (dis, i));
    }
    result.1
}

pub fn k_means_lookup_many<S: ScalarLike>(vector: &[S], centroids: &Vec2<S>) -> Vec<(F32, usize)> {
    assert!(!centroids.is_empty());
    let mut seq = Vec::new();
    for i in 0..centroids.len() {
        let dis = S::euclid_distance(vector, &centroids[i]);
        seq.push((dis, i));
    }
    seq
}
