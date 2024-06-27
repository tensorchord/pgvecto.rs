#![allow(clippy::needless_range_loop)]

pub mod operator;

use crate::operator::OperatorElkanKMeans;
use base::operator::*;
use base::scalar::*;
use base::vector::VectorBorrowed;
use common::vec2::Vec2;
use num_traits::{Float, Zero};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::ops::{Index, IndexMut};
use stoppable_rayon as rayon;

const ITERATIONS: usize = 100;

pub fn elkan_k_means<O: OperatorElkanKMeans>(
    c: usize,
    mut samples: Vec2<Scalar<O>>,
) -> Vec2<Scalar<O>> {
    assert!(c > 0);
    for i in 0..samples.len() {
        O::elkan_k_means_normalize(&mut samples[i]);
    }
    let mut elkan_k_means = ElkanKMeans::<O>::new(c, samples);
    for _ in 0..ITERATIONS {
        rayon::check();
        if elkan_k_means.iterate() {
            break;
        }
    }
    elkan_k_means.finish()
}

pub fn elkan_k_means_lookup<O: OperatorElkanKMeans>(
    vector: Borrowed<O>,
    centroids: &Vec2<Scalar<O>>,
) -> usize {
    assert!(!centroids.is_empty());
    let mut vector = vector.to_vec();
    O::elkan_k_means_normalize(&mut vector);
    let mut result = (F32::infinity(), 0);
    for i in 0..centroids.len() {
        let dis = O::elkan_k_means_distance(&vector, &centroids[i]);
        result = std::cmp::min(result, (dis, i));
    }
    result.1
}

pub fn elkan_k_means_lookup_dense<O: OperatorElkanKMeans>(
    mut vector: Vec<Scalar<O>>,
    centroids: &Vec2<Scalar<O>>,
) -> usize {
    assert!(!centroids.is_empty());
    O::elkan_k_means_normalize(&mut vector);
    let mut result = (F32::infinity(), 0);
    for i in 0..centroids.len() {
        let dis = O::elkan_k_means_distance(&vector, &centroids[i]);
        result = std::cmp::min(result, (dis, i));
    }
    result.1
}

pub fn elkan_k_means_caluate<O: OperatorElkanKMeans>(
    vector: Borrowed<O>,
    centroids: &Vec2<Scalar<O>>,
) -> Vec<(F32, usize)> {
    assert!(!centroids.is_empty());
    let mut vector = vector.to_vec();
    O::elkan_k_means_normalize(&mut vector);
    let mut seq = Vec::new();
    for i in 0..centroids.len() {
        let dis = O::elkan_k_means_distance(&vector, &centroids[i]);
        seq.push((dis, i));
    }
    seq
}

struct ElkanKMeans<O: Operator> {
    dims: u32,
    c: usize,
    centroids: Vec2<Scalar<O>>,
    lowerbound: Square,
    upperbound: Vec<F32>,
    assign: Vec<usize>,
    rand: StdRng,
    samples: Vec2<Scalar<O>>,
}

const DELTA: f32 = 1.0 / 1024.0;

impl<O: OperatorElkanKMeans> ElkanKMeans<O> {
    fn new(c: usize, samples: Vec2<Scalar<O>>) -> Self {
        let n = samples.len();
        let dims = samples.dims();

        let mut rand = StdRng::from_entropy();
        let mut centroids = Vec2::new(dims, c);
        let mut lowerbound = Square::new(n, c);
        let mut upperbound = vec![F32::zero(); n];
        let mut assign = vec![0usize; n];

        centroids[0].copy_from_slice(&samples[rand.gen_range(0..n)]);

        let mut weight = vec![F32::infinity(); n];
        let mut dis = vec![F32::zero(); n];
        for i in 0..c {
            let mut sum = F32::zero();
            for j in 0..n {
                dis[j] = O::elkan_k_means_distance(&samples[j], &centroids[i]);
            }
            for j in 0..n {
                lowerbound[(j, i)] = dis[j];
                if dis[j] * dis[j] < weight[j] {
                    weight[j] = dis[j] * dis[j];
                }
                sum += weight[j];
            }
            if i + 1 == c {
                break;
            }
            let index = 'a: {
                let mut choice = sum * rand.gen_range(0.0..1.0);
                for j in 0..(n - 1) {
                    choice -= weight[j];
                    if choice <= F32::zero() {
                        break 'a j;
                    }
                }
                n - 1
            };
            centroids[i + 1].copy_from_slice(&samples[index]);
        }

        for i in 0..n {
            let mut minimal = F32::infinity();
            let mut target = 0;
            for j in 0..c {
                let dis = lowerbound[(i, j)];
                if dis < minimal {
                    minimal = dis;
                    target = j;
                }
            }
            assign[i] = target;
            upperbound[i] = minimal;
        }

        Self {
            dims,
            c,
            centroids,
            lowerbound,
            upperbound,
            assign,
            rand,
            samples,
        }
    }

    fn iterate(&mut self) -> bool {
        let c = self.c;
        let dims = self.dims;
        let samples = &self.samples;
        let rand = &mut self.rand;
        let assign = &mut self.assign;
        let centroids = &mut self.centroids;
        let lowerbound = &mut self.lowerbound;
        let upperbound = &mut self.upperbound;
        let mut change = 0;
        let n = samples.len();
        if n <= c {
            let c = self.c;
            let samples = &self.samples;
            let rand = &mut self.rand;
            let centroids = &mut self.centroids;
            let n = samples.len();
            let dims = samples.dims();
            let sorted_index = samples.argsort();
            for i in 0..n {
                let index = sorted_index.get(i).unwrap();
                let last = sorted_index.get(std::cmp::max(i, 1) - 1).unwrap();
                if *index == 0 || samples[*last] != samples[*index] {
                    centroids[i].copy_from_slice(&samples[*index]);
                } else {
                    let rand_centroids: Vec<_> = (0..dims)
                        .map(|_| Scalar::<O>::from_f32(rand.gen_range(0.0..1.0f32)))
                        .collect();
                    centroids[i].copy_from_slice(rand_centroids.as_slice());
                }
            }
            for i in n..c {
                let rand_centroids: Vec<_> = (0..dims)
                    .map(|_| Scalar::<O>::from_f32(rand.gen_range(0.0..1.0f32)))
                    .collect();
                centroids[i].copy_from_slice(rand_centroids.as_slice());
            }
            return true;
        }

        // Step 1
        let mut dist0 = Square::new(c, c);
        let mut sp = vec![F32::zero(); c];
        for i in 0..c {
            for j in 0..c {
                dist0[(i, j)] = O::elkan_k_means_distance(&centroids[i], &centroids[j]) * 0.5;
            }
        }
        for i in 0..c {
            let mut minimal = F32::infinity();
            for j in 0..c {
                if i == j {
                    continue;
                }
                let dis = dist0[(i, j)];
                if dis < minimal {
                    minimal = dis;
                }
            }
            sp[i] = minimal;
        }
        let mut dis = vec![F32::zero(); n];
        for i in 0..n {
            if upperbound[i] > sp[assign[i]] {
                dis[i] = O::elkan_k_means_distance(&samples[i], &centroids[assign[i]]);
            }
        }
        for i in 0..n {
            // Step 2
            if upperbound[i] <= sp[assign[i]] {
                continue;
            }
            let mut minimal = dis[i];
            lowerbound[(i, assign[i])] = minimal;
            upperbound[i] = minimal;
            // Step 3
            for j in 0..c {
                if j == assign[i] {
                    continue;
                }
                if upperbound[i] <= lowerbound[(i, j)] {
                    continue;
                }
                if upperbound[i] <= dist0[(assign[i], j)] {
                    continue;
                }
                if minimal > lowerbound[(i, j)] || minimal > dist0[(assign[i], j)] {
                    let dis = O::elkan_k_means_distance(&samples[i], &centroids[j]);
                    lowerbound[(i, j)] = dis;
                    if dis < minimal {
                        minimal = dis;
                        assign[i] = j;
                        upperbound[i] = dis;
                        change += 1;
                    }
                }
            }
        }

        // Step 4, 7
        let old = std::mem::replace(centroids, Vec2::new(dims, c));
        let mut count = vec![F32::zero(); c];
        centroids.fill(Scalar::<O>::zero());
        for i in 0..n {
            for j in 0..dims as usize {
                centroids[self.assign[i]][j] += samples[i][j];
            }
            count[self.assign[i]] += 1.0;
        }
        for i in 0..c {
            if count[i] == F32::zero() {
                continue;
            }
            for dim in 0..dims as usize {
                centroids[i][dim] /= Scalar::<O>::from_f32(count[i].into());
            }
        }
        for i in 0..c {
            if count[i] != F32::zero() {
                continue;
            }
            let mut o = 0;
            loop {
                let alpha = F32::from_f32(rand.gen_range(0.0..1.0f32));
                let beta = (count[o] - 1.0) / (n - c) as f32;
                if alpha < beta {
                    break;
                }
                o = (o + 1) % c;
            }
            centroids.copy_within(o, i);
            for dim in 0..dims as usize {
                if dim % 2 == 0 {
                    centroids[i][dim] *= Scalar::<O>::from_f32(1.0 + DELTA);
                    centroids[o][dim] *= Scalar::<O>::from_f32(1.0 - DELTA);
                } else {
                    centroids[i][dim] *= Scalar::<O>::from_f32(1.0 - DELTA);
                    centroids[o][dim] *= Scalar::<O>::from_f32(1.0 + DELTA);
                }
            }
            count[i] = count[o] / 2.0;
            count[o] = count[o] - count[i];
        }
        for i in 0..c {
            O::elkan_k_means_normalize(&mut centroids[i]);
        }

        // Step 5, 6
        let mut dist1 = vec![F32::zero(); c];
        for i in 0..c {
            dist1[i] = O::elkan_k_means_distance(&old[i], &centroids[i]);
        }
        for i in 0..n {
            for j in 0..c {
                self.lowerbound[(i, j)] =
                    std::cmp::max(self.lowerbound[(i, j)] - dist1[j], F32::zero());
            }
        }
        for i in 0..n {
            self.upperbound[i] += dist1[self.assign[i]];
        }

        change == 0
    }

    fn finish(self) -> Vec2<Scalar<O>> {
        self.centroids
    }
}

struct Square {
    x: usize,
    y: usize,
    v: Vec<F32>,
}

impl Square {
    pub fn new(x: usize, y: usize) -> Self {
        Self {
            x,
            y,
            v: base::pod::zeroed_vec(x * y),
        }
    }
}

impl Index<(usize, usize)> for Square {
    type Output = F32;

    fn index(&self, (x, y): (usize, usize)) -> &Self::Output {
        debug_assert!(x < self.x);
        debug_assert!(y < self.y);
        &self.v[x * self.y + y]
    }
}

impl IndexMut<(usize, usize)> for Square {
    fn index_mut(&mut self, (x, y): (usize, usize)) -> &mut Self::Output {
        debug_assert!(x < self.x);
        debug_assert!(y < self.y);
        &mut self.v[x * self.y + y]
    }
}
