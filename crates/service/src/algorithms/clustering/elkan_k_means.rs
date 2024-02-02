use crate::prelude::*;
use crate::utils::vec2::Vec2;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::iter::{IndexedParallelIterator, IntoParallelRefMutIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use std::ops::{Index, IndexMut};

pub struct ElkanKMeans<S: G> {
    dims: u16,
    c: usize,
    pub centroids: Vec2<S>,
    lowerbound: Square,
    upperbound: Vec<F32>,
    assign: Vec<usize>,
    rand: StdRng,
    samples: Vec2<S>,
}

const DELTA: f32 = 1.0 / 1024.0;

impl<S: G> ElkanKMeans<S> {
    pub fn new(c: usize, samples: Vec2<S>) -> Self {
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
            dis.par_iter_mut().enumerate().for_each(|(j, x)| {
                *x = S::elkan_k_means_distance(&samples[j], &centroids[i]);
            });
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

    /// Quick approach if we have little data
    fn quick_centroids(&mut self) -> bool {
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
                    .map(|_| S::Scalar::from_f32(rand.gen_range(0.0..1.0f32)))
                    .collect();
                centroids[i].copy_from_slice(rand_centroids.as_slice());
            }
        }
        for i in n..c {
            let rand_centroids: Vec<_> = (0..dims)
                .map(|_| S::Scalar::from_f32(rand.gen_range(0.0..1.0f32)))
                .collect();
            centroids[i].copy_from_slice(rand_centroids.as_slice());
        }
        true
    }

    pub fn iterate(&mut self) -> bool {
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
            return self.quick_centroids();
        }

        // Step 1
        let mut dist0 = Square::new(c, c);
        let mut sp = vec![F32::zero(); c];
        dist0.v.par_iter_mut().enumerate().for_each(|(ii, v)| {
            let i = ii / c;
            let j = ii % c;
            if i <= j {
                *v = S::elkan_k_means_distance(&centroids[i], &centroids[j]) * 0.5;
            }
        });
        for i in 1..c {
            for j in 0..i - 1 {
                dist0[(i, j)] = dist0[(j, i)];
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
        dis.par_iter_mut().enumerate().for_each(|(i, x)| {
            if upperbound[i] > sp[assign[i]] {
                *x = S::elkan_k_means_distance(&samples[i], &centroids[assign[i]]);
            }
        });
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
                    let dis = S::elkan_k_means_distance(&samples[i], &centroids[j]);
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
        centroids.fill(S::Scalar::zero());
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
                centroids[i][dim] /= S::Scalar::from_f32(count[i].into());
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
                    centroids[i][dim] *= S::Scalar::from_f32(1.0 + DELTA);
                    centroids[o][dim] *= S::Scalar::from_f32(1.0 - DELTA);
                } else {
                    centroids[i][dim] *= S::Scalar::from_f32(1.0 - DELTA);
                    centroids[o][dim] *= S::Scalar::from_f32(1.0 + DELTA);
                }
            }
            count[i] = count[o] / 2.0;
            count[o] = count[o] - count[i];
        }
        centroids.par_chunks_mut(dims as usize).for_each(|v| {
            S::elkan_k_means_normalize(v);
        });

        // Step 5, 6
        let mut dist1 = vec![F32::zero(); c];
        dist1.par_iter_mut().enumerate().for_each(|(i, v)| {
            *v = S::elkan_k_means_distance(&old[i], &centroids[i]);
        });
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

    pub fn finish(self) -> Vec2<S> {
        self.centroids
    }
}

pub struct Square {
    x: usize,
    y: usize,
    v: Vec<F32>,
}

impl Square {
    pub fn new(x: usize, y: usize) -> Self {
        Self {
            x,
            y,
            v: bytemuck::zeroed_vec(x * y),
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
