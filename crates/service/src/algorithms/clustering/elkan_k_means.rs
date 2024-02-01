use crate::prelude::*;
use crate::utils::cells::SyncUnsafeCell;
use crate::utils::vec2::Vec2;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::iter::{
    IndexedParallelIterator, IntoParallelIterator, IntoParallelRefMutIterator, ParallelIterator,
};
use rayon::slice::ParallelSliceMut;
use std::ops::{Index, IndexMut};
use std::sync::atomic::{AtomicUsize, Ordering};

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
        let lowerbound = SyncUnsafeCell::new(Square::new(n, c));
        let mut upperbound = vec![F32::zero(); n];
        let mut assign = vec![0usize; n];

        centroids[0].copy_from_slice(&samples[rand.gen_range(0..n)]);

        let weight = SyncUnsafeCell::new(vec![F32::infinity(); n]);
        for i in 0..c {
            let mut sum = F32::zero();
            (0..n).into_par_iter().for_each(|j| {
                let dis = S::elkan_k_means_distance(&samples[j], &centroids[i]);
                unsafe {
                    (&mut *lowerbound.get())[(j, i)] = dis;
                }
                if dis * dis < weight.get_ref()[j] {
                    unsafe {
                        (&mut *weight.get())[j] = dis * dis;
                    }
                }
            });
            for j in 0..n {
                sum += weight.get_ref()[j];
            }
            if i + 1 == c {
                break;
            }
            let index = 'a: {
                let mut choice = sum * rand.gen_range(0.0..1.0);
                for j in 0..(n - 1) {
                    choice -= weight.get_ref()[j];
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
                let dis = lowerbound.get_ref()[(i, j)];
                if dis < minimal {
                    minimal = dis;
                    target = j;
                }
            }
            assign[i] = target;
            upperbound[i] = minimal;
        }
        let lowerbound = lowerbound.get_ref().clone();

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
        let centroids = &mut self.centroids;
        let change = AtomicUsize::new(0);
        let n = samples.len();
        if n <= c {
            return self.quick_centroids();
        }

        // Step 1
        let dist0 = SyncUnsafeCell::new(Square::new(c, c));
        let mut sp = vec![F32::zero(); c];
        (0..c).into_par_iter().for_each(|i| {
            for j in i + 1..c {
                let dis = S::elkan_k_means_distance(&centroids[i], &centroids[j]) * 0.5;
                unsafe {
                    (&mut *dist0.get())[(i, j)] = dis;
                    (&mut *dist0.get())[(j, i)] = dis;
                }
            }
        });
        let dist0 = dist0.get_ref().clone();
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

        let assign = SyncUnsafeCell::new(self.assign.clone());
        let lowerbound = SyncUnsafeCell::new(self.lowerbound.clone());
        let upperbound = SyncUnsafeCell::new(self.upperbound.clone());

        (0..n).into_par_iter().for_each(|i| {
            // Step 2
            if upperbound.get_ref()[i] <= sp[assign.get_ref()[i]] {
                return;
            }
            let mut minimal =
                S::elkan_k_means_distance(&samples[i], &centroids[assign.get_ref()[i]]);
            unsafe {
                (&mut *lowerbound.get())[(i, assign.get_ref()[i])] = minimal;
                (&mut *upperbound.get())[i] = minimal;
            }
            // Step 3
            for j in 0..c {
                if j == assign.get_ref()[i] {
                    continue;
                }
                if upperbound.get_ref()[i] <= lowerbound.get_ref()[(i, j)] {
                    continue;
                }
                if upperbound.get_ref()[i] <= dist0[(assign.get_ref()[i], j)] {
                    continue;
                }
                if minimal > lowerbound.get_ref()[(i, j)]
                    || minimal > dist0[(assign.get_ref()[i], j)]
                {
                    let dis = S::elkan_k_means_distance(&samples[i], &centroids[j]);
                    unsafe {
                        (&mut *lowerbound.get())[(i, j)] = dis;
                    }
                    if dis < minimal {
                        minimal = dis;
                        unsafe {
                            (&mut *assign.get())[i] = j;
                            (&mut *upperbound.get())[i] = dis;
                        }
                        change.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });

        self.assign = assign.get_ref().clone();
        self.lowerbound = lowerbound.get_ref().clone();
        self.upperbound = upperbound.get_ref().clone();

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

        change.load(Ordering::Relaxed) == 0
    }

    pub fn finish(self) -> Vec2<S> {
        self.centroids
    }
}

#[derive(Clone)]
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
