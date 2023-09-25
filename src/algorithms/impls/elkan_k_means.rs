use crate::prelude::*;

use crate::algorithms::utils::vec2::Vec2;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::ops::{Index, IndexMut};

pub struct ElkanKMeans {
    dims: u16,
    c: usize,
    pub centroids: Vec2,
    lowerbound: Square,
    upperbound: Vec<Scalar>,
    assign: Vec<usize>,
    rand: StdRng,
    samples: Vec2,
    d: Distance,
}

const DELTA: f32 = 1.0 / 1024.0;

impl ElkanKMeans {
    pub fn new(c: usize, samples: Vec2, d: Distance) -> Self {
        let n = samples.len();
        let dims = samples.dims();

        let mut rand = StdRng::from_entropy();
        let mut centroids = Vec2::new(dims, c);
        let mut lowerbound = Square::new(n, c);
        let mut upperbound = vec![Scalar::Z; n];
        let mut assign = vec![0usize; n];

        centroids[0].copy_from_slice(&samples[rand.gen_range(0..n)]);

        let mut weight = vec![Scalar::INFINITY; n];
        for i in 0..c {
            let mut sum = Scalar::Z;
            for j in 0..n {
                let dis = d.elkan_k_means_distance(&samples[j], &centroids[i]);
                lowerbound[(j, i)] = dis;
                if dis * dis < weight[j] {
                    weight[j] = dis * dis;
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
                    if choice <= Scalar::Z {
                        break 'a j;
                    }
                }
                n - 1
            };
            centroids[i + 1].copy_from_slice(&samples[index]);
        }

        for i in 0..n {
            let mut minimal = Scalar::INFINITY;
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
            d,
        }
    }

    pub fn iterate(&mut self) -> bool {
        let c = self.c;
        let f = |lhs: &[Scalar], rhs: &[Scalar]| self.d.elkan_k_means_distance(lhs, rhs);
        let dims = self.dims;
        let samples = &self.samples;
        let rand = &mut self.rand;
        let assign = &mut self.assign;
        let centroids = &mut self.centroids;
        let lowerbound = &mut self.lowerbound;
        let upperbound = &mut self.upperbound;
        let mut change = 0;
        let n = samples.len();

        // Step 1
        let mut dist0 = Square::new(c, c);
        let mut sp = vec![Scalar::Z; c];
        for i in 0..c {
            for j in i + 1..c {
                let dis = f(&centroids[i], &centroids[j]) * 0.5;
                dist0[(i, j)] = dis;
                dist0[(j, i)] = dis;
            }
        }
        for i in 0..c {
            let mut minimal = Scalar::INFINITY;
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

        for i in 0..n {
            // Step 2
            if upperbound[i] <= sp[assign[i]] {
                continue;
            }
            let mut minimal = f(&samples[i], &centroids[assign[i]]);
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
                    let dis = f(&samples[i], &centroids[j]);
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
        let mut count = vec![Scalar::Z; c];
        centroids.fill(Scalar::Z);
        for i in 0..n {
            for j in 0..dims as usize {
                centroids[assign[i]][j] += samples[i][j];
            }
            count[assign[i]] += 1.0;
        }
        for i in 0..c {
            if count[i] == Scalar::Z {
                continue;
            }
            for dim in 0..dims as usize {
                centroids[i][dim] /= count[i];
            }
        }
        for i in 0..c {
            if count[i] != Scalar::Z {
                continue;
            }
            let mut o = 0;
            loop {
                let alpha = Scalar(rand.gen_range(0.0..1.0));
                let beta = (count[o] - 1.0) / (n - c) as Float;
                if alpha < beta {
                    break;
                }
                o = (o + 1) % c;
            }
            centroids.copy_within(o, i);
            for dim in 0..dims as usize {
                if dim % 2 == 0 {
                    centroids[i][dim] *= 1.0 + DELTA;
                    centroids[o][dim] *= 1.0 - DELTA;
                } else {
                    centroids[i][dim] *= 1.0 - DELTA;
                    centroids[o][dim] *= 1.0 + DELTA;
                }
            }
            count[i] = count[o] / 2.0;
            count[o] = count[o] - count[i];
        }
        for i in 0..c {
            self.d.elkan_k_means_normalize(&mut centroids[i]);
        }

        // Step 5, 6
        let mut dist1 = vec![Scalar::Z; c];
        for i in 0..c {
            dist1[i] = f(&old[i], &centroids[i]);
        }
        for i in 0..n {
            for j in 0..c {
                lowerbound[(i, j)] = (lowerbound[(i, j)] - dist1[j]).max(Scalar::Z);
            }
        }
        for i in 0..n {
            upperbound[i] += dist1[assign[i]];
        }

        change == 0
    }

    pub fn finish(self) -> Vec2 {
        self.centroids
    }
}

pub struct Square {
    x: usize,
    y: usize,
    v: Box<[Scalar]>,
}

impl Square {
    pub fn new(x: usize, y: usize) -> Self {
        Self {
            x,
            y,
            v: unsafe { Box::new_uninit_slice(x * y).assume_init() },
        }
    }
}

impl Index<(usize, usize)> for Square {
    type Output = Scalar;

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
