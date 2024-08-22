use base::scalar::*;
use common::vec2::Vec2;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::ops::{Index, IndexMut};

pub struct ElkanKMeans<S, F> {
    dims: usize,
    c: usize,
    spherical: F,
    centroids: Vec2<S>,
    lowerbound: Square,
    upperbound: Vec<f32>,
    assign: Vec<usize>,
    rand: StdRng,
    samples: Vec2<S>,
    first: bool,
}

const DELTA: f32 = 1.0 / 1024.0;

impl<S: ScalarLike, F: FnMut(&mut [S])> ElkanKMeans<S, F> {
    pub fn new(c: usize, samples: Vec2<S>, spherical: F) -> Self {
        let n = samples.shape_0();
        let dims = samples.shape_1();

        let mut rand = StdRng::from_entropy();
        let mut centroids = Vec2::zeros((c, dims));
        let mut lowerbound = Square::new(n, c);
        let mut upperbound = vec![0.0f32; n];
        let mut assign = vec![0usize; n];

        centroids[(0,)].copy_from_slice(&samples[(rand.gen_range(0..n),)]);

        let mut weight = vec![f32::INFINITY; n];
        let mut dis = vec![0.0f32; n];
        for i in 0..c {
            let mut sum = 0.0f32;
            for j in 0..n {
                dis[j] = S::reduce_sum_of_d2(&samples[(j,)], &centroids[(i,)]).sqrt();
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
                    if choice <= 0.0f32 {
                        break 'a j;
                    }
                }
                n - 1
            };
            centroids[(i + 1,)].copy_from_slice(&samples[(index,)]);
        }

        for i in 0..n {
            let mut minimal = f32::INFINITY;
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
            spherical,
            centroids,
            lowerbound,
            upperbound,
            assign,
            rand,
            samples,
            first: true,
        }
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
        let n = samples.shape_0();
        // Step 1
        let mut dist0 = Square::new(c, c);
        let mut sp = vec![0.0f32; c];
        for i in 0..c {
            for j in 0..c {
                dist0[(i, j)] =
                    S::reduce_sum_of_d2(&centroids[(i,)], &centroids[(j,)]).sqrt() * 0.5;
            }
        }
        for i in 0..c {
            let mut minimal = f32::INFINITY;
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
        let mut dis = vec![0.0f32; n];
        for i in 0..n {
            if upperbound[i] > sp[assign[i]] {
                dis[i] = S::reduce_sum_of_d2(&samples[(i,)], &centroids[(assign[i],)]).sqrt();
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
                    let dis = S::reduce_sum_of_d2(&samples[(i,)], &centroids[(j,)]).sqrt();
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
        let old_centroids = std::mem::replace(centroids, Vec2::zeros((c, dims)));
        let mut count = vec![0.0f32; c];
        for i in 0..n {
            S::vector_add_inplace(&mut centroids[(self.assign[i],)], &samples[(i,)]);
            count[self.assign[i]] += 1.0;
        }
        for i in 0..c {
            if count[i] == 0.0f32 {
                continue;
            }
            S::vector_div_scalar_inplace(&mut centroids[(i,)], count[i]);
        }
        for i in 0..c {
            if count[i] != 0.0f32 {
                continue;
            }
            let mut o = 0;
            loop {
                let alpha = f32::from_f32(rand.gen_range(0.0..1.0f32));
                let beta = (count[o] - 1.0) / (n - c) as f32;
                if alpha < beta {
                    break;
                }
                o = (o + 1) % c;
            }
            centroids.copy_within((o,), (i,));
            S::kmeans_helper(&mut centroids[(i,)], 1.0 + DELTA, 1.0 - DELTA);
            S::kmeans_helper(&mut centroids[(o,)], 1.0 - DELTA, 1.0 + DELTA);
            count[i] = count[o] / 2.0;
            count[o] -= count[i];
        }
        for i in 0..c {
            (self.spherical)(&mut centroids[(i,)]);
        }

        // Step 5, 6
        let mut dist1 = vec![0.0f32; c];
        for i in 0..c {
            dist1[i] = S::reduce_sum_of_d2(&old_centroids[(i,)], &centroids[(i,)]).sqrt();
        }
        for i in 0..n {
            for j in 0..c {
                self.lowerbound[(i, j)] = 0.0f32.max(self.lowerbound[(i, j)] - dist1[j]);
            }
        }
        for i in 0..n {
            self.upperbound[i] += dist1[self.assign[i]];
        }
        if self.first {
            self.first = false;
            false
        } else {
            change == 0
        }
    }

    pub fn finish(self) -> Vec2<S> {
        self.centroids
    }
}

struct Square {
    x: usize,
    y: usize,
    v: Vec<f32>,
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
    type Output = f32;

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
