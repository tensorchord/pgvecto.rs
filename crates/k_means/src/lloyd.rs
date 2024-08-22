use base::scalar::*;
use common::vec2::Vec2;
use num_traits::{Float, Zero};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use stoppable_rayon as rayon;

pub struct LloydKMeans<S, F> {
    dims: usize,
    c: usize,
    spherical: F,
    centroids: Vec<Vec<S>>,
    assign: Vec<usize>,
    rand: StdRng,
    samples: Vec2<S>,
}

const DELTA: f32 = 1.0 / 1024.0;

impl<S: ScalarLike, F: Fn(&mut [S]) + Sync> LloydKMeans<S, F> {
    pub fn new(c: usize, samples: Vec2<S>, spherical: F) -> Self {
        let n = samples.shape_0();
        let dims = samples.shape_1();

        let mut rand = StdRng::from_entropy();
        let mut centroids = Vec::with_capacity(c);
        let mut assign = vec![0usize; n];

        centroids.push(samples[(rand.gen_range(0..n),)].to_vec());

        let mut weight = vec![F32::infinity(); n];
        let mut dis = vec![F32::zero(); n];
        for i in 0..c {
            let mut sum = F32::zero();
            for j in 0..n {
                dis[j] = S::impl_l2(&samples[(j,)], &centroids[i]);
            }
            for j in 0..n {
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
            centroids.push(samples[(index,)].to_vec());
        }

        for j in 0..n {
            let mut minimal = F32::infinity();
            let mut target = 0;
            for i in 0..c {
                let dis = S::impl_l2(&samples[(j,)], &centroids[i]);
                if dis < minimal {
                    minimal = dis;
                    target = i;
                }
            }
            assign[j] = target;
        }

        Self {
            dims,
            c,
            spherical,
            centroids,
            assign,
            rand,
            samples,
        }
    }

    pub fn iterate(&mut self) -> bool {
        let dims = self.dims;
        let c = self.c;
        let rand = &mut self.rand;
        let samples = &self.samples;
        let n = samples.shape_0();

        let (sum, mut count) = (0..n)
            .into_par_iter()
            .fold(
                || (vec![vec![S::zero(); dims]; c], vec![F32::zero(); c]),
                |(mut sum, mut count), i| {
                    for j in 0..dims {
                        sum[self.assign[i]][j] += samples[(i, j)];
                    }
                    count[self.assign[i]] += 1.0;
                    (sum, count)
                },
            )
            .reduce(
                || (vec![vec![S::zero(); dims]; c], vec![F32::zero(); c]),
                |(mut sum, mut count), (sum_1, count_1)| {
                    for i in 0..c {
                        for k in 0..dims {
                            sum[i][k] += sum_1[i][k];
                        }
                        count[i] += count_1[i];
                    }
                    (sum, count)
                },
            );

        let mut centroids = (0..c)
            .into_par_iter()
            .map(|i| {
                let mut centroid = vec![S::zero(); dims];
                for k in 0..dims {
                    centroid[k] = sum[i][k] / S::from_f32(count[i].into());
                }
                (self.spherical)(&mut centroid);
                centroid
            })
            .collect::<Vec<_>>();

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
            centroids[i] = centroids[o].clone();
            for k in 0..dims {
                if k % 2 == 0 {
                    centroids[i][k] *= S::from_f32(1.0 + DELTA);
                    centroids[o][k] *= S::from_f32(1.0 - DELTA);
                } else {
                    centroids[i][k] *= S::from_f32(1.0 - DELTA);
                    centroids[o][k] *= S::from_f32(1.0 + DELTA);
                }
            }
            count[i] = count[o] / 2.0;
            count[o] = count[o] - count[i];
        }

        let assign = (0..n)
            .into_par_iter()
            .map(|i| {
                let mut result = (F32::infinity(), 0);
                for j in 0..c {
                    let dis = S::impl_l2(&samples[(i,)], &centroids[j]);
                    result = std::cmp::min(result, (dis, j));
                }
                result.1
            })
            .collect::<Vec<_>>();

        let result = (0..n).all(|i| assign[i] == self.assign[i]);

        self.centroids = centroids;
        self.assign = assign;

        result
    }

    pub fn finish(self) -> Vec2<S> {
        let mut centroids = Vec2::zeros((self.c, self.dims));
        for i in 0..self.c {
            centroids[(i,)].copy_from_slice(&self.centroids[i]);
        }
        centroids
    }
}
