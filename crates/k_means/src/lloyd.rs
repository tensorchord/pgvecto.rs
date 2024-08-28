use base::scalar::*;
use common::vec2::Vec2;
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

        centroids.push(samples[(rand.gen_range(0..n),)].to_vec());

        let mut weight = vec![f32::INFINITY; n];
        for i in 0..c {
            let dis_2 = (0..n)
                .into_par_iter()
                .map(|j| S::reduce_sum_of_d2(&samples[(j,)], &centroids[i]))
                .collect::<Vec<_>>();
            for j in 0..n {
                if dis_2[j] < weight[j] {
                    weight[j] = dis_2[j];
                }
            }
            let sum = f32::reduce_sum_of_x(&weight);
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
            centroids.push(samples[(index,)].to_vec());
        }

        let assign = (0..n)
            .into_par_iter()
            .map(|i| {
                let mut result = (f32::INFINITY, 0);
                for j in 0..c {
                    let dis_2 = S::reduce_sum_of_d2(&samples[(i,)], &centroids[j]);
                    if dis_2 <= result.0 {
                        result = (dis_2, j);
                    }
                }
                result.1
            })
            .collect::<Vec<_>>();

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
                || (vec![vec![S::zero(); dims]; c], vec![f32::zero(); c]),
                |(mut sum, mut count), i| {
                    S::vector_add_inplace(&mut sum[self.assign[i]], &samples[(i,)]);
                    count[self.assign[i]] += 1.0;
                    (sum, count)
                },
            )
            .reduce(
                || (vec![vec![S::zero(); dims]; c], vec![f32::zero(); c]),
                |(mut sum, mut count), (sum_1, count_1)| {
                    for i in 0..c {
                        S::vector_add_inplace(&mut sum[i], &sum_1[i]);
                        count[i] += count_1[i];
                    }
                    (sum, count)
                },
            );

        let mut centroids = (0..c)
            .into_par_iter()
            .map(|i| {
                let mut centroid = S::vector_mul_scalar(&sum[i], 1.0 / count[i]);
                (self.spherical)(&mut centroid);
                centroid
            })
            .collect::<Vec<_>>();

        for i in 0..c {
            if count[i] != f32::zero() {
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
            centroids[i] = centroids[o].clone();
            S::kmeans_helper(&mut centroids[i], 1.0 + DELTA, 1.0 - DELTA);
            S::kmeans_helper(&mut centroids[o], 1.0 - DELTA, 1.0 + DELTA);
            count[i] = count[o] / 2.0;
            count[o] -= count[i];
        }

        let assign = (0..n)
            .into_par_iter()
            .map(|i| {
                let mut result = (f32::INFINITY, 0);
                for j in 0..c {
                    let dis_2 = S::reduce_sum_of_d2(&samples[(i,)], &centroids[j]);
                    if dis_2 <= result.0 {
                        result = (dis_2, j);
                    }
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
