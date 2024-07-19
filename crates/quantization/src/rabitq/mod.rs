use std::ops::Div;

use base::{
    index::{RaBitQuantizationOptions, SearchOptions, VectorOptions},
    operator::{Borrowed, Owned, Scalar},
    scalar::{ScalarLike, F32},
    search::{Reranker, Vectors},
    vector::{VectorBorrowed, VectorOwned},
};
use nalgebra::{debug::RandomOrthogonal, Dim, Dyn};
use num_traits::{Float, Zero};
use operator::OperatorRaBitQ;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

pub mod operator;

const EPSILON: F32 = F32(1.9);
const THETA_LOG_DIM: u32 = 4;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct RaBitQuantizer<O: OperatorRaBitQ> {
    dim: u32,
    dim_pad_64: u32,
    projection: Vec<Vec<Scalar<O>>>,
    binary_x: Vec<u64>,
    distance_to_centroid: Vec<Scalar<O>>,
    dot_product_x: Vec<Scalar<O>>,
}

impl<O: OperatorRaBitQ> RaBitQuantizer<O> {
    pub fn train(
        vector_options: VectorOptions,
        options: RaBitQuantizationOptions,
        vectors: &impl Vectors<O>,
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy,
    ) -> Self {
        let dim_pad = (vector_options.dims + 63) / 64 * 64;
        let projection = gen_random_orthogonal::<O>(dim_pad as usize);
        let n = vectors.len();
        let mut distance_to_centroid = vec![Scalar::<O>::zero(); n as usize];
        let mut quantized_x = vec![vec![Scalar::<O>::zero(); dim_pad as usize]; n as usize];
        for i in 0..n {
            let vector = transform(vectors.vector(i)).as_borrowed().to_vec();
            distance_to_centroid[i as usize] = O::vector_dot_product(&vector, &vector).sqrt();
            for j in 0..vector_options.dims as usize {
                quantized_x[i as usize][j] = O::vector_dot_product(&projection[j], &vector);
            }
        }
        let mut binary_vec_x = Vec::with_capacity(n as usize);
        let mut signed_x = Vec::with_capacity(n as usize);
        for i in 0..(n as usize) {
            binary_vec_x.push(O::vector_binarize_u64(&quantized_x[i]));
            signed_x.push(O::vector_binarize_one(&quantized_x[i]));
        }
        let binary_x: Vec<u64> = binary_vec_x.into_iter().flatten().collect();
        let mut dot_product_x = vec![Scalar::<O>::zero(); n as usize];
        for i in 0..(n as usize) {
            let norm = O::vector_dot_product(&quantized_x[i], &quantized_x[i]).sqrt()
                * Scalar::<O>::from_f32(dim_pad as f32).sqrt();
            dot_product_x[i as usize] =
                O::vector_dot_product(&quantized_x[i], &signed_x[i]).div(norm);
        }

        Self {
            dim: vector_options.dims,
            dim_pad_64: dim_pad,
            projection,
            binary_x,
            distance_to_centroid,
            dot_product_x,
        }
    }

    pub fn width(&self) -> usize {
        (self.dim / 64) as usize
    }

    pub fn encode(&self, vector: &[Scalar<O>]) -> Vec<u8> {
        unimplemented!()
    }

    pub fn preprocess(&self, lhs: Borrowed<'_, O>) -> O::RabitQuantizationPreprocessed {
        unimplemented!()
    }

    pub fn process(&self, preprocessed: &O::RabitQuantizationPreprocessed, rhs: &[u8]) -> F32 {
        unimplemented!()
    }

    pub fn flat_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        unimplemented!()
    }

    pub fn ivf_naive_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        unimplemented!()
    }

    pub fn ivf_residual_rerank<'a, T: 'a>(
        &'a self,
        vectors: Vec<Owned<O>>,
        opts: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T, usize> + 'a> {
        unimplemented!()
    }

    pub fn graph_rerank<'a, T: 'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        opts: &'a SearchOptions,
        c: impl Fn(u32) -> &'a [u8] + 'a,
        r: impl Fn(u32) -> (F32, T) + 'a,
    ) -> Box<dyn Reranker<T> + 'a> {
        unimplemented!()
    }
}

fn gen_random_orthogonal<O: OperatorRaBitQ>(dim: usize) -> Vec<Vec<Scalar<O>>> {
    let mut rng = thread_rng();
    let random_orth: RandomOrthogonal<f32, Dyn> =
        RandomOrthogonal::new(Dim::from_usize(dim), || rng.gen());
    let random_matrix = random_orth.unwrap();
    let mut projection = vec![Vec::with_capacity(dim); dim];
    // use the transpose of the random matrix as the inverse of the projection matrix
    // TODO: inverse or not?
    for (i, vec) in random_matrix.column_iter().enumerate() {
        for val in vec.iter() {
            projection[i].push(ScalarLike::from_f32(*val));
        }
    }

    projection
}
