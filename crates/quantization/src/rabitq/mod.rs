use base::{
    index::IndexOptions,
    operator::{Borrowed, Owned, Scalar},
    scalar::{ScalarLike, F32},
    search::Vectors,
};
use nalgebra::{debug::RandomOrthogonal, Dim, Dyn};
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
}

impl<O: OperatorRaBitQ> RaBitQuantizer<O> {
    pub fn train(
        options: IndexOptions,
        vectors: &impl Vectors<O>,
        transform: impl Fn(Borrowed<'_, O>) -> Owned<O> + Copy,
    ) -> Self {
        let dim_pad = (options.vector.dims + 63) / 64 * 64;
        let projection = gen_random_orthogonal::<O>(dim_pad as usize);

        Self {
            dim: options.vector.dims,
            dim_pad_64: dim_pad,
            projection,
        }
    }

    pub fn width(&self) -> usize {
        (self.dim / 64) as usize
    }

    pub fn encode(&self, vector: &[Scalar<O>]) -> Vec<u8> {
        unimplemented!()
    }

    pub fn distance(&self, lhs: Borrowed<'_, O>, rhs: &[u8]) -> F32 {
        self.rough(lhs, rhs)
    }

    pub fn rough(&self, lhs: Borrowed<'_, O>, rhs: &[u8]) -> F32 {
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
    for (i, vec) in random_matrix.column_iter().enumerate() {
        for val in vec.iter() {
            projection[i].push(ScalarLike::from_f32(*val));
        }
    }

    projection
}
