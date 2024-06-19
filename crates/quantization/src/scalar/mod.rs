pub mod operator;

use self::operator::OperatorScalarQuantization;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::Vectors;
use base::vector::*;
use num_traits::Float;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ScalarQuantizer<O: OperatorScalarQuantization> {
    dims: u32,
    max: Vec<Scalar<O>>,
    min: Vec<Scalar<O>>,
}

impl<O: OperatorScalarQuantization> ScalarQuantizer<O> {
    pub fn train(options: IndexOptions, vectors: &impl Vectors<O>) -> Self {
        let dims = options.vector.dims;
        let mut max = vec![Scalar::<O>::neg_infinity(); dims as usize];
        let mut min = vec![Scalar::<O>::infinity(); dims as usize];
        let n = vectors.len();
        for i in 0..n {
            let vector = vectors.vector(i).to_vec();
            for j in 0..dims as usize {
                max[j] = std::cmp::max(max[j], vector[j]);
                min[j] = std::cmp::min(min[j], vector[j]);
            }
        }
        Self { dims, max, min }
    }

    pub fn width(&self) -> usize {
        self.dims as usize
    }

    pub fn encode(&self, vector: &[Scalar<O>]) -> Vec<u8> {
        let dims = self.dims;
        let mut result = vec![0u8; dims as usize];
        for i in 0..dims as usize {
            let w =
                (((vector[i] - self.min[i]) / (self.max[i] - self.min[i])).to_f32() * 256.0) as u32;
            result[i] = w.clamp(0, 255) as u8;
        }
        result
    }

    pub fn distance(&self, lhs: Borrowed<'_, O>, rhs: &[u8]) -> F32 {
        O::scalar_quantization_distance(self.dims as _, &self.max, &self.min, lhs, rhs)
    }
}
