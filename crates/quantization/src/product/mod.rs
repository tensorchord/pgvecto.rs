pub mod operator;

use self::operator::OperatorProductQuantization;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::*;
use common::sample::sample_subvector;
use common::sample::sample_subvector_transform;
use common::vec2::Vec2;
use elkan_k_means::elkan_k_means;
use num_traits::Float;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct ProductQuantizer<O: OperatorProductQuantization> {
    dims: u32,
    ratio: u32,
    centroids: Vec2<Scalar<O>>,
}

impl<O: OperatorProductQuantization> ProductQuantizer<O> {
    pub fn train(
        options: IndexOptions,
        product_quantization_options: ProductQuantizationOptions,
        vectors: &impl Vectors<O>,
    ) -> Self {
        let dims = options.vector.dims;
        let ratio = product_quantization_options.ratio as u32;
        let width = dims.div_ceil(ratio);
        let mut centroids = Vec2::new(dims, 256);
        for i in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let start = (i * ratio) as usize;
            let end = start + subdims as usize;
            let subsamples = sample_subvector(vectors, start, end);
            let centroid = elkan_k_means::<O::PQL2>(256, subsamples);
            for j in 0u8..=255 {
                centroids[j as usize][(i * ratio) as usize..][..subdims as usize]
                    .copy_from_slice(&centroid[j as usize]);
            }
        }
        Self {
            dims,
            ratio,
            centroids,
        }
    }

    pub fn train_transform(
        options: IndexOptions,
        product_quantization_options: ProductQuantizationOptions,
        vectors: &impl Vectors<O>,
        transform_subvector: impl Fn(&mut [Scalar<O>], usize, usize) -> &[Scalar<O>],
    ) -> Self {
        let dims = options.vector.dims;
        let ratio = product_quantization_options.ratio as u32;
        let width = dims.div_ceil(ratio);
        let mut centroids = Vec2::new(dims, 256);
        for i in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let start = (i * ratio) as usize;
            let end = start + subdims as usize;
            let subsamples = sample_subvector_transform(vectors, start, end, |v| {
                transform_subvector(v, start, end)
            });
            let centroid = elkan_k_means::<O::PQL2>(256, subsamples);
            for j in 0u8..=255 {
                centroids[j as usize][(i * ratio) as usize..][..subdims as usize]
                    .copy_from_slice(&centroid[j as usize]);
            }
        }
        Self {
            dims,
            ratio,
            centroids,
        }
    }

    #[inline(always)]
    pub fn width(&self) -> usize {
        self.dims as usize
    }

    #[inline(always)]
    pub fn ratio(&self) -> usize {
        self.ratio as usize
    }

    #[inline(always)]
    pub fn centroids(&self) -> &Vec2<Scalar<O>> {
        &self.centroids
    }

    pub fn encode(&self, vector: &[Scalar<O>]) -> Vec<u8> {
        let dims = self.dims;
        let ratio = self.ratio;
        let width = dims.div_ceil(ratio);
        let mut result = Vec::with_capacity(width as usize);
        for i in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let mut minimal = F32::infinity();
            let mut target = 0u8;
            let left = &vector[(i * ratio) as usize..][..subdims as usize];
            for j in 0u8..=255 {
                let right = &self.centroids[j as usize][(i * ratio) as usize..][..subdims as usize];
                let dis = O::dense_l2_distance(left, right);
                if dis < minimal {
                    minimal = dis;
                    target = j;
                }
            }
            result.push(target);
        }
        result
    }

    pub fn distance(&self, lhs: Borrowed<'_, O>, rhs: &[u8]) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        O::product_quantization_distance(dims, ratio, &self.centroids, lhs, rhs)
    }

    pub fn distance_with_delta(
        &self,
        lhs: Borrowed<'_, O>,
        rhs: &[u8],
        delta: &[Scalar<O>],
    ) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        O::product_quantization_distance_with_delta(dims, ratio, &self.centroids, lhs, rhs, delta)
    }
}
