pub mod operator;

use self::operator::OperatorProductQuantization;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::*;
use base::vector::*;
use common::dir_ops::sync_dir;
use common::mmap_array::MmapArray;
use common::vec2::Vec2;
use elkan_k_means::ElkanKMeans;
use num_traits::{Float, Zero};
use rand::seq::index::sample;
use rand::thread_rng;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

pub struct ProductQuantization<O: OperatorProductQuantization, C: Collection<O>> {
    dims: u32,
    ratio: u32,
    centroids: Vec<Scalar<O>>,
    codes: MmapArray<u8>,
    _maker: PhantomData<fn(C) -> C>,
}

unsafe impl<O: OperatorProductQuantization, C: Collection<O>> Send for ProductQuantization<O, C> {}
unsafe impl<O: OperatorProductQuantization, C: Collection<O>> Sync for ProductQuantization<O, C> {}

impl<O: OperatorProductQuantization, C: Collection<O>> ProductQuantization<O, C> {
    fn codes(&self, i: u32) -> &[u8] {
        let width = self.dims.div_ceil(self.ratio);
        let s = i as usize * width as usize;
        let e = (i + 1) as usize * width as usize;
        &self.codes[s..e]
    }
}

impl<O: OperatorProductQuantization, C: Collection<O>> ProductQuantization<O, C> {
    pub fn create(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        collection: &Arc<C>,
        permutation: Vec<u32>, // permutation is the mapping from placements to original ids
    ) -> Self {
        std::fs::create_dir(path).unwrap();
        let QuantizationOptions::Product(quantization_options) = quantization_options else {
            unreachable!()
        };
        let dims = options.vector.dims;
        let ratio = quantization_options.ratio as u32;
        let n = collection.len();
        let m = std::cmp::min(n, quantization_options.sample);
        let samples = {
            let f = sample(&mut thread_rng(), n as usize, m as usize).into_vec();
            let mut samples = Vec2::<Scalar<O>>::new(dims, m as usize);
            for i in 0..m {
                samples[i as usize]
                    .copy_from_slice(collection.vector(f[i as usize] as u32).to_vec().as_ref());
            }
            samples
        };
        let width = dims.div_ceil(ratio);
        let mut centroids = vec![Scalar::<O>::zero(); 256 * dims as usize];
        for i in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let mut subsamples = Vec2::<Scalar<O>>::new(subdims, m as usize);
            for j in 0..m {
                let src = &samples[j as usize][(i * ratio) as usize..][..subdims as usize];
                subsamples[j as usize].copy_from_slice(src);
            }
            let mut k_means = ElkanKMeans::<O::ProductQuantizationL2>::new(256, subsamples);
            for _ in 0..25 {
                if k_means.iterate() {
                    break;
                }
            }
            let centroid = k_means.finish();
            for j in 0u8..=255 {
                centroids[j as usize * dims as usize..][(i * ratio) as usize..][..subdims as usize]
                    .copy_from_slice(&centroid[j as usize]);
            }
        }
        let codes_iter = (0..n).flat_map(|i| {
            let vector = collection.vector(permutation[i as usize]).to_vec();
            let width = dims.div_ceil(ratio);
            let mut result = Vec::with_capacity(width as usize);
            for i in 0..width {
                let subdims = std::cmp::min(ratio, dims - ratio * i);
                let mut minimal = F32::infinity();
                let mut target = 0u8;
                let left = &vector[(i * ratio) as usize..][..subdims as usize];
                for j in 0u8..=255 {
                    let right = &centroids[j as usize * dims as usize..][(i * ratio) as usize..]
                        [..subdims as usize];
                    let dis = O::product_quantization_l2_distance(left, right);
                    if dis < minimal {
                        minimal = dis;
                        target = j;
                    }
                }
                result.push(target);
            }
            result.into_iter()
        });
        sync_dir(path);
        std::fs::write(
            path.join("centroids"),
            serde_json::to_string(&centroids).unwrap(),
        )
        .unwrap();
        let codes = MmapArray::create(&path.join("codes"), codes_iter);
        Self {
            dims,
            ratio,
            centroids,
            codes,
            _maker: PhantomData,
        }
    }

    pub fn open(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        _: &Arc<C>,
    ) -> Self {
        let QuantizationOptions::Product(quantization_options) = quantization_options else {
            unreachable!()
        };
        let centroids =
            serde_json::from_slice(&std::fs::read(path.join("centroids")).unwrap()).unwrap();
        let codes = MmapArray::open(&path.join("codes"));
        Self {
            dims: options.vector.dims,
            ratio: quantization_options.ratio as _,
            centroids,
            codes,
            _maker: PhantomData,
        }
    }

    pub fn distance(&self, lhs: Borrowed<'_, O>, rhs: u32) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        let rhs = self.codes(rhs);
        O::product_quantization_distance(dims, ratio, &self.centroids, lhs, rhs)
    }

    pub fn distance2(&self, lhs: u32, rhs: u32) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        let lhs = self.codes(lhs);
        let rhs = self.codes(rhs);
        O::product_quantization_distance2(dims, ratio, &self.centroids, lhs, rhs)
    }
}
