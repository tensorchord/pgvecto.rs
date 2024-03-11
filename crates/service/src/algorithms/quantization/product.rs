use crate::algorithms::clustering::elkan_k_means::ElkanKMeans;
use crate::algorithms::quantization::Quan;
use crate::algorithms::raw::Raw;
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use crate::utils::mmap_array::MmapArray;
use crate::utils::vec2::Vec2;
use rand::seq::index::sample;
use rand::thread_rng;
use rayon::iter::IndexedParallelIterator;
use rayon::iter::ParallelIterator;
use rayon::slice::ParallelSliceMut;
use std::path::Path;
use std::sync::Arc;

pub struct ProductQuantization<S: G> {
    dims: u32,
    ratio: u32,
    centroids: Vec<Scalar<S>>,
    codes: MmapArray<u8>,
    precomputed_table: Vec<F32>,
}

unsafe impl<S: G> Send for ProductQuantization<S> {}
unsafe impl<S: G> Sync for ProductQuantization<S> {}

impl<S: G> ProductQuantization<S> {
    pub fn codes(&self, i: u32) -> &[u8] {
        let width = self.dims.div_ceil(self.ratio);
        let s = i as usize * width as usize;
        let e = (i + 1) as usize * width as usize;
        &self.codes[s..e]
    }
}

impl<S: G> Quan<S> for ProductQuantization<S> {
    fn create(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Arc<Raw<S>>,
        permutation: Vec<u32>, // permutation is the mapping from placements to original ids
    ) -> Self {
        Self::with_normalizer(
            path,
            options,
            quantization_options,
            raw,
            |_, _| (),
            permutation,
        )
    }

    fn open2(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        _: &Arc<Raw<S>>,
    ) -> Self {
        let QuantizationOptions::Product(quantization_options) = quantization_options else {
            unreachable!()
        };
        let centroids =
            serde_json::from_slice(&std::fs::read(path.join("centroids")).unwrap()).unwrap();
        let codes = MmapArray::open(&path.join("codes"));
        let precomputed_table =
            serde_json::from_slice(&std::fs::read(path.join("table")).unwrap()).unwrap();
        Self {
            dims: options.vector.dims,
            ratio: quantization_options.ratio as _,
            centroids,
            codes,
            precomputed_table,
        }
    }

    fn distance(&self, lhs: Borrowed<'_, S>, rhs: u32) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        let rhs = self.codes(rhs);
        S::product_quantization_distance(dims, ratio, &self.centroids, lhs, rhs)
    }

    fn distance2(&self, lhs: u32, rhs: u32) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        let lhs = self.codes(lhs);
        let rhs = self.codes(rhs);
        S::product_quantization_distance2(dims, ratio, &self.centroids, lhs, rhs)
    }
}

impl<S: G> ProductQuantization<S> {
    pub fn with_normalizer<F>(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Raw<S>,
        normalizer: F,
        permutation: Vec<u32>,
    ) -> Self
    where
        F: Fn(u32, &mut [Scalar<S>]),
    {
        std::fs::create_dir(path).unwrap();
        let QuantizationOptions::Product(quantization_options) = quantization_options else {
            unreachable!()
        };
        let dims = options.vector.dims;
        let ratio = quantization_options.ratio as u32;
        let n = raw.len();
        let m = std::cmp::min(n, quantization_options.sample);
        let samples = {
            let f = sample(&mut thread_rng(), n as usize, m as usize).into_vec();
            let mut samples = Vec2::<Scalar<S>>::new(dims, m as usize);
            for i in 0..m {
                samples[i as usize]
                    .copy_from_slice(raw.vector(f[i as usize] as u32).to_vec().as_ref());
            }
            samples
        };
        let width = dims.div_ceil(ratio);
        let mut centroids = vec![Scalar::<S>::zero(); 256 * dims as usize];
        for i in 0..width {
            let subdims = std::cmp::min(ratio, dims - ratio * i);
            let mut subsamples = Vec2::<Scalar<S>>::new(subdims, m as usize);
            for j in 0..m {
                let src = &samples[j as usize][(i * ratio) as usize..][..subdims as usize];
                subsamples[j as usize].copy_from_slice(src);
            }
            let mut k_means = ElkanKMeans::<S::ProductQuantizationL2>::new(256, subsamples);
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
            let mut vector = raw.vector(permutation[i as usize]).to_vec();
            normalizer(permutation[i as usize], &mut vector);
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
                    let dis = S::product_quantization_l2_distance(left, right);
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
            precomputed_table: Vec::new(),
        }
    }

    pub fn encode(
        path: &Path,
        options: IndexOptions,
        quantization_options: QuantizationOptions,
        raw: &Vec2<Scalar<S>>,
    ) -> Self {
        std::fs::create_dir(path).unwrap();
        let QuantizationOptions::Product(quantization_options) = quantization_options else {
            unreachable!()
        };
        let dims = options.vector.dims;
        let ratio = quantization_options.ratio as u32;
        let n = raw.len();
        let m = std::cmp::min(n, quantization_options.sample as usize);
        let samples = {
            let f = sample(&mut thread_rng(), n, m).into_vec();
            let mut samples = Vec2::new(dims, m);
            for i in 0..m {
                samples[i].copy_from_slice(&raw[f[i]]);
            }
            samples
        };
        let width = dims.div_ceil(ratio);
        // a temp layout (width * 256 * subdims) for par_chunks_mut
        let mut tmp_centroids = vec![Scalar::<S>::zero(); 256 * dims as usize];
        // this par_for parallelizes over sub quantizers
        tmp_centroids
            .par_chunks_mut(256 * ratio as usize)
            .enumerate()
            .for_each(|(i, v)| {
                // i is the index of subquantizer
                let subdims = std::cmp::min(ratio, dims - ratio * i as u32) as usize;
                let mut subsamples = Vec2::new(subdims as u32, m);
                for j in 0..m {
                    let src = &samples[j][i * ratio as usize..][..subdims];
                    subsamples[j].copy_from_slice(src);
                }
                let mut k_means = ElkanKMeans::<S::ProductQuantizationL2>::new(256, subsamples);
                for _ in 0..25 {
                    if k_means.iterate() {
                        break;
                    }
                }
                let centroid = k_means.finish();
                for j in 0usize..=255 {
                    v[j * subdims..][..subdims].copy_from_slice(&centroid[j]);
                }
            });
        // transform back to normal layout (256 * width * subdims)
        let mut centroids = vec![Scalar::<S>::zero(); 256 * dims as usize];
        centroids
            .par_chunks_mut(dims as usize)
            .enumerate()
            .for_each(|(i, v)| {
                for j in 0..width {
                    let subdims = std::cmp::min(ratio, dims - ratio * j) as usize;
                    v[(j * ratio) as usize..][..subdims].copy_from_slice(
                        &tmp_centroids[(j * ratio) as usize * 256..][i * subdims..][..subdims],
                    );
                }
            });
        let mut codes = vec![0u8; n * width as usize];
        codes
            .par_chunks_mut(width as usize)
            .enumerate()
            .for_each(|(id, v)| {
                let vector = raw[id].to_vec();
                let width = dims.div_ceil(ratio);
                for i in 0..width {
                    let subdims = std::cmp::min(ratio, dims - ratio * i);
                    let mut minimal = F32::infinity();
                    let mut target = 0u8;
                    let left = &vector[(i * ratio) as usize..][..subdims as usize];
                    for j in 0u8..=255 {
                        let right = &centroids[j as usize * dims as usize..]
                            [(i * ratio) as usize..][..subdims as usize];
                        let dis = S::ProductQuantizationL2::product_quantization_dense_distance(
                            left, right,
                        );
                        if dis < minimal {
                            minimal = dis;
                            target = j;
                        }
                    }
                    v[i as usize] = target;
                }
            });
        sync_dir(path);
        std::fs::write(
            path.join("centroids"),
            serde_json::to_string(&centroids).unwrap(),
        )
        .unwrap();
        let codes = MmapArray::create(&path.join("codes"), codes.into_iter());
        Self {
            dims,
            ratio,
            centroids,
            codes,
            precomputed_table: Vec::new(),
        }
    }

    // compute term3 at build time
    pub fn precompute_table(&mut self, path: &Path, coarse_centroids: &Vec2<Scalar<S>>) {
        let nlist = coarse_centroids.len();
        let dims = self.dims;
        let ratio = self.ratio;
        let width = dims.div_ceil(ratio);
        self.precomputed_table
            .resize(nlist * width as usize * 256, F32::zero());
        self.precomputed_table
            .par_chunks_mut(width as usize * 256)
            .enumerate()
            .for_each(|(i, v)| {
                let x_c = &coarse_centroids[i];
                for j in 0..width {
                    let subdims = std::cmp::min(ratio, dims - ratio * j);
                    let sub_x_c = &x_c[(j * ratio) as usize..][..subdims as usize];
                    for k in 0usize..256 {
                        let sub_x_r = &self.centroids[k * dims as usize..][(j * ratio) as usize..]
                            [..subdims as usize];
                        v[j as usize * 256 + k] = squared_norm::<S>(subdims, sub_x_r)
                            + F32(2.0) * inner_product::<S>(subdims, sub_x_c, sub_x_r);
                    }
                }
            });
        std::fs::write(
            path.join("table"),
            serde_json::to_string(&self.precomputed_table).unwrap(),
        )
        .unwrap();
    }

    // compute term2 at query time
    pub fn init_query(&self, query: &[Scalar<S>]) -> Vec<F32> {
        if S::DISTANCE_KIND == DistanceKind::Cos {
            return Vec::new();
        }
        let dims = self.dims;
        let ratio = self.ratio;
        let width = dims.div_ceil(ratio);
        let mut runtime_table = vec![F32::zero(); width as usize * 256];
        for i in 0..256 {
            for j in 0..width {
                let subdims = std::cmp::min(ratio, dims - ratio * j);
                let sub_query = &query[(j * ratio) as usize..][..subdims as usize];
                let centroid = &self.centroids[i * dims as usize..][(j * ratio) as usize..]
                    [..subdims as usize];
                runtime_table[j as usize * 256 + i] =
                    F32(-1.0) * inner_product::<S>(subdims, sub_query, centroid);
            }
        }
        runtime_table
    }

    // add up all terms given codes
    pub fn distance_with_codes(
        &self,
        lhs: Borrowed<'_, S>,
        rhs: u32,
        delta: &[Scalar<S>],
        key: usize,
        coarse_dis: F32,
        runtime_table: &[F32],
    ) -> F32 {
        if S::DISTANCE_KIND == DistanceKind::Cos {
            return self.distance_with_delta(lhs, rhs, delta);
        }
        let mut result = coarse_dis;
        let codes = self.codes(rhs);
        let width = self.dims.div_ceil(self.ratio);
        let precomputed_table = &self.precomputed_table[key * width as usize * 256..];
        if S::DISTANCE_KIND == DistanceKind::L2 {
            for i in 0..width {
                result += precomputed_table[i as usize * 256 + codes[i as usize] as usize]
                    + F32(2.0) * runtime_table[i as usize * 256 + codes[i as usize] as usize];
            }
        } else if S::DISTANCE_KIND == DistanceKind::Dot {
            for i in 0..width {
                result += runtime_table[i as usize * 256 + codes[i as usize] as usize];
            }
        }
        result
    }

    pub fn distance_with_delta(&self, lhs: Borrowed<'_, S>, rhs: u32, delta: &[Scalar<S>]) -> F32 {
        let dims = self.dims;
        let ratio = self.ratio;
        let rhs = self.codes(rhs);
        S::product_quantization_distance_with_delta(dims, ratio, &self.centroids, lhs, rhs, delta)
    }
}

pub fn squared_norm<S: G>(dims: u32, vec: &[Scalar<S>]) -> F32 {
    let mut result = F32::zero();
    for i in 0..dims as usize {
        result += F32((vec[i] * vec[i]).to_f32());
    }
    result
}

pub fn inner_product<S: G>(dims: u32, lhs: &[Scalar<S>], rhs: &[Scalar<S>]) -> F32 {
    let mut result = F32::zero();
    for i in 0..dims as usize {
        result += F32((lhs[i] * rhs[i]).to_f32());
    }
    result
}
