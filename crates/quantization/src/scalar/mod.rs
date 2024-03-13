pub mod operator;

use self::operator::OperatorScalarQuantization;
use base::index::*;
use base::operator::*;
use base::scalar::*;
use base::search::Collection;
use base::vector::*;
use common::dir_ops::sync_dir;
use common::mmap_array::MmapArray;
use num_traits::Float;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

pub struct ScalarQuantization<O: OperatorScalarQuantization, C: Collection<O>> {
    dims: u16,
    max: Vec<Scalar<O>>,
    min: Vec<Scalar<O>>,
    codes: MmapArray<u8>,
    _maker: PhantomData<fn(C) -> C>,
}

unsafe impl<O: OperatorScalarQuantization, C: Collection<O>> Send for ScalarQuantization<O, C> {}
unsafe impl<O: OperatorScalarQuantization, C: Collection<O>> Sync for ScalarQuantization<O, C> {}

impl<O: OperatorScalarQuantization, C: Collection<O>> ScalarQuantization<O, C> {
    fn codes(&self, i: u32) -> &[u8] {
        let s = i as usize * self.dims as usize;
        let e = (i + 1) as usize * self.dims as usize;
        &self.codes[s..e]
    }
}

impl<O: OperatorScalarQuantization, C: Collection<O>> ScalarQuantization<O, C> {
    pub fn create(
        path: &Path,
        options: IndexOptions,
        _: QuantizationOptions,
        collection: &Arc<C>,
        permutation: Vec<u32>, // permutation is the mapping from placements to original ids
    ) -> Self {
        std::fs::create_dir(path).unwrap();
        let dims: u16 = options.vector.dims.try_into().unwrap();
        let mut max = vec![Scalar::<O>::neg_infinity(); dims as usize];
        let mut min = vec![Scalar::<O>::infinity(); dims as usize];
        let n = collection.len();
        for i in 0..n {
            let vector = collection.vector(permutation[i as usize]).to_vec();
            for j in 0..dims as usize {
                max[j] = std::cmp::max(max[j], vector[j]);
                min[j] = std::cmp::min(min[j], vector[j]);
            }
        }
        std::fs::write(path.join("max"), serde_json::to_string(&max).unwrap()).unwrap();
        std::fs::write(path.join("min"), serde_json::to_string(&min).unwrap()).unwrap();
        let codes_iter = (0..n).flat_map(|i| {
            let vector = collection.vector(permutation[i as usize]).to_vec();
            let mut result = vec![0u8; dims as usize];
            for i in 0..dims as usize {
                let w = (((vector[i] - min[i]) / (max[i] - min[i])).to_f32() * 256.0) as u32;
                result[i] = w.clamp(0, 255) as u8;
            }
            result.into_iter()
        });
        let codes = MmapArray::create(&path.join("codes"), codes_iter);
        sync_dir(path);
        Self {
            dims,
            max,
            min,
            codes,
            _maker: PhantomData,
        }
    }

    pub fn open(path: &Path, options: IndexOptions, _: QuantizationOptions, _: &Arc<C>) -> Self {
        let dims: u16 = options.vector.dims.try_into().unwrap();
        let max = serde_json::from_slice(&std::fs::read("max").unwrap()).unwrap();
        let min = serde_json::from_slice(&std::fs::read("min").unwrap()).unwrap();
        let codes = MmapArray::open(&path.join("codes"));
        Self {
            dims,
            max,
            min,
            codes,
            _maker: PhantomData,
        }
    }

    pub fn distance(&self, lhs: Borrowed<'_, O>, rhs: u32) -> F32 {
        let dims = self.dims;
        let rhs = self.codes(rhs);
        O::scalar_quantization_distance(dims, &self.max, &self.min, lhs, rhs)
    }

    pub fn distance2(&self, lhs: u32, rhs: u32) -> F32 {
        let dims = self.dims;
        let lhs = self.codes(lhs);
        let rhs = self.codes(rhs);
        O::scalar_quantization_distance2(dims, &self.max, &self.min, lhs, rhs)
    }
}
