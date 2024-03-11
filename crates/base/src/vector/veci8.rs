use super::{VectorBorrowed, VectorOwned};
use crate::scalar::{F32, I8};
use num_traits::Float;
use serde::{Deserialize, Serialize};

#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn i8_quantization(vector: &[F32]) -> (Vec<I8>, F32, F32) {
    let min = vector.iter().copied().fold(F32::infinity(), Float::min);
    let max = vector.iter().copied().fold(F32::neg_infinity(), Float::max);
    let alpha = (max - min) / 254.0;
    let offset = (max + min) / 2.0;
    let result = vector
        .iter()
        .map(|&x| ((x - offset) / alpha).into())
        .collect();
    (result, alpha, offset)
}

#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn i8_dequantization(vector: &[I8], alpha: F32, offset: F32) -> Vec<F32> {
    vector
        .iter()
        .map(|&x| (x.to_f32() * alpha + offset))
        .collect()
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn i8_precompute(data: &[I8], alpha: F32, offset: F32) -> (F32, F32) {
    let sum = data.iter().map(|&x| x.to_f32() * alpha).sum();
    let l2_norm = data
        .iter()
        .map(|&x| (x.to_f32() * alpha + offset) * (x.to_f32() * alpha + offset))
        .sum::<F32>()
        .sqrt();
    (sum, l2_norm)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Veci8Owned {
    dims: u32,
    data: Vec<I8>,
    alpha: F32,
    offset: F32,
    // sum of a_i * alpha, precomputed for dot
    sum: F32,
    // l2 norm of original f_i, precomputed for l2
    l2_norm: F32,
}

impl Veci8Owned {
    #[inline(always)]
    pub fn new(dims: u32, data: Vec<I8>, alpha: F32, offset: F32) -> Self {
        Self::new_checked(dims, data, alpha, offset).unwrap()
    }

    #[inline(always)]
    pub fn new_checked(dims: u32, data: Vec<I8>, alpha: F32, offset: F32) -> Option<Self> {
        if dims == 0 || dims > 65535 {
            return None;
        }
        let (sum, l2_norm) = i8_precompute(&data, alpha, offset);
        Some(unsafe { Self::new_unchecked(dims, data, alpha, offset, sum, l2_norm) })
    }

    /// # Safety
    ///
    /// * `dims` must be in `1..=65535`.
    /// * `dims` must be equal to `values.len()`.
    #[inline(always)]
    pub unsafe fn new_unchecked(
        dims: u32,
        data: Vec<I8>,
        alpha: F32,
        offset: F32,
        sum: F32,
        l2_norm: F32,
    ) -> Self {
        Veci8Owned {
            dims,
            data,
            alpha,
            offset,
            sum,
            l2_norm,
        }
    }

    pub fn data(&self) -> &[I8] {
        &self.data
    }

    pub fn alpha(&self) -> F32 {
        self.alpha
    }

    pub fn alpha_mut(&mut self) -> &mut F32 {
        &mut self.alpha
    }

    pub fn offset_mut(&mut self) -> &mut F32 {
        &mut self.offset
    }

    pub fn offset(&self) -> F32 {
        self.offset
    }

    pub fn sum(&self) -> F32 {
        self.sum
    }

    pub fn l2_norm(&self) -> F32 {
        self.l2_norm
    }

    pub fn dims(&self) -> u32 {
        self.dims
    }
}

impl VectorOwned for Veci8Owned {
    // For i8 quantization, the scalar type is used for type in kmeans, it use F32 to store the centroids and examples in the kmeans.
    type Scalar = F32;
    type Borrowed<'a> = Veci8Borrowed<'a>;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    fn for_borrow(&self) -> Veci8Borrowed<'_> {
        Veci8Borrowed {
            dims: self.dims,
            data: &self.data,
            alpha: self.alpha,
            offset: self.offset,
            sum: self.sum,
            l2_norm: self.l2_norm,
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        i8_dequantization(&self.data, self.alpha, self.offset)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Veci8Borrowed<'a> {
    dims: u32,
    data: &'a [I8],
    alpha: F32,
    offset: F32,
    // sum of a_i * alpha, precomputed for dot
    sum: F32,
    // l2 norm of original f_i, precomputed for l2
    l2_norm: F32,
}

impl<'a> Veci8Borrowed<'a> {
    #[inline(always)]
    pub fn new(
        dims: u32,
        data: &'a [I8],
        alpha: F32,
        offset: F32,
        sum: F32,
        l2_norm: F32,
    ) -> Veci8Borrowed<'a> {
        Self::new_checked(dims, data, alpha, offset, sum, l2_norm).unwrap()
    }

    #[inline(always)]
    pub fn new_checked(
        dims: u32,
        data: &'a [I8],
        alpha: F32,
        offset: F32,
        sum: F32,
        l2_norm: F32,
    ) -> Option<Self> {
        if dims == 0 || dims > 65535 {
            return None;
        }
        // TODO: should we check the precomputed result?
        // let (sum_calc, l2_norm_calc) = i8_precompute(data, alpha, offset);
        // if sum != sum_calc || l2_norm != l2_norm_calc {
        //     return None;
        // }
        Some(unsafe { Self::new_unchecked(dims, data, alpha, offset, sum, l2_norm) })
    }

    /// # Safety
    ///
    /// * `dims` must be in `1..=65535`.
    /// * `dims` must be equal to `values.len()`.
    /// * precomputed result must be correct
    #[inline(always)]
    pub unsafe fn new_unchecked(
        dims: u32,
        data: &'a [I8],
        alpha: F32,
        offset: F32,
        sum: F32,
        l2_norm: F32,
    ) -> Self {
        Veci8Borrowed {
            dims,
            data,
            alpha,
            offset,
            sum,
            l2_norm,
        }
    }

    pub fn to_owned(&self) -> Veci8Owned {
        Veci8Owned {
            dims: self.dims,
            data: self.data.to_vec(),
            alpha: self.alpha,
            offset: self.offset,
            sum: self.sum,
            l2_norm: self.l2_norm,
        }
    }

    pub fn data(&self) -> &[I8] {
        self.data
    }

    pub fn alpha(&self) -> F32 {
        self.alpha
    }

    pub fn offset(&self) -> F32 {
        self.offset
    }

    pub fn sum(&self) -> F32 {
        self.sum
    }

    pub fn l2_norm(&self) -> F32 {
        self.l2_norm
    }

    pub fn dims(&self) -> u32 {
        self.dims
    }

    pub fn normalize(&self) -> Veci8Owned {
        let l = self.l2_norm();
        let alpha = self.alpha() / l;
        let offset = self.offset() / l;
        let sum = self.sum() / l;
        let l2_norm = F32(1.0);
        unsafe {
            Veci8Owned::new_unchecked(
                self.dims(),
                self.data().to_vec(),
                alpha,
                offset,
                sum,
                l2_norm,
            )
        }
    }
}

impl VectorBorrowed for Veci8Borrowed<'_> {
    // For i8 quantization, the scalar type is used for type in kmeans, it use F32 to store the centroids and examples in the kmeans.
    type Scalar = F32;
    type Owned = Veci8Owned;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    fn for_own(&self) -> Veci8Owned {
        Veci8Owned {
            dims: self.dims,
            data: self.data.to_vec(),
            alpha: self.alpha,
            offset: self.offset,
            sum: self.sum,
            l2_norm: self.l2_norm,
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        i8_dequantization(self.data, self.alpha, self.offset)
    }
}

impl From<Veci8Borrowed<'_>> for Veci8Owned {
    fn from(value: Veci8Borrowed<'_>) -> Self {
        Self {
            dims: value.dims,
            data: value.data.to_vec(),
            alpha: value.alpha,
            offset: value.offset,
            sum: value.sum,
            l2_norm: value.l2_norm,
        }
    }
}

impl<'a> From<&'a Veci8Owned> for Veci8Borrowed<'a> {
    fn from(value: &'a Veci8Owned) -> Self {
        Self {
            dims: value.dims,
            data: &value.data,
            alpha: value.alpha,
            offset: value.offset,
            sum: value.sum,
            l2_norm: value.l2_norm,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quantization_roundtrip() {
        let vector = vec![F32(0.0), F32(1.0), F32(2.0), F32(3.0), F32(4.0)];
        let (result, alpha, offset) = i8_quantization(&vector);
        assert_eq!(result, vec![I8(-127), I8(-63), I8(0), I8(63), I8(127)]);
        assert_eq!(alpha, F32(4.0 / 254.0));
        assert_eq!(offset, F32(2.0));
        let vector = i8_dequantization(result.as_slice(), alpha, offset);
        for (i, x) in vector.iter().enumerate() {
            assert!((x.0 - (i as f32)).abs() < 0.05);
        }
    }
}
