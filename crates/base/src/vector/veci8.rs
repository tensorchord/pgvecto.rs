use super::{VectorBorrowed, VectorKind, VectorOwned};
use crate::scalar::{F32, I8};
use num_traits::Float;
use serde::{Deserialize, Serialize};

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

    const VECTOR_KIND: VectorKind = VectorKind::Veci8;

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

#[detect::multiversion(v4, v3, v2, neon, fallback)]
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

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn i8_dequantization(vector: &[I8], alpha: F32, offset: F32) -> Vec<F32> {
    vector
        .iter()
        .map(|&x| (x.to_f32() * alpha + offset))
        .collect()
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn i8_precompute(data: &[I8], alpha: F32, offset: F32) -> (F32, F32) {
    let sum = data.iter().map(|&x| x.to_f32() * alpha).sum();
    let l2_norm = data
        .iter()
        .map(|&x| (x.to_f32() * alpha + offset) * (x.to_f32() * alpha + offset))
        .sum::<F32>()
        .sqrt();
    (sum, l2_norm)
}

#[inline]
#[cfg(any(target_arch = "x86_64", doc))]
#[doc(cfg(target_arch = "x86_64"))]
#[detect::target_cpu(enable = "v4_avx512vnni")]
unsafe fn dot_internal_v4_avx512vnni(x: &[I8], y: &[I8]) -> F32 {
    use std::arch::x86_64::*;
    assert_eq!(x.len(), y.len());
    let mut sum = 0;
    let mut i = x.len();
    let mut p_x = x.as_ptr() as *const i8;
    let mut p_y = y.as_ptr() as *const i8;
    let mut vec_x;
    let mut vec_y;
    unsafe {
        let mut result = _mm512_setzero_si512();
        let zero = _mm512_setzero_si512();
        while i > 0 {
            if i < 64 {
                let mask = _bzhi_u64(0xFFFF_FFFF_FFFF_FFFF, i as u32);
                vec_x = _mm512_maskz_loadu_epi8(mask, p_x);
                vec_y = _mm512_maskz_loadu_epi8(mask, p_y);
                i = 0;
            } else {
                vec_x = _mm512_loadu_epi8(p_x);
                vec_y = _mm512_loadu_epi8(p_y);
                i -= 64;
                p_x = p_x.add(64);
                p_y = p_y.add(64);
            }
            // There are only _mm512_dpbusd_epi32 support, dpbusd will zeroextend a[i] and signextend b[i] first, so we need to convert a[i] positive and change corresponding b[i] to get right result.
            // And because we use -b[i] here, the range of quantization should be [-127, 127] instead of [-128, 127] to avoid overflow.
            let neg_mask = _mm512_movepi8_mask(vec_x);
            vec_x = _mm512_mask_abs_epi8(vec_x, neg_mask, vec_x);
            // Get -b[i] here, use saturating sub to avoid overflow. There are some precision loss here.
            vec_y = _mm512_mask_subs_epi8(vec_y, neg_mask, zero, vec_y);
            result = _mm512_dpbusd_epi32(result, vec_x, vec_y);
        }
        sum += _mm512_reduce_add_epi32(result);
    }
    F32(sum as f32)
}

#[cfg(all(target_arch = "x86_64", test))]
#[test]
fn dot_internal_v4_avx512vnni_test() {
    // A large epsilon is set for loss of precision caused by saturation arithmetic
    const EPSILON: F32 = F32(512.0);
    detect::init();
    if !detect::v4_avx512vnni::detect() {
        println!("test {} ... skipped (v4_avx512vnni)", module_path!());
        return;
    }
    for _ in 0..300 {
        let lhs = std::array::from_fn::<_, 400, _>(|_| I8(rand::random()));
        let rhs = std::array::from_fn::<_, 400, _>(|_| I8(rand::random()));
        let specialized = unsafe { dot_internal_v4_avx512vnni(&lhs, &rhs) };
        let fallback = unsafe { dot_internal_fallback(&lhs, &rhs) };
        assert!(
            (specialized - fallback).abs() < EPSILON,
            "specialized = {specialized}, fallback = {fallback}."
        );
    }
}

#[detect::multiversion(v4_avx512vnni = import, v4, v3, v2, neon, fallback = export)]
fn dot_internal(x: &[I8], y: &[I8]) -> F32 {
    // i8 * i8 fall in range of i16. Since our length is less than (2^16 - 1), the result won't overflow.
    let mut sum = 0;
    assert_eq!(x.len(), y.len());
    let length = x.len();
    // according to https://godbolt.org/z/ff48vW4es, this loop will be autovectorized
    for i in 0..length {
        sum += (x[i].0 as i16 * y[i].0 as i16) as i32;
    }
    F32(sum as f32)
}

pub fn dot(x: &Veci8Borrowed<'_>, y: &Veci8Borrowed<'_>) -> F32 {
    // (alpha_x * x[i] + offset_x) * (alpha_y * y[i] + offset_y)
    // = alpha_x * alpha_y * x[i] * y[i] + alpha_x * offset_y * x[i] + alpha_y * offset_x * y[i] + offset_x * offset_y
    // Sum(dot(origin_x[i] , origin_y[i])) = alpha_x * alpha_y * Sum(dot(x[i], y[i])) + offset_y * Sum(alpha_x * x[i]) + offset_x * Sum(alpha_y * y[i]) + offset_x * offset_y * dims
    let dot_xy = dot_internal(x.data(), y.data());
    x.alpha() * y.alpha() * dot_xy
        + x.offset() * y.sum()
        + y.offset() * x.sum()
        + x.offset() * y.offset() * F32(x.dims() as f32)
}

pub fn sl2(x: &Veci8Borrowed<'_>, y: &Veci8Borrowed<'_>) -> F32 {
    // Sum(l2(origin_x[i] - origin_y[i])) = sum(x[i] ^ 2 - 2 * x[i] * y[i] + y[i] ^ 2)
    // = dot(x, x) - 2 * dot(x, y) + dot(y, y)
    x.l2_norm() * x.l2_norm() - F32(2.0) * dot(x, y) + y.l2_norm() * y.l2_norm()
}

pub fn cosine(x: &Veci8Borrowed<'_>, y: &Veci8Borrowed<'_>) -> F32 {
    // dot(x, y) / (l2(x) * l2(y))
    let dot_xy = dot(x, y);
    let l2_x = x.l2_norm();
    let l2_y = y.l2_norm();
    dot_xy / (l2_x * l2_y)
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn l2_2<'a>(lhs: Veci8Borrowed<'a>, rhs: &[F32]) -> F32 {
    let data = lhs.data();
    assert_eq!(data.len(), rhs.len());
    data.iter()
        .zip(rhs.iter())
        .map(|(&x, &y)| {
            (x.to_f32() * lhs.alpha() + lhs.offset() - y)
                * (x.to_f32() * lhs.alpha() + lhs.offset() - y)
        })
        .sum::<F32>()
}

#[detect::multiversion(v4, v3, v2, neon, fallback)]
pub fn dot_2<'a>(lhs: Veci8Borrowed<'a>, rhs: &[F32]) -> F32 {
    let data = lhs.data();
    assert_eq!(data.len(), rhs.len());
    data.iter()
        .zip(rhs.iter())
        .map(|(&x, &y)| (x.to_f32() * lhs.alpha() + lhs.offset()) * y)
        .sum::<F32>()
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

    fn new_random_vec_f32(size: usize) -> Vec<F32> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        (0..size)
            .map(|_| F32(rng.gen_range(-100000.0..100000.0)))
            .collect()
    }

    fn vec_to_owned(vec: Vec<F32>) -> Veci8Owned {
        let (v, alpha, offset) = i8_quantization(&vec);
        Veci8Owned::new(v.len() as u32, v, alpha, offset)
    }

    #[test]
    fn test_dot_i8() {
        let x = vec![F32(1.0), F32(2.0), F32(3.0)];
        let y = vec![F32(3.0), F32(2.0), F32(1.0)];
        let x_owned = vec_to_owned(x);
        let ref_x = x_owned.for_borrow();
        let y_owned = vec_to_owned(y);
        let ref_y = y_owned.for_borrow();
        let result = dot(&ref_x, &ref_y);
        assert!((result.0 - 10.0).abs() < 0.1);
    }

    #[test]
    fn test_cos_i8() {
        let x = vec![F32(1.0), F32(2.0), F32(3.0)];
        let y = vec![F32(3.0), F32(2.0), F32(1.0)];
        let x_owned = vec_to_owned(x);
        let ref_x = x_owned.for_borrow();
        let y_owned = vec_to_owned(y);
        let ref_y = y_owned.for_borrow();
        let result = cosine(&ref_x, &ref_y);
        assert!((result.0 - (10.0 / 14.0)).abs() < 0.1);
        // test cos_i8 using random generated data, check the precision
        let x = new_random_vec_f32(1000);
        let y = new_random_vec_f32(1000);
        let xy = x.iter().zip(y.iter()).map(|(&x, &y)| x * y).sum::<F32>().0;
        let l2_x = x.iter().map(|&x| x * x).sum::<F32>().0.sqrt();
        let l2_y = y.iter().map(|&y| y * y).sum::<F32>().0.sqrt();
        let result_expected = xy / (l2_x * l2_y);
        let x_owned = vec_to_owned(x);
        let ref_x = x_owned.for_borrow();
        let y_owned = vec_to_owned(y);
        let ref_y = y_owned.for_borrow();
        let result = cosine(&ref_x, &ref_y);
        assert!(
            result_expected < 0.01
                || (result.0 - result_expected).abs() < 0.01
                || (result.0 - result_expected).abs() / result_expected < 0.1
        );
    }

    #[test]
    fn test_l2_i8() {
        let x = vec![F32(1.0), F32(2.0), F32(3.0)];
        let y = vec![F32(3.0), F32(2.0), F32(1.0)];
        let x_owned = vec_to_owned(x);
        let ref_x = x_owned.for_borrow();
        let y_owned = vec_to_owned(y);
        let ref_y = y_owned.for_borrow();
        let result = sl2(&ref_x, &ref_y);
        assert!((result.0 - 8.0).abs() < 0.1);
        // test l2_i8 using random generated data, check the precision
        let x = new_random_vec_f32(1000);
        let y = new_random_vec_f32(1000);
        let result_expected = x
            .iter()
            .zip(y.iter())
            .map(|(&x, &y)| (x - y) * (x - y))
            .sum::<F32>()
            .0;
        let x_owned = vec_to_owned(x);
        let ref_x = x_owned.for_borrow();
        let y_owned = vec_to_owned(y);
        let ref_y = y_owned.for_borrow();
        let result = sl2(&ref_x, &ref_y);
        assert!(
            result_expected < 1.0 || (result.0 - result_expected).abs() / result_expected < 0.05
        );
    }
}
