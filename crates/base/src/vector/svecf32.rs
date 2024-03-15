use crate::scalar::F32;
use crate::vector::{VectorBorrowed, VectorKind, VectorOwned};
use num_traits::{Float, Zero};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SVecf32Owned {
    dims: u32,
    indexes: Vec<u32>,
    values: Vec<F32>,
}

impl SVecf32Owned {
    #[inline(always)]
    pub fn new(dims: u32, indexes: Vec<u32>, values: Vec<F32>) -> Self {
        Self::new_checked(dims, indexes, values).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(dims: u32, indexes: Vec<u32>, values: Vec<F32>) -> Option<Self> {
        if !(1..=1_048_575).contains(&dims) {
            return None;
        }
        if indexes.len() != values.len() {
            return None;
        }
        let len = indexes.len();
        for i in 1..len {
            if !(indexes[i - 1] < indexes[i]) {
                return None;
            }
        }
        if len != 0 && !(indexes[len - 1] < dims) {
            return None;
        }
        for i in 0..len {
            if values[i].is_zero() {
                return None;
            }
        }
        unsafe { Some(Self::new_unchecked(dims, indexes, values)) }
    }
    /// # Safety
    ///
    /// * `dims` must be in `1..=1_048_575`.
    /// * `indexes.len()` must be equal to `values.len()`.
    /// * `indexes` must be a strictly increasing sequence and the last in the sequence must be less than `dims`.
    /// * A floating number in `values` must not be positive zero or negative zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(dims: u32, indexes: Vec<u32>, values: Vec<F32>) -> Self {
        Self {
            dims,
            indexes,
            values,
        }
    }
    #[inline(always)]
    pub fn indexes(&self) -> &[u32] {
        &self.indexes
    }
    #[inline(always)]
    pub fn values(&self) -> &[F32] {
        &self.values
    }
}

impl VectorOwned for SVecf32Owned {
    type Scalar = F32;
    type Borrowed<'a> = SVecf32Borrowed<'a>;

    const VECTOR_KIND: VectorKind = VectorKind::SVecf32;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    fn for_borrow(&self) -> SVecf32Borrowed<'_> {
        SVecf32Borrowed {
            dims: self.dims,
            indexes: &self.indexes,
            values: &self.values,
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        let mut dense = vec![F32::zero(); self.dims as usize];
        for (&index, &value) in self.indexes.iter().zip(self.values.iter()) {
            dense[index as usize] = value;
        }
        dense
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SVecf32Borrowed<'a> {
    dims: u32,
    indexes: &'a [u32],
    values: &'a [F32],
}

impl<'a> SVecf32Borrowed<'a> {
    #[inline(always)]
    pub fn new(dims: u32, indexes: &'a [u32], values: &'a [F32]) -> Self {
        Self::new_checked(dims, indexes, values).unwrap()
    }
    #[inline(always)]
    pub fn new_checked(dims: u32, indexes: &'a [u32], values: &'a [F32]) -> Option<Self> {
        if !(1..=1_048_575).contains(&dims) {
            return None;
        }
        if indexes.len() != values.len() {
            return None;
        }
        let len = indexes.len();
        for i in 1..len {
            if !(indexes[i - 1] < indexes[i]) {
                return None;
            }
        }
        if len != 0 && !(indexes[len - 1] < dims) {
            return None;
        }
        for i in 0..len {
            if values[i].is_zero() {
                return None;
            }
        }
        unsafe { Some(Self::new_unchecked(dims, indexes, values)) }
    }
    /// # Safety
    ///
    /// * `dims` must be in `1..=1_048_575`.
    /// * `indexes.len()` must be equal to `values.len()`.
    /// * `indexes` must be a strictly increasing sequence and the last in the sequence must be less than `dims`.
    /// * A floating number in `values` must not be positive zero or negative zero.
    #[inline(always)]
    pub unsafe fn new_unchecked(dims: u32, indexes: &'a [u32], values: &'a [F32]) -> Self {
        Self {
            dims,
            indexes,
            values,
        }
    }
    #[inline(always)]
    pub fn indexes(&self) -> &[u32] {
        self.indexes
    }
    #[inline(always)]
    pub fn values(&self) -> &[F32] {
        self.values
    }
}

impl<'a> VectorBorrowed for SVecf32Borrowed<'a> {
    type Scalar = F32;
    type Owned = SVecf32Owned;

    #[inline(always)]
    fn dims(&self) -> u32 {
        self.dims
    }

    fn for_own(&self) -> SVecf32Owned {
        SVecf32Owned {
            dims: self.dims,
            indexes: self.indexes.to_vec(),
            values: self.values.to_vec(),
        }
    }

    fn to_vec(&self) -> Vec<F32> {
        let mut dense = vec![F32::zero(); self.dims as usize];
        for (&index, &value) in self.indexes.iter().zip(self.values.iter()) {
            dense[index as usize] = value;
        }
        dense
    }
}

impl<'a> SVecf32Borrowed<'a> {
    #[inline(always)]
    pub fn len(&self) -> u32 {
        self.indexes.len().try_into().unwrap()
    }
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
fn cosine_fallback<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    let mut lhs_pos = 0;
    let mut rhs_pos = 0;
    let size1 = lhs.len() as usize;
    let size2 = rhs.len() as usize;
    let mut xy = F32::zero();
    let mut x2 = F32::zero();
    let mut y2 = F32::zero();
    while lhs_pos < size1 && rhs_pos < size2 {
        let lhs_index = lhs.indexes()[lhs_pos];
        let rhs_index = rhs.indexes()[rhs_pos];
        match lhs_index.cmp(&rhs_index) {
            std::cmp::Ordering::Less => {
                x2 += lhs.values()[lhs_pos] * lhs.values()[lhs_pos];
                lhs_pos += 1;
            }
            std::cmp::Ordering::Greater => {
                y2 += rhs.values()[rhs_pos] * rhs.values()[rhs_pos];
                rhs_pos += 1;
            }
            std::cmp::Ordering::Equal => {
                xy += lhs.values()[lhs_pos] * rhs.values()[rhs_pos];
                x2 += lhs.values()[lhs_pos] * lhs.values()[lhs_pos];
                y2 += rhs.values()[rhs_pos] * rhs.values()[rhs_pos];
                lhs_pos += 1;
                rhs_pos += 1;
            }
        }
    }
    for i in lhs_pos..size1 {
        x2 += lhs.values()[i] * lhs.values()[i];
    }
    for i in rhs_pos..size2 {
        y2 += rhs.values()[i] * rhs.values()[i];
    }
    xy / (x2 * y2).sqrt()
}

#[inline]
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512f,bmi2")]
unsafe fn cosine_v4<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    use std::arch::x86_64::*;
    use std::cmp::min;
    #[inline]
    #[target_feature(enable = "avx512bw,avx512f,bmi2")]
    pub unsafe fn _mm512_maskz_loadu_epi32(k: __mmask16, mem_addr: *const i32) -> __m512i {
        let mut dst: __m512i;
        unsafe {
            std::arch::asm!(
                "vmovdqu32 {dst}{{{k}}} {{z}}, [{p}]",
                p = in(reg) mem_addr,
                k = in(kreg) k,
                dst = out(zmm_reg) dst,
                options(pure, readonly, nostack)
            );
        }
        dst
    }
    #[inline]
    #[target_feature(enable = "avx512bw,avx512f,bmi2")]
    pub unsafe fn _mm512_maskz_loadu_ps(k: __mmask16, mem_addr: *const f32) -> __m512 {
        let mut dst: __m512;
        unsafe {
            std::arch::asm!(
                "vmovups {dst}{{{k}}} {{z}}, [{p}]",
                p = in(reg) mem_addr,
                k = in(kreg) k,
                dst = out(zmm_reg) dst,
                options(pure, readonly, nostack)
            );
        }
        dst
    }
    unsafe {
        const W: usize = 16;
        let mut lhs_pos = 0;
        let mut rhs_pos = 0;
        let size1 = lhs.len() as usize;
        let size2 = rhs.len() as usize;
        let lhs_size = size1 / W * W;
        let rhs_size = size2 / W * W;
        let lhs_idx = lhs.indexes().as_ptr() as *const i32;
        let rhs_idx = rhs.indexes().as_ptr() as *const i32;
        let lhs_val = lhs.values().as_ptr() as *const f32;
        let rhs_val = rhs.values().as_ptr() as *const f32;
        let mut xy = _mm512_setzero_ps();
        while lhs_pos < lhs_size && rhs_pos < rhs_size {
            let i_l = _mm512_loadu_epi32(lhs_idx.add(lhs_pos));
            let i_r = _mm512_loadu_epi32(rhs_idx.add(rhs_pos));
            let (m_l, m_r) = emulate_mm512_2intersect_epi32(i_l, i_r);
            let v_l = _mm512_loadu_ps(lhs_val.add(lhs_pos));
            let v_r = _mm512_loadu_ps(rhs_val.add(rhs_pos));
            let v_l = _mm512_maskz_compress_ps(m_l, v_l);
            let v_r = _mm512_maskz_compress_ps(m_r, v_r);
            xy = _mm512_fmadd_ps(v_l, v_r, xy);
            let l_max = lhs.indexes().get_unchecked(lhs_pos + W - 1);
            let r_max = rhs.indexes().get_unchecked(rhs_pos + W - 1);
            match l_max.cmp(r_max) {
                std::cmp::Ordering::Less => {
                    lhs_pos += W;
                }
                std::cmp::Ordering::Greater => {
                    rhs_pos += W;
                }
                std::cmp::Ordering::Equal => {
                    lhs_pos += W;
                    rhs_pos += W;
                }
            }
        }
        while lhs_pos < size1 && rhs_pos < size2 {
            let len_l = min(W, size1 - lhs_pos);
            let len_r = min(W, size2 - rhs_pos);
            let mask_l = _bzhi_u32(0xFFFF, len_l as u32) as u16;
            let mask_r = _bzhi_u32(0xFFFF, len_r as u32) as u16;
            let i_l = _mm512_maskz_loadu_epi32(mask_l, lhs_idx.add(lhs_pos));
            let i_r = _mm512_maskz_loadu_epi32(mask_r, rhs_idx.add(rhs_pos));
            let (m_l, m_r) = emulate_mm512_2intersect_epi32(i_l, i_r);
            let v_l = _mm512_maskz_loadu_ps(mask_l, lhs_val.add(lhs_pos));
            let v_r = _mm512_maskz_loadu_ps(mask_r, rhs_val.add(rhs_pos));
            let v_l = _mm512_maskz_compress_ps(m_l, v_l);
            let v_r = _mm512_maskz_compress_ps(m_r, v_r);
            xy = _mm512_fmadd_ps(v_l, v_r, xy);
            let l_max = lhs.indexes().get_unchecked(lhs_pos + len_l - 1);
            let r_max = rhs.indexes().get_unchecked(rhs_pos + len_r - 1);
            match l_max.cmp(r_max) {
                std::cmp::Ordering::Less => {
                    lhs_pos += W;
                }
                std::cmp::Ordering::Greater => {
                    rhs_pos += W;
                }
                std::cmp::Ordering::Equal => {
                    lhs_pos += W;
                    rhs_pos += W;
                }
            }
        }
        let rxy = _mm512_reduce_add_ps(xy);

        let mut xx = _mm512_setzero_ps();
        let mut lhs_pos = 0;
        while lhs_pos < lhs_size {
            let v = _mm512_loadu_ps(lhs_val.add(lhs_pos));
            xx = _mm512_fmadd_ps(v, v, xx);
            lhs_pos += W;
        }
        let v = _mm512_maskz_loadu_ps(
            _bzhi_u32(0xFFFF, (size1 - lhs_pos) as u32) as u16,
            lhs_val.add(lhs_pos),
        );
        xx = _mm512_fmadd_ps(v, v, xx);
        let rxx = _mm512_reduce_add_ps(xx);

        let mut yy = _mm512_setzero_ps();
        let mut rhs_pos = 0;
        while rhs_pos < rhs_size {
            let v = _mm512_loadu_ps(rhs_val.add(rhs_pos));
            yy = _mm512_fmadd_ps(v, v, yy);
            rhs_pos += W;
        }
        let v = _mm512_maskz_loadu_ps(
            _bzhi_u32(0xFFFF, (size2 - rhs_pos) as u32) as u16,
            rhs_val.add(rhs_pos),
        );
        yy = _mm512_fmadd_ps(v, v, yy);
        let ryy = _mm512_reduce_add_ps(yy);

        F32(rxy / (rxx * ryy).sqrt())
    }
}

#[inline(always)]
pub fn cosine<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    assert_eq!(lhs.dims(), rhs.dims());
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v4() {
        return unsafe { cosine_v4(lhs, rhs) };
    }
    cosine_fallback(lhs, rhs)
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
fn dot_fallback<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    let mut lhs_pos = 0;
    let mut rhs_pos = 0;
    let size1 = lhs.len() as usize;
    let size2 = rhs.len() as usize;
    let mut xy = F32::zero();
    while lhs_pos < size1 && rhs_pos < size2 {
        let lhs_index = lhs.indexes()[lhs_pos];
        let rhs_index = rhs.indexes()[rhs_pos];
        match lhs_index.cmp(&rhs_index) {
            std::cmp::Ordering::Less => {
                lhs_pos += 1;
            }
            std::cmp::Ordering::Greater => {
                rhs_pos += 1;
            }
            std::cmp::Ordering::Equal => {
                xy += lhs.values()[lhs_pos] * rhs.values()[rhs_pos];
                lhs_pos += 1;
                rhs_pos += 1;
            }
        }
    }
    xy
}

#[inline]
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512f,bmi2")]
unsafe fn dot_v4<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    use std::arch::x86_64::*;
    use std::cmp::min;
    #[inline]
    #[target_feature(enable = "avx512bw,avx512f,bmi2")]
    pub unsafe fn _mm512_maskz_loadu_epi32(k: __mmask16, mem_addr: *const i32) -> __m512i {
        let mut dst: __m512i;
        unsafe {
            std::arch::asm!(
                "vmovdqu32 {dst}{{{k}}} {{z}}, [{p}]",
                p = in(reg) mem_addr,
                k = in(kreg) k,
                dst = out(zmm_reg) dst,
                options(pure, readonly, nostack)
            );
        }
        dst
    }
    #[inline]
    #[target_feature(enable = "avx512bw,avx512f,bmi2")]
    pub unsafe fn _mm512_maskz_loadu_ps(k: __mmask16, mem_addr: *const f32) -> __m512 {
        let mut dst: __m512;
        unsafe {
            std::arch::asm!(
                "vmovups {dst}{{{k}}} {{z}}, [{p}]",
                p = in(reg) mem_addr,
                k = in(kreg) k,
                dst = out(zmm_reg) dst,
                options(pure, readonly, nostack)
            );
        }
        dst
    }
    unsafe {
        const W: usize = 16;
        let mut lhs_pos = 0;
        let mut rhs_pos = 0;
        let size1 = lhs.len() as usize;
        let size2 = rhs.len() as usize;
        let lhs_size = size1 / W * W;
        let rhs_size = size2 / W * W;
        let lhs_idx = lhs.indexes().as_ptr() as *const i32;
        let rhs_idx = rhs.indexes().as_ptr() as *const i32;
        let lhs_val = lhs.values().as_ptr() as *const f32;
        let rhs_val = rhs.values().as_ptr() as *const f32;
        let mut xy = _mm512_setzero_ps();
        while lhs_pos < lhs_size && rhs_pos < rhs_size {
            let i_l = _mm512_loadu_epi32(lhs_idx.add(lhs_pos));
            let i_r = _mm512_loadu_epi32(rhs_idx.add(rhs_pos));
            let (m_l, m_r) = emulate_mm512_2intersect_epi32(i_l, i_r);
            let v_l = _mm512_loadu_ps(lhs_val.add(lhs_pos));
            let v_r = _mm512_loadu_ps(rhs_val.add(rhs_pos));
            let v_l = _mm512_maskz_compress_ps(m_l, v_l);
            let v_r = _mm512_maskz_compress_ps(m_r, v_r);
            xy = _mm512_fmadd_ps(v_l, v_r, xy);
            let l_max = lhs.indexes().get_unchecked(lhs_pos + W - 1);
            let r_max = rhs.indexes().get_unchecked(rhs_pos + W - 1);
            match l_max.cmp(r_max) {
                std::cmp::Ordering::Less => {
                    lhs_pos += W;
                }
                std::cmp::Ordering::Greater => {
                    rhs_pos += W;
                }
                std::cmp::Ordering::Equal => {
                    lhs_pos += W;
                    rhs_pos += W;
                }
            }
        }
        while lhs_pos < size1 && rhs_pos < size2 {
            let len_l = min(W, size1 - lhs_pos);
            let len_r = min(W, size2 - rhs_pos);
            let mask_l = _bzhi_u32(0xFFFF, len_l as u32) as u16;
            let mask_r = _bzhi_u32(0xFFFF, len_r as u32) as u16;
            let i_l = _mm512_maskz_loadu_epi32(mask_l, lhs_idx.add(lhs_pos));
            let i_r = _mm512_maskz_loadu_epi32(mask_r, rhs_idx.add(rhs_pos));
            let (m_l, m_r) = emulate_mm512_2intersect_epi32(i_l, i_r);
            let v_l = _mm512_maskz_loadu_ps(mask_l, lhs_val.add(lhs_pos));
            let v_r = _mm512_maskz_loadu_ps(mask_r, rhs_val.add(rhs_pos));
            let v_l = _mm512_maskz_compress_ps(m_l, v_l);
            let v_r = _mm512_maskz_compress_ps(m_r, v_r);
            xy = _mm512_fmadd_ps(v_l, v_r, xy);
            let l_max = lhs.indexes().get_unchecked(lhs_pos + len_l - 1);
            let r_max = rhs.indexes().get_unchecked(rhs_pos + len_r - 1);
            match l_max.cmp(r_max) {
                std::cmp::Ordering::Less => {
                    lhs_pos += W;
                }
                std::cmp::Ordering::Greater => {
                    rhs_pos += W;
                }
                std::cmp::Ordering::Equal => {
                    lhs_pos += W;
                    rhs_pos += W;
                }
            }
        }
        F32(_mm512_reduce_add_ps(xy))
    }
}

#[inline(always)]
pub fn dot<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    assert_eq!(lhs.dims(), rhs.dims());
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v4() {
        return unsafe { dot_v4(lhs, rhs) };
    }
    dot_fallback(lhs, rhs)
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn dot_2<'a>(lhs: SVecf32Borrowed<'a>, rhs: &[F32]) -> F32 {
    let mut xy = F32::zero();
    for i in 0..lhs.len() as usize {
        xy += lhs.values()[i] * rhs[lhs.indexes()[i] as usize];
    }
    xy
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
fn sl2_fallback<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    let mut lhs_pos = 0;
    let mut rhs_pos = 0;
    let size1 = lhs.len() as usize;
    let size2 = rhs.len() as usize;
    let mut d2 = F32::zero();
    while lhs_pos < size1 && rhs_pos < size2 {
        let lhs_index = lhs.indexes()[lhs_pos];
        let rhs_index = rhs.indexes()[rhs_pos];
        match lhs_index.cmp(&rhs_index) {
            std::cmp::Ordering::Equal => {
                let d = lhs.values()[lhs_pos] - rhs.values()[rhs_pos];
                d2 += d * d;
                lhs_pos += 1;
                rhs_pos += 1;
            }
            std::cmp::Ordering::Less => {
                d2 += lhs.values()[lhs_pos] * lhs.values()[lhs_pos];
                lhs_pos += 1;
            }
            std::cmp::Ordering::Greater => {
                d2 += rhs.values()[rhs_pos] * rhs.values()[rhs_pos];
                rhs_pos += 1;
            }
        }
    }
    for i in lhs_pos..size1 {
        d2 += lhs.values()[i] * lhs.values()[i];
    }
    for i in rhs_pos..size2 {
        d2 += rhs.values()[i] * rhs.values()[i];
    }
    d2
}

#[inline]
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512f,bmi2")]
unsafe fn sl2_v4<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    use std::arch::x86_64::*;
    use std::cmp::min;
    #[inline]
    #[target_feature(enable = "avx512bw,avx512f,bmi2")]
    pub unsafe fn _mm512_maskz_loadu_epi32(k: __mmask16, mem_addr: *const i32) -> __m512i {
        let mut dst: __m512i;
        unsafe {
            std::arch::asm!(
                "vmovdqu32 {dst}{{{k}}} {{z}}, [{p}]",
                p = in(reg) mem_addr,
                k = in(kreg) k,
                dst = out(zmm_reg) dst,
                options(pure, readonly, nostack)
            );
        }
        dst
    }
    #[inline]
    #[target_feature(enable = "avx512bw,avx512f,bmi2")]
    pub unsafe fn _mm512_maskz_loadu_ps(k: __mmask16, mem_addr: *const f32) -> __m512 {
        let mut dst: __m512;
        unsafe {
            std::arch::asm!(
                "vmovups {dst}{{{k}}} {{z}}, [{p}]",
                p = in(reg) mem_addr,
                k = in(kreg) k,
                dst = out(zmm_reg) dst,
                options(pure, readonly, nostack)
            );
        }
        dst
    }
    unsafe {
        const W: usize = 16;
        let mut lhs_pos = 0;
        let mut rhs_pos = 0;
        let size1 = lhs.len() as usize;
        let size2 = rhs.len() as usize;
        let lhs_size = size1 / W * W;
        let rhs_size = size2 / W * W;
        let lhs_idx = lhs.indexes().as_ptr() as *const i32;
        let rhs_idx = rhs.indexes().as_ptr() as *const i32;
        let lhs_val = lhs.values().as_ptr() as *const f32;
        let rhs_val = rhs.values().as_ptr() as *const f32;
        let mut dd = _mm512_setzero_ps();
        while lhs_pos < lhs_size && rhs_pos < rhs_size {
            let i_l = _mm512_loadu_epi32(lhs_idx.add(lhs_pos));
            let i_r = _mm512_loadu_epi32(rhs_idx.add(rhs_pos));
            let (m_l, m_r) = emulate_mm512_2intersect_epi32(i_l, i_r);
            let v_l = _mm512_loadu_ps(lhs_val.add(lhs_pos));
            let v_r = _mm512_loadu_ps(rhs_val.add(rhs_pos));
            let v_l = _mm512_maskz_compress_ps(m_l, v_l);
            let v_r = _mm512_maskz_compress_ps(m_r, v_r);
            let d = _mm512_sub_ps(v_l, v_r);
            dd = _mm512_fmadd_ps(d, d, dd);
            dd = _mm512_fmsub_ps(v_l, v_l, dd);
            dd = _mm512_fmsub_ps(v_r, v_r, dd);
            let l_max = lhs.indexes().get_unchecked(lhs_pos + W - 1);
            let r_max = rhs.indexes().get_unchecked(rhs_pos + W - 1);
            match l_max.cmp(r_max) {
                std::cmp::Ordering::Less => {
                    lhs_pos += W;
                }
                std::cmp::Ordering::Greater => {
                    rhs_pos += W;
                }
                std::cmp::Ordering::Equal => {
                    lhs_pos += W;
                    rhs_pos += W;
                }
            }
        }
        while lhs_pos < size1 && rhs_pos < size2 {
            let len_l = min(W, size1 - lhs_pos);
            let len_r = min(W, size2 - rhs_pos);
            let mask_l = _bzhi_u32(0xFFFF, len_l as u32) as u16;
            let mask_r = _bzhi_u32(0xFFFF, len_r as u32) as u16;
            let i_l = _mm512_maskz_loadu_epi32(mask_l, lhs_idx.add(lhs_pos));
            let i_r = _mm512_maskz_loadu_epi32(mask_r, rhs_idx.add(rhs_pos));
            let (m_l, m_r) = emulate_mm512_2intersect_epi32(i_l, i_r);
            let v_l = _mm512_maskz_loadu_ps(mask_l, lhs_val.add(lhs_pos));
            let v_r = _mm512_maskz_loadu_ps(mask_r, rhs_val.add(rhs_pos));
            let v_l = _mm512_maskz_compress_ps(m_l, v_l);
            let v_r = _mm512_maskz_compress_ps(m_r, v_r);
            let d = _mm512_sub_ps(v_l, v_r);
            dd = _mm512_fmadd_ps(d, d, dd);
            dd = _mm512_fmsub_ps(v_l, v_l, dd);
            dd = _mm512_fmsub_ps(v_r, v_r, dd);
            let l_max = lhs.indexes().get_unchecked(lhs_pos + len_l - 1);
            let r_max = rhs.indexes().get_unchecked(rhs_pos + len_r - 1);
            match l_max.cmp(r_max) {
                std::cmp::Ordering::Less => {
                    lhs_pos += W;
                }
                std::cmp::Ordering::Greater => {
                    rhs_pos += W;
                }
                std::cmp::Ordering::Equal => {
                    lhs_pos += W;
                    rhs_pos += W;
                }
            }
        }

        let mut lhs_pos = 0;
        while lhs_pos < lhs_size {
            let v = _mm512_loadu_ps(lhs_val.add(lhs_pos));
            dd = _mm512_fmadd_ps(v, v, dd);
            lhs_pos += W;
        }
        let v = _mm512_maskz_loadu_ps(
            _bzhi_u32(0xFFFF, (size1 - lhs_pos) as u32) as u16,
            lhs_val.add(lhs_pos),
        );
        dd = _mm512_fmadd_ps(v, v, dd);
        let mut rhs_pos = 0;
        while rhs_pos < rhs_size {
            let v = _mm512_loadu_ps(rhs_val.add(rhs_pos));
            dd = _mm512_fmadd_ps(v, v, dd);
            rhs_pos += W;
        }
        let v = _mm512_maskz_loadu_ps(
            _bzhi_u32(0xFFFF, (size2 - rhs_pos) as u32) as u16,
            rhs_val.add(rhs_pos),
        );
        dd = _mm512_fmadd_ps(v, v, dd);

        F32(_mm512_reduce_add_ps(dd))
    }
}

#[inline(always)]
pub fn sl2<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    assert_eq!(lhs.dims(), rhs.dims());
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v4() {
        return unsafe { sl2_v4(lhs, rhs) };
    }
    sl2_fallback(lhs, rhs)
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn sl2_2<'a>(lhs: SVecf32Borrowed<'a>, rhs: &[F32]) -> F32 {
    let mut d2 = F32::zero();
    let mut lhs_pos = 0;
    let mut rhs_pos = 0;
    while lhs_pos < lhs.len() {
        let index_eq = lhs.indexes()[lhs_pos as usize] == rhs_pos;
        let d =
            F32(index_eq as u32 as f32) * lhs.values()[lhs_pos as usize] - rhs[rhs_pos as usize];
        d2 += d * d;
        lhs_pos += index_eq as u32;
        rhs_pos += 1;
    }
    for i in rhs_pos..rhs.len() as u32 {
        d2 += rhs[i as usize] * rhs[i as usize];
    }
    d2
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn length<'a>(vector: SVecf32Borrowed<'a>) -> F32 {
    let mut dot = F32::zero();
    for &i in vector.values() {
        dot += i * i;
    }
    dot.sqrt()
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn l2_normalize(vector: &mut SVecf32Owned) {
    let l = length(vector.for_borrow());
    let dims = vector.dims();
    let indexes = vector.indexes().to_vec();
    let mut values = vector.values().to_vec();
    for i in values.iter_mut() {
        *i /= l;
    }
    *vector = SVecf32Owned::new(dims, indexes, values);
}

// VP2INTERSECT emulation.
// Díez-Cañas, G. (2021). Faster-Than-Native Alternatives for x86 VP2INTERSECT
// Instructions. arXiv preprint arXiv:2112.06342.
#[inline]
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512bw,avx512f")]
unsafe fn emulate_mm512_2intersect_epi32(
    a: std::arch::x86_64::__m512i,
    b: std::arch::x86_64::__m512i,
) -> (u16, u16) {
    use std::arch::x86_64::*;
    unsafe {
        let a1 = _mm512_alignr_epi32(a, a, 4);
        let a2 = _mm512_alignr_epi32(a, a, 8);
        let a3 = _mm512_alignr_epi32(a, a, 12);
        let b1 = _mm512_shuffle_epi32(b, _MM_PERM_ADCB);
        let b2 = _mm512_shuffle_epi32(b, _MM_PERM_BADC);
        let b3 = _mm512_shuffle_epi32(b, _MM_PERM_CBAD);
        let m00 = _mm512_cmpeq_epi32_mask(a, b);
        let m01 = _mm512_cmpeq_epi32_mask(a, b1);
        let m02 = _mm512_cmpeq_epi32_mask(a, b2);
        let m03 = _mm512_cmpeq_epi32_mask(a, b3);
        let m10 = _mm512_cmpeq_epi32_mask(a1, b);
        let m11 = _mm512_cmpeq_epi32_mask(a1, b1);
        let m12 = _mm512_cmpeq_epi32_mask(a1, b2);
        let m13 = _mm512_cmpeq_epi32_mask(a1, b3);
        let m20 = _mm512_cmpeq_epi32_mask(a2, b);
        let m21 = _mm512_cmpeq_epi32_mask(a2, b1);
        let m22 = _mm512_cmpeq_epi32_mask(a2, b2);
        let m23 = _mm512_cmpeq_epi32_mask(a2, b3);
        let m30 = _mm512_cmpeq_epi32_mask(a3, b);
        let m31 = _mm512_cmpeq_epi32_mask(a3, b1);
        let m32 = _mm512_cmpeq_epi32_mask(a3, b2);
        let m33 = _mm512_cmpeq_epi32_mask(a3, b3);

        let m0 = m00 | m10 | m20 | m30;
        let m1 = m01 | m11 | m21 | m31;
        let m2 = m02 | m12 | m22 | m32;
        let m3 = m03 | m13 | m23 | m33;

        let res_a = m00
            | m01
            | m02
            | m03
            | (m10 | m11 | m12 | m13).rotate_left(4)
            | (m20 | m21 | m22 | m23).rotate_left(8)
            | (m30 | m31 | m32 | m33).rotate_right(4);

        let res_b = m0
            | ((0x7777 & m1) << 1)
            | ((m1 >> 3) & 0x1111)
            | ((0x3333 & m2) << 2)
            | ((m2 >> 2) & 0x3333)
            | ((0x1111 & m3) << 3)
            | ((m3 >> 1) & 0x7777);
        (res_a, res_b)
    }
}

#[cfg(target_arch = "x86_64")]
#[cfg(test)]
mod tests {
    use super::*;

    const LHS_SIZE: usize = 300;
    const RHS_SIZE: usize = 350;
    const EPS: F32 = F32(1e-5);

    pub fn random_svector(len: usize) -> SVecf32Owned {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut indexes: Vec<u32> = (0..len).map(|_| rng.gen_range(0..30000)).collect();
        indexes.sort_unstable();
        indexes.dedup();
        let values: Vec<F32> = (0..indexes.len())
            .map(|_| F32(rng.gen_range(-1.0..1.0)))
            .collect();
        SVecf32Owned::new(30000, indexes, values)
    }

    #[test]
    fn test_cosine_svector() {
        let x = random_svector(LHS_SIZE);
        let y = random_svector(RHS_SIZE);
        let cosine_fallback = cosine_fallback(x.for_borrow(), y.for_borrow());
        #[cfg(target_arch = "x86_64")]
        if detect::x86_64::detect_v4() {
            let cosine_v4 = unsafe { cosine_v4(x.for_borrow(), y.for_borrow()) };
            assert!(
                cosine_fallback - cosine_v4 < EPS,
                "cosine_fallback: {}, cosine_v4: {}",
                cosine_fallback,
                cosine_v4
            );
        }
    }

    #[test]
    fn test_dot_svector() {
        let x = random_svector(LHS_SIZE);
        let y = random_svector(RHS_SIZE);
        let dot_fallback = dot_fallback(x.for_borrow(), y.for_borrow());
        #[cfg(target_arch = "x86_64")]
        if detect::x86_64::detect_v4() {
            let dot_v4 = unsafe { dot_v4(x.for_borrow(), y.for_borrow()) };
            assert!(
                dot_fallback - dot_v4 < EPS,
                "dot_fallback: {}, dot_v4: {}",
                dot_fallback,
                dot_v4
            );
        }
    }

    #[test]
    fn test_sl2_svector() {
        let x = random_svector(LHS_SIZE);
        let y = random_svector(RHS_SIZE);
        let sl2_fallback = sl2_fallback(x.for_borrow(), y.for_borrow());
        #[cfg(target_arch = "x86_64")]
        if detect::x86_64::detect_v4() {
            let sl2_v4 = unsafe { sl2_v4(x.for_borrow(), y.for_borrow()) };
            assert!(
                sl2_fallback - sl2_v4 < EPS,
                "sl2_fallback: {}, sl2_v4: {}",
                sl2_fallback,
                sl2_v4
            );
        }
    }
}
