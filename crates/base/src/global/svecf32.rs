use super::SVecf32Owned;
use crate::scalar::*;
use crate::vector::*;
use num_traits::{Float, Zero};
use std::arch::x86_64::*;

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn cosine<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
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
        let lhs_value = lhs.values()[lhs_pos];
        let rhs_value = rhs.values()[rhs_pos];
        xy += F32((lhs_index == rhs_index) as u32 as f32) * lhs_value * rhs_value;
        x2 += F32((lhs_index <= rhs_index) as u32 as f32) * lhs_value * lhs_value;
        y2 += F32((lhs_index >= rhs_index) as u32 as f32) * rhs_value * rhs_value;
        lhs_pos += (lhs_index <= rhs_index) as usize;
        rhs_pos += (lhs_index >= rhs_index) as usize;
    }
    for i in lhs_pos..size1 {
        x2 += lhs.values()[i] * lhs.values()[i];
    }
    for i in rhs_pos..size2 {
        y2 += rhs.values()[i] * rhs.values()[i];
    }
    xy / (x2 * y2).sqrt()
}

#[inline(always)]
pub fn dot<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    #[inline(always)]
    #[multiversion::multiversion(targets(
        "x86_64/x86-64-v3",
        "x86_64/x86-64-v2",
        "aarch64+neon"
    ))]
    fn dot<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
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

    #[inline(always)]
    #[cfg(target_arch = "x86_64")]
    fn dot_avx<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
        unsafe {
            const W: usize = 16;
            let mut lhs_pos = 0;
            let mut rhs_pos = 0;
            let size1 = lhs.len() as usize;
            let size2 = rhs.len() as usize;
            let lhs_size = (size1 + W + 1) / W * W;
            let rhs_size = (size2 + W + 1) / W * W;
            let mut xy = _mm512_setzero_ps();
            while lhs_pos < lhs_size && rhs_pos < rhs_size {
                let i_l = _mm512_loadu_epi32(lhs.indexes()[lhs_pos..].as_ptr().cast());
                let i_r = _mm512_loadu_epi32(rhs.indexes()[rhs_pos..].as_ptr().cast());
                let (m_l, m_r) = emulate_mm512_2intersect_epi32(i_l, i_r);
                let v_l = _mm512_loadu_ps(lhs.values()[lhs_pos..].as_ptr().cast());
                let v_r = _mm512_loadu_ps(rhs.values()[rhs_pos..].as_ptr().cast());
                let v_l = _mm512_maskz_compress_ps(m_l, v_l);
                let v_r = _mm512_maskz_compress_ps(m_r, v_r);
                xy = _mm512_fmadd_ps(v_l, v_r, xy);
                let l_max = lhs.indexes()[lhs_pos + W - 1];
                let r_max = rhs.indexes()[rhs_pos + W - 1];
                match l_max.cmp(&r_max) {
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
                use std::cmp::min;
                let len_l = min(W, size1 - lhs_pos);
                let len_r = min(W, size2 - rhs_pos);
                let mask_l = _bzhi_u32(0xFF, len_l as u32) as u16;
                let mask_r = _bzhi_u32(0xFF, len_r as u32) as u16;
                let i_l =
                    _mm512_maskz_loadu_epi32(mask_l, lhs.indexes()[lhs_pos..].as_ptr().cast());
                let i_r =
                    _mm512_maskz_loadu_epi32(mask_r, rhs.indexes()[rhs_pos..].as_ptr().cast());
                let (m_l, m_r) = emulate_mm512_2intersect_epi32(i_l, i_r);
                let v_l = _mm512_maskz_loadu_ps(mask_l, lhs.values()[lhs_pos..].as_ptr().cast());
                let v_r = _mm512_maskz_loadu_ps(mask_r, rhs.values()[rhs_pos..].as_ptr().cast());
                let v_l = _mm512_maskz_compress_ps(m_l, v_l);
                let v_r = _mm512_maskz_compress_ps(m_r, v_r);
                xy = _mm512_fmadd_ps(v_l, v_r, xy);
                let l_max = lhs.indexes()[lhs_pos + len_l - 1];
                let r_max = rhs.indexes()[rhs_pos + len_r - 1];
                match l_max.cmp(&r_max) {
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

    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_avx512vp2intersect() {
        unsafe {
            return F32(c::v_sparse_dot_avx512vp2intersect(
                lhs.indexes().as_ptr(),
                rhs.indexes().as_ptr(),
                lhs.values().as_ptr(),
                rhs.values().as_ptr(),
                lhs.len(),
                rhs.len(),
            ));
        }
    }
    #[cfg(target_arch = "x86_64")]
    if detect::x86_64::detect_v4() {
        unsafe {
            return dot_avx(lhs, rhs);
        }
    }
    dot(lhs, rhs)
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
pub fn sl2<'a>(lhs: SVecf32Borrowed<'a>, rhs: SVecf32Borrowed<'a>) -> F32 {
    let mut lhs_pos = 0;
    let mut rhs_pos = 0;
    let size1 = lhs.len() as usize;
    let size2 = rhs.len() as usize;
    let mut d2 = F32::zero();
    while lhs_pos < size1 && rhs_pos < size2 {
        let lhs_index = lhs.indexes()[lhs_pos];
        let rhs_index = rhs.indexes()[rhs_pos];
        let lhs_value = lhs.values()[lhs_pos];
        let rhs_value = rhs.values()[rhs_pos];
        let d = F32((lhs_index <= rhs_index) as u32 as f32) * lhs_value
            - F32((lhs_index >= rhs_index) as u32 as f32) * rhs_value;
        d2 += d * d;
        lhs_pos += (lhs_index <= rhs_index) as usize;
        rhs_pos += (lhs_index >= rhs_index) as usize;
    }
    for i in lhs_pos..size1 {
        d2 += lhs.values()[i] * lhs.values()[i];
    }
    for i in rhs_pos..size2 {
        d2 += rhs.values()[i] * rhs.values()[i];
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
#[inline(always)]
#[cfg(target_arch = "x86_64")]
unsafe fn emulate_mm512_2intersect_epi32(a: __m512i, b: __m512i) -> (u16, u16) {
    unsafe {
        let a1 = _mm512_alignr_epi32(a, a, 4);
        let b1 = _mm512_shuffle_epi32(b, _MM_PERM_ADCB);
        let m00 = _mm512_cmpeq_epi32_mask(a, b);
        let b2 = _mm512_shuffle_epi32(b, _MM_PERM_BADC);
        let b3 = _mm512_shuffle_epi32(b, _MM_PERM_CBAD);
        let m01 = _mm512_cmpeq_epi32_mask(a, b1);
        let m02 = _mm512_cmpeq_epi32_mask(a, b2);
        let m03 = _mm512_cmpeq_epi32_mask(a, b3);
        let a2 = _mm512_alignr_epi32(a, a, 8);
        let m10 = _mm512_cmpeq_epi32_mask(a1, b);
        let m11 = _mm512_cmpeq_epi32_mask(a1, b1);
        let m12 = _mm512_cmpeq_epi32_mask(a1, b2);
        let m13 = _mm512_cmpeq_epi32_mask(a1, b3);
        let a3 = _mm512_alignr_epi32(a, a, 12);
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
