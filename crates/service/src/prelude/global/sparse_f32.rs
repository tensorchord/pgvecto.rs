use crate::prelude::*;

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn cosine<'a>(lhs: SparseF32Ref<'a>, rhs: SparseF32Ref<'a>) -> F32 {
    let mut pos1 = 0;
    let mut pos2 = 0;
    let size1 = lhs.length() as usize;
    let size2 = rhs.length() as usize;
    let mut xy = F32::zero();
    let mut x2 = F32::zero();
    let mut y2 = F32::zero();
    while pos1 < size1 && pos2 < size2 {
        let lhs_index = lhs.indexes[pos1];
        let rhs_index = rhs.indexes[pos2];
        let lhs_value = lhs.values[pos1];
        let rhs_value = rhs.values[pos2];
        xy += F32((lhs_index == rhs_index) as u32 as f32) * lhs_value * rhs_value;
        x2 += F32((lhs_index <= rhs_index) as u32 as f32) * lhs_value * lhs_value;
        y2 += F32((lhs_index >= rhs_index) as u32 as f32) * rhs_value * rhs_value;
        pos1 += (lhs_index <= rhs_index) as usize;
        pos2 += (lhs_index >= rhs_index) as usize;
    }
    for i in pos1..size1 {
        x2 += lhs.values[i] * lhs.values[i];
    }
    for i in pos2..size2 {
        y2 += rhs.values[i] * rhs.values[i];
    }
    xy / (x2 * y2).sqrt()
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn dot<'a>(lhs: SparseF32Ref<'a>, rhs: SparseF32Ref<'a>) -> F32 {
    let mut pos1 = 0;
    let mut pos2 = 0;
    let size1 = lhs.length() as usize;
    let size2 = rhs.length() as usize;
    let mut xy = F32::zero();
    while pos1 < size1 && pos2 < size2 {
        let lhs_index = lhs.indexes[pos1];
        let rhs_index = rhs.indexes[pos2];
        let lhs_value = lhs.values[pos1];
        let rhs_value = rhs.values[pos2];
        xy += F32((lhs_index == rhs_index) as u32 as f32) * lhs_value * rhs_value;
        pos1 += (lhs_index <= rhs_index) as usize;
        pos2 += (lhs_index >= rhs_index) as usize;
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
pub fn dot_2<'a>(lhs: SparseF32Ref<'a>, rhs: &[F32]) -> F32 {
    let mut xy = F32::zero();
    for i in 0..lhs.indexes.len() {
        xy += lhs.values[i] * rhs[lhs.indexes[i] as usize];
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
pub fn sl2<'a>(lhs: SparseF32Ref<'a>, rhs: SparseF32Ref<'a>) -> F32 {
    let mut pos1 = 0;
    let mut pos2 = 0;
    let size1 = lhs.length() as usize;
    let size2 = rhs.length() as usize;
    let mut d2 = F32::zero();
    while pos1 < size1 && pos2 < size2 {
        let lhs_index = lhs.indexes[pos1];
        let rhs_index = rhs.indexes[pos2];
        let lhs_value = lhs.values[pos1];
        let rhs_value = rhs.values[pos2];
        let d = F32((lhs_index <= rhs_index) as u32 as f32) * lhs_value
            - F32((lhs_index >= rhs_index) as u32 as f32) * rhs_value;
        d2 += d * d;
        pos1 += (lhs_index <= rhs_index) as usize;
        pos2 += (lhs_index >= rhs_index) as usize;
    }
    for i in pos1..size1 {
        d2 += lhs.values[i] * lhs.values[i];
    }
    for i in pos2..size2 {
        d2 += rhs.values[i] * rhs.values[i];
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
pub fn sl2_2<'a>(lhs: SparseF32Ref<'a>, rhs: &[F32]) -> F32 {
    let mut d2 = F32::zero();
    let mut index = 0;
    for i in 0..lhs.dims {
        let has_index = index < lhs.indexes.len() && lhs.indexes[index] == i;
        let d = rhs[i as usize] - F32(has_index as u32 as f32) * lhs.values[index];
        d2 += d * d;
        index += has_index as usize;
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
pub fn length<'a>(vector: SparseF32Ref<'a>) -> F32 {
    let mut dot = F32::zero();
    for &i in vector.values {
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
pub fn l2_normalize(vector: &mut SparseF32) {
    let l = length(SparseF32Ref::from(vector as &SparseF32));
    for i in vector.values.iter_mut() {
        *i /= l;
    }
}
