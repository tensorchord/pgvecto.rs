use crate::prelude::*;

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn cosine<'a>(lhs: BinaryVecRef<'a>, rhs: BinaryVecRef<'a>) -> F32 {
    let lhs = lhs.values;
    let rhs = rhs.values;
    assert!(lhs.len() == rhs.len());
    let xy = (lhs.to_bitvec() & rhs).count_ones() as f32;
    let x2 = lhs.count_ones() as f32;
    let y2 = rhs.count_ones() as f32;
    F32(xy / (x2 * y2).sqrt())
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn dot<'a>(lhs: BinaryVecRef<'a>, rhs: BinaryVecRef<'a>) -> F32 {
    let lhs = lhs.values;
    let rhs = rhs.values;
    assert!(lhs.len() == rhs.len());
    let xy = (lhs.to_bitvec() & rhs).count_ones() as f32;
    F32(xy)
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn sl2<'a>(lhs: BinaryVecRef<'a>, rhs: BinaryVecRef<'a>) -> F32 {
    let lhs = lhs.values;
    let rhs = rhs.values;
    assert!(lhs.len() == rhs.len());
    let d2 = (lhs.to_bitvec() ^ rhs).count_ones() as f32;
    F32(d2)
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn length<'a>(vector: BinaryVecRef<'a>) -> F32 {
    let vector = vector.values;
    let l = vector.count_ones() as f32;
    F32(l.sqrt())
}

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn l2_normalize<'a>(vector: BinaryVecRef<'a>) -> Vec<F32> {
    let l = length(vector);
    vector
        .values
        .iter()
        .map(|i| F32(*i as u32 as f32) / l)
        .collect()
}
