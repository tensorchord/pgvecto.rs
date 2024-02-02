use crate::prelude::*;

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn cosine<'a>(lhs: SparseF32Ref<'a>, rhs: SparseF32Ref<'a>) -> F32 {
    let mut lhs_iter = lhs.iter().peekable();
    let mut rhs_iter = rhs.iter().peekable();
    let mut xy = F32::zero();
    let mut x2 = F32::zero();
    let mut y2 = F32::zero();
    while let (Some(&lhs), Some(&rhs)) = (lhs_iter.peek(), rhs_iter.peek()) {
        match lhs.index.cmp(&rhs.index) {
            std::cmp::Ordering::Less => {
                x2 += lhs.value * lhs.value;
                lhs_iter.next();
            }
            std::cmp::Ordering::Equal => {
                xy += lhs.value * rhs.value;
                x2 += lhs.value * lhs.value;
                y2 += rhs.value * rhs.value;
                lhs_iter.next();
                rhs_iter.next();
            }
            std::cmp::Ordering::Greater => {
                y2 += rhs.value * rhs.value;
                rhs_iter.next();
            }
        }
    }
    for lhs in lhs_iter {
        x2 += lhs.value * lhs.value;
    }
    for rhs in rhs_iter {
        y2 += rhs.value * rhs.value;
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
    let mut lhs_iter = lhs.iter().peekable();
    let mut rhs_iter = rhs.iter().peekable();
    let mut xy = F32::zero();
    while let (Some(&lhs), Some(&rhs)) = (lhs_iter.peek(), rhs_iter.peek()) {
        match lhs.index.cmp(&rhs.index) {
            std::cmp::Ordering::Less => {
                lhs_iter.next();
            }
            std::cmp::Ordering::Equal => {
                xy += lhs.value * rhs.value;
                lhs_iter.next();
                rhs_iter.next();
            }
            std::cmp::Ordering::Greater => {
                rhs_iter.next();
            }
        }
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
    let mut lhs_iter = lhs.iter().peekable();
    let mut rhs_iter = rhs.iter().peekable();
    let mut d2 = F32::zero();
    while let (Some(&lhs), Some(&rhs)) = (lhs_iter.peek(), rhs_iter.peek()) {
        match lhs.index.cmp(&rhs.index) {
            std::cmp::Ordering::Less => {
                let d = lhs.value;
                d2 += d * d;
                lhs_iter.next();
            }
            std::cmp::Ordering::Equal => {
                let d = lhs.value - rhs.value;
                d2 += d * d;
                lhs_iter.next();
                rhs_iter.next();
            }
            std::cmp::Ordering::Greater => {
                let d = rhs.value;
                d2 += d * d;
                rhs_iter.next();
            }
        }
    }
    for lhs in lhs_iter {
        let d = lhs.value;
        d2 += d * d;
    }
    for rhs in rhs_iter {
        let d = rhs.value;
        d2 += d * d;
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
        let d = if index < lhs.indexes.len() && lhs.indexes[index] == i {
            let d = lhs.values[index] - rhs[i as usize];
            index += 1;
            d
        } else {
            rhs[i as usize]
        };
        d2 += d * d;
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
