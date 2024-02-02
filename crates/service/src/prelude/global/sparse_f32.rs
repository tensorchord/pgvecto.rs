use crate::prelude::*;

#[inline(always)]
#[multiversion::multiversion(targets(
    "x86_64/x86-64-v4",
    "x86_64/x86-64-v3",
    "x86_64/x86-64-v2",
    "aarch64+neon"
))]
pub fn cosine(lhs: &[SparseF32Element], rhs: &[SparseF32Element]) -> F32 {
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
pub fn dot(lhs: &[SparseF32Element], rhs: &[SparseF32Element]) -> F32 {
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
pub fn dot_2(lhs: &[SparseF32Element], rhs: &[F32]) -> F32 {
    let mut xy = F32::zero();
    for &SparseF32Element { index, value } in lhs {
        xy += value * rhs[index as usize];
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
pub fn sl2(lhs: &[SparseF32Element], rhs: &[SparseF32Element]) -> F32 {
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
pub fn sl2_2(lhs: &[SparseF32Element], rhs: &[F32]) -> F32 {
    let mut d2 = F32::zero();
    let mut i = 0;
    for &SparseF32Element { index, value } in lhs {
        while i < index {
            let d = rhs[i as usize];
            d2 += d * d;
            i += 1;
        }
        let d = value - rhs[i as usize];
        d2 += d * d;
        i += 1;
    }
    while i < rhs.len() as u32 {
        let d = rhs[i as usize];
        d2 += d * d;
        i += 1;
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
pub fn length(vector: &[SparseF32Element]) -> F32 {
    let mut dot = F32::zero();
    for i in vector {
        dot += i.value * i.value;
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
pub fn l2_normalize(vector: &mut [SparseF32Element]) {
    let l = length(vector);
    for i in vector {
        i.value /= l;
    }
}
