use crate::{prelude::*, utils::iter::RefPeekable};

pub fn cosine(lhs: &[SparseF32Element], rhs: &[SparseF32Element]) -> F32 {
    let mut lhs_iter = RefPeekable::new(lhs.iter());
    let mut rhs_iter = RefPeekable::new(rhs.iter());
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

pub fn dot(lhs: &[SparseF32Element], rhs: &[SparseF32Element]) -> F32 {
    let mut lhs_iter = RefPeekable::new(lhs.iter());
    let mut rhs_iter = RefPeekable::new(rhs.iter());
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

pub fn sl2(lhs: &[SparseF32Element], rhs: &[SparseF32Element]) -> F32 {
    let mut lhs_iter = RefPeekable::new(lhs.iter());
    let mut rhs_iter = RefPeekable::new(rhs.iter());
    let mut d2 = F32::zero();
    while let (Some(&lhs), Some(&rhs)) = (lhs_iter.peek(), rhs_iter.peek()) {
        match lhs.index.cmp(&rhs.index) {
            std::cmp::Ordering::Less => {
                lhs_iter.next();
            }
            std::cmp::Ordering::Equal => {
                let d = lhs.value - rhs.value;
                d2 += d * d;
                lhs_iter.next();
                rhs_iter.next();
            }
            std::cmp::Ordering::Greater => {
                rhs_iter.next();
            }
        }
    }
    d2
}

#[allow(dead_code)]
pub fn length(vector: &[SparseF32Element]) -> F32 {
    let mut dot = F32::zero();
    for i in vector {
        dot += i.value * i.value;
    }
    dot.sqrt()
}

#[allow(dead_code)]
pub fn l2_normalize(vector: &mut [SparseF32Element]) {
    let l = length(vector);
    for i in vector {
        i.value /= l;
    }
}
