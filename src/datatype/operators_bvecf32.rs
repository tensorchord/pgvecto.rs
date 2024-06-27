use crate::datatype::memory_bvecf32::{BVecf32Input, BVecf32Output};
use crate::error::*;
use crate::utils::range::*;
use base::operator::*;
use base::scalar::*;
use base::vector::*;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_and(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    let n = check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    let mut data = vec![0_usize; n.div_ceil(usize::BITS as _)];
    for i in 0..data.len() {
        data[i] = lhs.data()[i] & rhs.data()[i];
    }
    BVecf32Output::new(BVecf32Borrowed::new(n as _, &data))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_or(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    let n = check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    let mut data = vec![0_usize; n.div_ceil(usize::BITS as _)];
    for i in 0..data.len() {
        data[i] = lhs.data()[i] | rhs.data()[i];
    }
    BVecf32Output::new(BVecf32Borrowed::new(n as _, &data))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_xor(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    let n = check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    let mut data = vec![0_usize; n.div_ceil(usize::BITS as _)];
    for i in 0..data.len() {
        data[i] = lhs.data()[i] ^ rhs.data()[i];
    }
    BVecf32Output::new(BVecf32Borrowed::new(n as _, &data))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_lt(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() < rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_lte(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() <= rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_gt(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() > rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_gte(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() >= rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_eq(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() == rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_neq(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() != rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_cosine(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Cos::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_dot(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Dot::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_l2(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32L2::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_jaccard(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Jaccard::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

const RELDIS_NAME: &str = RELDIS_NAME_BVECF32;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_rel_operator_lt(
    lhs: BVecf32Input<'_>,
    rhs: pgrx::composite_type!(RELDIS_NAME),
) -> bool {
    let source: BVecf32Input<'_> = composite_get(&rhs, RELDIS_SOURCE);
    check_value_dims_65535(source.dims());
    check_matched_dims(lhs.dims(), source.dims());
    let operator: &str = composite_get(&rhs, RELDIS_OPERATOR);
    let threshold: f32 = composite_get(&rhs, RELDIS_THRESHOLD);

    match operator {
        "<->" => BVecf32L2::distance(lhs.for_borrow(), source.for_borrow()).to_f32() < threshold,
        "<=>" => BVecf32Cos::distance(lhs.for_borrow(), source.for_borrow()).to_f32() < threshold,
        "<#>" => BVecf32Dot::distance(lhs.for_borrow(), source.for_borrow()).to_f32() < threshold,
        "<~>" => {
            BVecf32Jaccard::distance(lhs.for_borrow(), source.for_borrow()).to_f32() < threshold
        }
        op => pgrx::error!("Bad input: {RELDIS_OPERATOR} {op} at {RELDIS_NAME}"),
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_rel_operator_lte(
    lhs: BVecf32Input<'_>,
    rhs: pgrx::composite_type!(RELDIS_NAME),
) -> bool {
    let source: BVecf32Input<'_> = composite_get(&rhs, RELDIS_SOURCE);
    check_value_dims_65535(source.dims());
    check_matched_dims(lhs.dims(), source.dims());
    let operator: &str = composite_get(&rhs, RELDIS_OPERATOR);
    let threshold: f32 = composite_get(&rhs, RELDIS_THRESHOLD);

    match operator {
        "<->" => BVecf32L2::distance(lhs.for_borrow(), source.for_borrow()).to_f32() <= threshold,
        "<=>" => BVecf32Cos::distance(lhs.for_borrow(), source.for_borrow()).to_f32() <= threshold,
        "<#>" => BVecf32Dot::distance(lhs.for_borrow(), source.for_borrow()).to_f32() <= threshold,
        "<~>" => {
            BVecf32Jaccard::distance(lhs.for_borrow(), source.for_borrow()).to_f32() <= threshold
        }
        op => pgrx::error!("Bad input: {RELDIS_OPERATOR} {op} at {RELDIS_NAME}"),
    }
}
