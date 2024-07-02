use crate::datatype::memory_vecf16::{Vecf16Input, Vecf16Output};
use crate::error::*;
use crate::utils::range::*;
use base::operator::*;
use base::scalar::*;
use base::vector::*;
use num_traits::Zero;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_add(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> Vecf16Output {
    let n = check_matched_dims(lhs.dims(), rhs.dims());
    let mut v = vec![F16::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] + rhs[i];
    }
    Vecf16Output::new(Vecf16Borrowed::new(&v))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_minus(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> Vecf16Output {
    let n = check_matched_dims(lhs.dims(), rhs.dims());
    let mut v = vec![F16::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] - rhs[i];
    }
    Vecf16Output::new(Vecf16Borrowed::new(&v))
}

/// Calculate the element-wise multiplication of two f16 vectors.
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_mul(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> Vecf16Output {
    let n = check_matched_dims(lhs.dims(), rhs.dims());
    let mut v = vec![F16::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] * rhs[i];
    }
    Vecf16Output::new(Vecf16Borrowed::new(&v))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_lt(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() < rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_lte(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() <= rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_gt(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() > rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_gte(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() >= rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_eq(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() == rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_neq(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() != rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_cosine(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    Vecf16Cos::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_dot(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    Vecf16Dot::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_operator_l2(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    Vecf16L2::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

const RELDIS_NAME: &str = RELDIS_NAME_VECF16;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_rel_operator_lt(
    lhs: Vecf16Input<'_>,
    rhs: pgrx::composite_type!(RELDIS_NAME),
) -> bool {
    let source: Vecf16Input<'_> = composite_get(&rhs, RELDIS_SOURCE);
    check_value_dims_65535(source.dims());
    check_matched_dims(lhs.dims(), source.dims());
    let operator: &str = composite_get(&rhs, RELDIS_OPERATOR);
    let threshold: f32 = composite_get(&rhs, RELDIS_THRESHOLD);

    match operator {
        "<->" => Vecf16L2::distance(lhs.for_borrow(), source.for_borrow()).to_f32() < threshold,
        "<=>" => Vecf16Cos::distance(lhs.for_borrow(), source.for_borrow()).to_f32() < threshold,
        "<#>" => Vecf16Dot::distance(lhs.for_borrow(), source.for_borrow()).to_f32() < threshold,
        op => pgrx::error!("Bad input: {RELDIS_OPERATOR} {op} at {RELDIS_NAME}"),
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_rel_operator_lte(
    lhs: Vecf16Input<'_>,
    rhs: pgrx::composite_type!(RELDIS_NAME),
) -> bool {
    let source: Vecf16Input<'_> = composite_get(&rhs, RELDIS_SOURCE);
    check_value_dims_65535(source.dims());
    check_matched_dims(lhs.dims(), source.dims());
    let operator: &str = composite_get(&rhs, RELDIS_OPERATOR);
    let threshold: f32 = composite_get(&rhs, RELDIS_THRESHOLD);

    match operator {
        "<->" => Vecf16L2::distance(lhs.for_borrow(), source.for_borrow()).to_f32() <= threshold,
        "<=>" => Vecf16Cos::distance(lhs.for_borrow(), source.for_borrow()).to_f32() <= threshold,
        "<#>" => Vecf16Dot::distance(lhs.for_borrow(), source.for_borrow()).to_f32() <= threshold,
        op => pgrx::error!("Bad input: {RELDIS_OPERATOR} {op} at {RELDIS_NAME}"),
    }
}
