use crate::datatype::vecf16::{Vecf16, Vecf16Input, Vecf16Output};
use crate::prelude::*;
use base::scalar::FloatCast;
use service::prelude::*;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_add(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> Vecf16Output {
    let n = check_matched_dimensions(lhs.len(), rhs.len());
    let mut v = vec![F16::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] + rhs[i];
    }
    Vecf16::new_in_postgres(&v)
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_minus(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> Vecf16Output {
    let n = check_matched_dimensions(lhs.len(), rhs.len());
    let mut v = vec![F16::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] - rhs[i];
    }
    Vecf16::new_in_postgres(&v)
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_lt(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() < rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_lte(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() <= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_gt(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() > rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_gte(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() >= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_eq(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() == rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_neq(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() != rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_cosine(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    F16Cos::distance(&lhs, &rhs).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_dot(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    F16Dot::distance(&lhs, &rhs).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf16_operator_l2(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    F16L2::distance(&lhs, &rhs).to_f32()
}
