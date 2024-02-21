use crate::datatype::vecf32::{Vecf32, Vecf32Input, Vecf32Output};
use crate::prelude::*;
use base::scalar::FloatCast;
use service::prelude::*;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_add(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> Vecf32Output {
    let n = check_matched_dimensions(lhs.len(), rhs.len());
    let mut v = vec![F32::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] + rhs[i];
    }
    Vecf32::new_in_postgres(&v)
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_minus(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> Vecf32Output {
    let n = check_matched_dimensions(lhs.len(), rhs.len());
    let mut v = vec![F32::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] - rhs[i];
    }
    Vecf32::new_in_postgres(&v)
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_lt(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() < rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_lte(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() <= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_gt(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() > rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_gte(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() >= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_eq(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() == rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_neq(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() != rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_cosine(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    F32Cos::distance(&lhs, &rhs).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_dot(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    F32Dot::distance(&lhs, &rhs).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_operator_l2(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    F32L2::distance(&lhs, &rhs).to_f32()
}
