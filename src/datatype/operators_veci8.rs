use std::ops::Deref;

use crate::datatype::veci8::{Veci8, Veci8Input, Veci8Output};
use crate::prelude::*;
use service::prelude::*;

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_add(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> Veci8Output {
    check_matched_dimensions(lhs.len(), rhs.len());
    let data = (0..lhs.len())
        .map(|i| lhs.index(i) + rhs.index(i))
        .collect::<Vec<_>>();
    let (vector, alpha, offset) = quantization(data);
    Veci8::new_in_postgres(vector.as_slice(), alpha, offset)
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_minus(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> Veci8Output {
    check_matched_dimensions(lhs.len(), rhs.len());
    let data = (0..lhs.len())
        .map(|i| lhs.index(i) - rhs.index(i))
        .collect::<Vec<_>>();
    let (vector, alpha, offset) = quantization(data);
    Veci8::new_in_postgres(vector.as_slice(), alpha, offset)
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_lt(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() < rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_lte(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() <= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_gt(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() > rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_gte(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() >= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_eq(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() == rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_neq(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dimensions(lhs.len(), rhs.len());
    lhs.deref() != rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_cosine(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    I8Cos::distance(lhs.to_ref(), rhs.to_ref()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_dot(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    I8Dot::distance(lhs.to_ref(), rhs.to_ref()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_l2(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    I8L2::distance(lhs.to_ref(), rhs.to_ref()).to_f32()
}
