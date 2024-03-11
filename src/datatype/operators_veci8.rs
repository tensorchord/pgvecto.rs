use crate::datatype::memory_veci8::{Veci8Input, Veci8Output};
use crate::prelude::*;
use base::global::*;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_add(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> Veci8Output {
    check_matched_dims(lhs.len(), rhs.len());
    let data = (0..lhs.len())
        .map(|i| lhs.index(i) + rhs.index(i))
        .collect::<Vec<_>>();
    let (vector, alpha, offset) = i8_quantization(&data);
    let (sum, l2_norm) = i8_precompute(&vector, alpha, offset);
    Veci8Output::new(
        Veci8Borrowed::new_checked(lhs.len() as u32, &vector, alpha, offset, sum, l2_norm).unwrap(),
    )
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_minus(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> Veci8Output {
    check_matched_dims(lhs.len(), rhs.len());
    let data = (0..lhs.len())
        .map(|i| lhs.index(i) - rhs.index(i))
        .collect::<Vec<_>>();
    let (vector, alpha, offset) = i8_quantization(&data);
    let (sum, l2_norm) = i8_precompute(&vector, alpha, offset);
    Veci8Output::new(
        Veci8Borrowed::new_checked(lhs.len() as u32, &vector, alpha, offset, sum, l2_norm).unwrap(),
    )
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_lt(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() < rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_lte(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() <= rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_gt(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() > rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_gte(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() >= rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_eq(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() == rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_neq(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() != rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_cosine(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dims(lhs.len(), rhs.len());
    Veci8Cos::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_dot(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dims(lhs.len(), rhs.len());
    Veci8Dot::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_l2(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dims(lhs.len(), rhs.len());
    Veci8L2::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}
