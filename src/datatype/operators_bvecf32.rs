use crate::datatype::memory_bvecf32::{BVecf32Input, BVecf32Output};
use crate::prelude::*;
use base::global::*;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_and(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    let n = check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    let mut results = BVecf32Owned::new_zeroed(n.try_into().unwrap());
    let slice = unsafe { results.data_mut() };
    for i in 0..slice.len() {
        slice[i] = lhs.data()[i] & rhs.data()[i];
    }
    BVecf32Output::new(results.for_borrow())
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_or(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    let mut results = BVecf32Owned::new_zeroed(lhs.dims().try_into().unwrap());
    let slice = unsafe { results.data_mut() };
    for i in 0..slice.len() {
        slice[i] = lhs.data()[i] | rhs.data()[i];
    }
    BVecf32Output::new(results.for_borrow())
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_xor(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    let mut results = BVecf32Owned::new_zeroed(lhs.dims().try_into().unwrap());
    let slice = unsafe { results.data_mut() };
    for i in 0..slice.len() {
        slice[i] = lhs.data()[i] ^ rhs.data()[i];
    }
    BVecf32Output::new(results.for_borrow())
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_lt(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() < rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_lte(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() <= rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_gt(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() > rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_gte(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() >= rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_eq(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() == rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_neq(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() != rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_cosine(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Cos::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_dot(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Dot::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_l2(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32L2::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvecf32_operator_jaccard(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Jaccard::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}
