use crate::datatype::bvector::{BVector, BVectorInput, BVectorOutput};
use crate::prelude::*;
use base::scalar::FloatCast;
use service::prelude::*;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_and(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> BVectorOutput {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    let results = lhs.data().values.to_bitvec() & rhs.data().values;
    BVector::new_in_postgres(BinaryVecRef { values: &results })
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_or(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> BVectorOutput {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    let results = lhs.data().values.to_bitvec() | rhs.data().values;
    BVector::new_in_postgres(BinaryVecRef { values: &results })
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_xor(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> BVectorOutput {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    let results = lhs.data().values.to_bitvec() ^ rhs.data().values;
    BVector::new_in_postgres(BinaryVecRef { values: &results })
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_lt(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() < rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_lte(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() <= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_gt(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() > rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_gte(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() >= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_eq(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() == rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_neq(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() != rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_cosine(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> f32 {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    BinaryCos::distance(lhs.data(), rhs.data()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_dot(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> f32 {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    BinaryDot::distance(lhs.data(), rhs.data()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_l2(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> f32 {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    BinaryL2::distance(lhs.data(), rhs.data()).to_f32()
}
