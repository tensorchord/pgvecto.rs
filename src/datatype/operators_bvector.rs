use crate::datatype::memory_bvector::{BVectorInput, BVectorOutput};
use crate::error::*;
use base::vector::*;
use std::num::NonZero;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_and(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> BVectorOutput {
    check_matched_dims(lhs.dims(), rhs.dims());
    BVectorOutput::new(
        lhs.as_borrowed()
            .operator_and(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_or(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> BVectorOutput {
    check_matched_dims(lhs.dims(), rhs.dims());
    BVectorOutput::new(
        lhs.as_borrowed()
            .operator_or(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_xor(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> BVectorOutput {
    check_matched_dims(lhs.dims(), rhs.dims());
    BVectorOutput::new(
        lhs.as_borrowed()
            .operator_xor(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_lt(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() < rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_lte(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() <= rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_gt(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() > rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_gte(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() >= rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_eq(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() == rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_neq(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() != rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_dot(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    BVectorBorrowed::operator_dot(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_hamming(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    BVectorBorrowed::operator_hamming(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_operator_jaccard(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    BVectorBorrowed::operator_jaccard(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_sphere_dot_in(
    lhs: BVectorInput<'_>,
    rhs: pgrx::composite_type!("sphere_bvector"),
) -> bool {
    let center: BVectorOutput = match rhs.get_by_index(NonZero::new(1).unwrap()) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty center at sphere"),
        Err(_) => unreachable!(),
    };
    check_matched_dims(lhs.dims(), center.dims());
    let radius: f32 = match rhs.get_by_index(NonZero::new(2).unwrap()) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty radius at sphere"),
        Err(_) => unreachable!(),
    };
    BVectorBorrowed::operator_dot(lhs.as_borrowed(), center.as_borrowed()).to_f32() < radius
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_sphere_hamming_in(
    lhs: BVectorInput<'_>,
    rhs: pgrx::composite_type!("sphere_bvector"),
) -> bool {
    let center: BVectorOutput = match rhs.get_by_index(NonZero::new(1).unwrap()) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty center at sphere"),
        Err(_) => unreachable!(),
    };
    check_matched_dims(lhs.dims(), center.dims());
    let radius: f32 = match rhs.get_by_index(NonZero::new(2).unwrap()) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty radius at sphere"),
        Err(_) => unreachable!(),
    };
    BVectorBorrowed::operator_hamming(lhs.as_borrowed(), center.as_borrowed()).to_f32() < radius
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_sphere_jaccard_in(
    lhs: BVectorInput<'_>,
    rhs: pgrx::composite_type!("sphere_bvector"),
) -> bool {
    let center: BVectorOutput = match rhs.get_by_index(NonZero::new(1).unwrap()) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty center at sphere"),
        Err(_) => unreachable!(),
    };
    check_value_dims_65535(center.dims());
    check_matched_dims(lhs.dims(), center.dims());
    let radius: f32 = match rhs.get_by_index(NonZero::new(2).unwrap()) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty radius at sphere"),
        Err(_) => unreachable!(),
    };
    BVectorBorrowed::operator_jaccard(lhs.as_borrowed(), center.as_borrowed()).to_f32() < radius
}
