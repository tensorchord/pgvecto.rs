use crate::datatype::memory_bvecf32::{BVecf32Input, BVecf32Output};
use crate::error::*;
use base::operator::*;
use base::scalar::*;
use base::vector::*;
use std::num::NonZero;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_and(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    check_matched_dims(lhs.dims(), rhs.dims());
    BVecf32Output::new(
        lhs.as_borrowed()
            .operator_and(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_or(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    check_matched_dims(lhs.dims(), rhs.dims());
    BVecf32Output::new(
        lhs.as_borrowed()
            .operator_or(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_xor(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    check_matched_dims(lhs.dims(), rhs.dims());
    BVecf32Output::new(
        lhs.as_borrowed()
            .operator_xor(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_lt(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().as_borrowed() < rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_lte(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().as_borrowed() <= rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_gt(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().as_borrowed() > rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_gte(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().as_borrowed() >= rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_eq(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().as_borrowed() == rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_neq(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().as_borrowed() != rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_cosine(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Cos::distance(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_dot(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Dot::distance(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_l2(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32L2::distance(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_jaccard(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Jaccard::distance(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_sphere_l2_in(
    lhs: BVecf32Input<'_>,
    rhs: pgrx::composite_type!("sphere_bvector"),
) -> bool {
    let center: BVecf32Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    BVecf32L2::distance(lhs.as_borrowed(), center.as_borrowed()) < F32(radius)
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_sphere_dot_in(
    lhs: BVecf32Input<'_>,
    rhs: pgrx::composite_type!("sphere_bvector"),
) -> bool {
    let center: BVecf32Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    BVecf32Dot::distance(lhs.as_borrowed(), center.as_borrowed()) < F32(radius)
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_sphere_cos_in(
    lhs: BVecf32Input<'_>,
    rhs: pgrx::composite_type!("sphere_bvector"),
) -> bool {
    let center: BVecf32Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    BVecf32Cos::distance(lhs.as_borrowed(), center.as_borrowed()) < F32(radius)
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_sphere_jaccard_in(
    lhs: BVecf32Input<'_>,
    rhs: pgrx::composite_type!("sphere_bvector"),
) -> bool {
    let center: BVecf32Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    BVecf32Jaccard::distance(lhs.as_borrowed(), center.as_borrowed()) < F32(radius)
}
