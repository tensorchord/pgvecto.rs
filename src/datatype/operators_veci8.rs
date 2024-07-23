use crate::datatype::memory_veci8::{Veci8Input, Veci8Output};
use crate::error::*;
use base::operator::*;
use base::scalar::*;
use base::vector::*;
use std::num::NonZero;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_add(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> Veci8Output {
    check_matched_dims(lhs.dims(), rhs.dims());
    Veci8Output::new(
        lhs.as_borrowed()
            .operator_add(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_minus(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> Veci8Output {
    check_matched_dims(lhs.dims(), rhs.dims());
    Veci8Output::new(
        lhs.as_borrowed()
            .operator_minus(rhs.as_borrowed())
            .as_borrowed(),
    )
}

/// Calculate the element-wise multiplication of two i8 vectors.
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_mul(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> Veci8Output {
    check_matched_dims(lhs.dims(), rhs.dims());
    Veci8Output::new(
        lhs.as_borrowed()
            .operator_mul(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_lt(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().as_borrowed() < rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_lte(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().as_borrowed() <= rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_gt(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().as_borrowed() > rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_gte(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().as_borrowed() >= rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_eq(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().as_borrowed() == rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_neq(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().dequantization().as_slice() != rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_cosine(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    Veci8Cos::distance(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_dot(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    Veci8Dot::distance(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_l2(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    Veci8L2::distance(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_sphere_l2_in(
    lhs: Veci8Input<'_>,
    rhs: pgrx::composite_type!("sphere_veci8"),
) -> bool {
    let center: Veci8Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    Veci8L2::distance(lhs.as_borrowed(), center.as_borrowed()) < F32(radius)
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_sphere_dot_in(
    lhs: Veci8Input<'_>,
    rhs: pgrx::composite_type!("sphere_veci8"),
) -> bool {
    let center: Veci8Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    Veci8Dot::distance(lhs.as_borrowed(), center.as_borrowed()) < F32(radius)
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_sphere_cos_in(
    lhs: Veci8Input<'_>,
    rhs: pgrx::composite_type!("sphere_veci8"),
) -> bool {
    let center: Veci8Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    Veci8Cos::distance(lhs.as_borrowed(), center.as_borrowed()) < F32(radius)
}
