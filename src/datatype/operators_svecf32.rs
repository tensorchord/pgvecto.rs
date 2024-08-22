use crate::datatype::memory_svecf32::{SVecf32Input, SVecf32Output};
use crate::error::*;
use base::vector::*;
use std::num::NonZero;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_add(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> SVecf32Output {
    check_matched_dims(lhs.dims(), rhs.dims());
    SVecf32Output::new(
        lhs.as_borrowed()
            .operator_add(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_minus(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> SVecf32Output {
    check_matched_dims(lhs.dims(), rhs.dims());
    SVecf32Output::new(
        lhs.as_borrowed()
            .operator_minus(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_mul(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> SVecf32Output {
    check_matched_dims(lhs.dims(), rhs.dims());
    SVecf32Output::new(
        lhs.as_borrowed()
            .operator_mul(rhs.as_borrowed())
            .as_borrowed(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_lt(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() < rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_lte(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() <= rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_gt(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() > rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_gte(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() >= rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_eq(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() == rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_neq(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.as_borrowed() != rhs.as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_dot(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    SVecf32Borrowed::operator_dot(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_l2(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    SVecf32Borrowed::operator_l2(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_cos(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    SVecf32Borrowed::operator_cos(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_sphere_dot_in(
    lhs: SVecf32Input<'_>,
    rhs: pgrx::composite_type!("sphere_svector"),
) -> bool {
    let center: SVecf32Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    SVecf32Borrowed::operator_dot(lhs.as_borrowed(), center.as_borrowed()).to_f32() < radius
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_sphere_l2_in(
    lhs: SVecf32Input<'_>,
    rhs: pgrx::composite_type!("sphere_svector"),
) -> bool {
    let center: SVecf32Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    SVecf32Borrowed::operator_l2(lhs.as_borrowed(), center.as_borrowed()).to_f32() < radius
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_sphere_cos_in(
    lhs: SVecf32Input<'_>,
    rhs: pgrx::composite_type!("sphere_svector"),
) -> bool {
    let center: SVecf32Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    SVecf32Borrowed::operator_cos(lhs.as_borrowed(), center.as_borrowed()).to_f32() < radius
}
