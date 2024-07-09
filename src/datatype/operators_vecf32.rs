use crate::datatype::memory_vecf32::{Vecf32Input, Vecf32Output};
use crate::error::*;
use base::operator::*;
use base::scalar::*;
use base::vector::*;
use num_traits::Zero;
use std::num::NonZero;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_add(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> Vecf32Output {
    let n = check_matched_dims(lhs.dims(), rhs.dims());
    let mut v = vec![F32::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] + rhs[i];
    }
    Vecf32Output::new(Vecf32Borrowed::new(&v))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_minus(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> Vecf32Output {
    let n = check_matched_dims(lhs.dims(), rhs.dims());
    let mut v = vec![F32::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] - rhs[i];
    }
    Vecf32Output::new(Vecf32Borrowed::new(&v))
}

/// Calculate the element-wise multiplication of two vectors.
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_mul(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> Vecf32Output {
    let n = check_matched_dims(lhs.dims(), rhs.dims());
    let mut v = vec![F32::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] * rhs[i];
    }
    Vecf32Output::new(Vecf32Borrowed::new(&v))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_lt(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() < rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_lte(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() <= rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_gt(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() > rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_gte(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() >= rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_eq(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() == rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_neq(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims(), rhs.dims());
    lhs.deref().slice() != rhs.deref().slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_cosine(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    Vecf32Cos::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_dot(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    Vecf32Dot::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_operator_l2(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims(), rhs.dims());
    Vecf32L2::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_sphere_l2_in(
    lhs: Vecf32Input<'_>,
    rhs: pgrx::composite_type!("sphere_vector"),
) -> bool {
    let center: Vecf32Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    Vecf32L2::distance(lhs.for_borrow(), center.for_borrow()) < F32(radius)
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_sphere_dot_in(
    lhs: Vecf32Input<'_>,
    rhs: pgrx::composite_type!("sphere_vector"),
) -> bool {
    let center: Vecf32Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    Vecf32Dot::distance(lhs.for_borrow(), center.for_borrow()) < F32(radius)
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_sphere_cos_in(
    lhs: Vecf32Input<'_>,
    rhs: pgrx::composite_type!("sphere_vector"),
) -> bool {
    let center: Vecf32Output = match rhs.get_by_index(NonZero::new(1).unwrap()) {
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
    Vecf32Cos::distance(lhs.for_borrow(), center.for_borrow()) < F32(radius)
}
