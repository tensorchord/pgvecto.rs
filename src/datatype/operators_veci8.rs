use crate::datatype::memory_veci8::{Veci8Input, Veci8Output};
use crate::error::*;
use base::operator::*;
use base::scalar::*;
use base::vector::*;
use std::num::NonZero;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_add(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> Veci8Output {
    check_matched_dims(lhs.len(), rhs.len());
    let data = (0..lhs.len())
        .map(|i| lhs.index(i) + rhs.index(i))
        .collect::<Vec<_>>();
    let (vector, alpha, offset) = veci8::i8_quantization(&data);
    let (sum, l2_norm) = veci8::i8_precompute(&vector, alpha, offset);
    Veci8Output::new(
        Veci8Borrowed::new_checked(lhs.len() as u32, &vector, alpha, offset, sum, l2_norm).unwrap(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_minus(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> Veci8Output {
    check_matched_dims(lhs.len(), rhs.len());
    let data = (0..lhs.len())
        .map(|i| lhs.index(i) - rhs.index(i))
        .collect::<Vec<_>>();
    let (vector, alpha, offset) = veci8::i8_quantization(&data);
    let (sum, l2_norm) = veci8::i8_precompute(&vector, alpha, offset);
    Veci8Output::new(
        Veci8Borrowed::new_checked(lhs.len() as u32, &vector, alpha, offset, sum, l2_norm).unwrap(),
    )
}

/// Calculate the element-wise multiplication of two i8 vectors.
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_mul(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> Veci8Output {
    check_matched_dims(lhs.len(), rhs.len());
    let data = (0..lhs.len())
        .map(|i| lhs.index(i) * rhs.index(i))
        .collect::<Vec<_>>();
    let (vector, alpha, offset) = veci8::i8_quantization(&data);
    let (sum, l2_norm) = veci8::i8_precompute(&vector, alpha, offset);
    Veci8Output::new(
        Veci8Borrowed::new_checked(lhs.len() as u32, &vector, alpha, offset, sum, l2_norm).unwrap(),
    )
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_lt(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() < rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_lte(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() <= rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_gt(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() > rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_gte(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() >= rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_eq(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() == rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_neq(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> bool {
    check_matched_dims(lhs.len(), rhs.len());
    lhs.deref().dequantization().as_slice() != rhs.deref().dequantization().as_slice()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_cosine(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dims(lhs.len(), rhs.len());
    Veci8Cos::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_dot(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dims(lhs.len(), rhs.len());
    Veci8Dot::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_operator_l2(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dims(lhs.len(), rhs.len());
    Veci8L2::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
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
    check_matched_dims(lhs.len(), center.len());
    let radius: f32 = match rhs.get_by_index(NonZero::new(2).unwrap()) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty radius at sphere"),
        Err(e) => pgrx::error!("Parse radius failed at sphere:{e}"),
    };
    Veci8L2::distance(lhs.for_borrow(), center.for_borrow()) < F32(radius)
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
    check_matched_dims(lhs.len(), center.len());
    let radius: f32 = match rhs.get_by_index(NonZero::new(2).unwrap()) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty radius at sphere"),
        Err(e) => pgrx::error!("Parse radius failed at sphere:{e}"),
    };
    Veci8Dot::distance(lhs.for_borrow(), center.for_borrow()) < F32(radius)
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
    check_matched_dims(lhs.len(), center.len());
    let radius: f32 = match rhs.get_by_index(NonZero::new(2).unwrap()) {
        Ok(Some(s)) => s,
        Ok(None) => pgrx::error!("Bad input: empty radius at sphere"),
        Err(e) => pgrx::error!("Parse radius failed at sphere:{e}"),
    };
    Veci8Cos::distance(lhs.for_borrow(), center.for_borrow()) < F32(radius)
}
