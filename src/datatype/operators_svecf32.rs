use crate::datatype::memory_svecf32::{SVecf32Input, SVecf32Output};
use crate::error::*;
use base::operator::*;
use base::scalar::*;
use base::vector::*;
use num_traits::Zero;
use std::num::NonZero;
use std::ops::Deref;

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

/// Calculate the element-wise multiplication of two sparse vectors.
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
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    compare(lhs, rhs).is_lt()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_lte(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    compare(lhs, rhs).is_le()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_gt(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    compare(lhs, rhs).is_gt()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_gte(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    compare(lhs, rhs).is_ge()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_eq(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().as_borrowed() == rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_neq(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().as_borrowed() != rhs.deref().as_borrowed()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_cosine(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    SVecf32Cos::distance(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_dot(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    SVecf32Dot::distance(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_operator_l2(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    SVecf32L2::distance(lhs.as_borrowed(), rhs.as_borrowed()).to_f32()
}

fn compare(a: SVecf32Input<'_>, b: SVecf32Input<'_>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    assert!(a.dims() == b.dims());
    let lhs = a.as_borrowed();
    let rhs = b.as_borrowed();
    let mut pos = 0;
    let size1 = lhs.len() as usize;
    let size2 = rhs.len() as usize;
    while pos < size1 && pos < size2 {
        let lhs_index = lhs.indexes()[pos];
        let rhs_index = rhs.indexes()[pos];
        let lhs_value = lhs.values()[pos];
        let rhs_value = rhs.values()[pos];
        match lhs_index.cmp(&rhs_index) {
            Ordering::Less => return lhs_value.cmp(&F32::zero()),
            Ordering::Greater => return F32::zero().cmp(&rhs_value),
            Ordering::Equal => match lhs_value.cmp(&rhs_value) {
                Ordering::Equal => {}
                x => return x,
            },
        }
        pos += 1;
    }
    match size1.cmp(&size2) {
        Ordering::Less => F32::zero().cmp(&rhs.values()[pos]),
        Ordering::Greater => lhs.values()[pos].cmp(&F32::zero()),
        Ordering::Equal => Ordering::Equal,
    }
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
    SVecf32L2::distance(lhs.as_borrowed(), center.as_borrowed()) < F32(radius)
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
    SVecf32Dot::distance(lhs.as_borrowed(), center.as_borrowed()) < F32(radius)
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
    SVecf32Cos::distance(lhs.as_borrowed(), center.as_borrowed()) < F32(radius)
}
