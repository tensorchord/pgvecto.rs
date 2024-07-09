use crate::datatype::memory_bvecf32::{BVecf32Input, BVecf32Output};
use crate::error::*;
use base::operator::*;
use base::scalar::*;
use base::vector::*;
use std::num::NonZero;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_and(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    let n = check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    let mut data = vec![0_usize; n.div_ceil(usize::BITS as _)];
    for i in 0..data.len() {
        data[i] = lhs.data()[i] & rhs.data()[i];
    }
    BVecf32Output::new(BVecf32Borrowed::new(n as _, &data))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_or(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    let n = check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    let mut data = vec![0_usize; n.div_ceil(usize::BITS as _)];
    for i in 0..data.len() {
        data[i] = lhs.data()[i] | rhs.data()[i];
    }
    BVecf32Output::new(BVecf32Borrowed::new(n as _, &data))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_xor(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> BVecf32Output {
    let n = check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    let mut data = vec![0_usize; n.div_ceil(usize::BITS as _)];
    for i in 0..data.len() {
        data[i] = lhs.data()[i] ^ rhs.data()[i];
    }
    BVecf32Output::new(BVecf32Borrowed::new(n as _, &data))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_lt(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() < rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_lte(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() <= rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_gt(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() > rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_gte(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() >= rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_eq(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() == rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_neq(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> bool {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    lhs.deref().for_borrow() != rhs.deref().for_borrow()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_cosine(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Cos::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_dot(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Dot::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_l2(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32L2::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_operator_jaccard(lhs: BVecf32Input<'_>, rhs: BVecf32Input<'_>) -> f32 {
    check_matched_dims(lhs.dims() as _, rhs.dims() as _);
    BVecf32Jaccard::distance(lhs.for_borrow(), rhs.for_borrow()).to_f32()
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
    BVecf32L2::distance(lhs.for_borrow(), center.for_borrow()) < F32(radius)
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
        Err(e) => pgrx::error!("Parse radius failed at sphere:{e}"),
    };
    BVecf32Dot::distance(lhs.for_borrow(), center.for_borrow()) < F32(radius)
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
        Err(e) => pgrx::error!("Parse radius failed at sphere:{e}"),
    };
    BVecf32Cos::distance(lhs.for_borrow(), center.for_borrow()) < F32(radius)
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
        Err(e) => pgrx::error!("Parse radius failed at sphere:{e}"),
    };
    BVecf32Jaccard::distance(lhs.for_borrow(), center.for_borrow()) < F32(radius)
}
