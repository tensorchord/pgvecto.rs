use crate::datatype::svecf32::{SVecf32, SVecf32Input, SVecf32Output};
use crate::prelude::*;
use base::scalar::FloatCast;
use service::prelude::*;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_add(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> SVecf32Output {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);

    let size1 = lhs.len();
    let size2 = rhs.len();
    let mut pos1 = 0;
    let mut pos2 = 0;
    let mut pos = 0;
    let mut indexes = vec![0u16; size1 + size2];
    let mut values = vec![F32::zero(); size1 + size2];
    let lhs = lhs.data();
    let rhs = rhs.data();
    while pos1 < size1 && pos2 < size2 {
        let lhs_index = lhs.indexes[pos1];
        let rhs_index = rhs.indexes[pos2];
        let lhs_value = lhs.values[pos1];
        let rhs_value = rhs.values[pos2];
        indexes[pos] = lhs_index.min(rhs_index);
        values[pos] = F32((lhs_index <= rhs_index) as u32 as f32) * lhs_value
            + F32((lhs_index >= rhs_index) as u32 as f32) * rhs_value;
        pos1 += (lhs_index <= rhs_index) as usize;
        pos2 += (lhs_index >= rhs_index) as usize;
        pos += (!values[pos].is_zero()) as usize;
    }
    for i in pos1..size1 {
        indexes[pos] = lhs.indexes[i];
        values[pos] = lhs.values[i];
        pos += 1;
    }
    for i in pos2..size2 {
        indexes[pos] = rhs.indexes[i];
        values[pos] = rhs.values[i];
        pos += 1;
    }
    indexes.truncate(pos);
    values.truncate(pos);

    SVecf32::new_in_postgres(SparseF32Ref {
        dims: lhs.dims(),
        indexes: &indexes,
        values: &values,
    })
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_minus(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> SVecf32Output {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);

    let size1 = lhs.len();
    let size2 = rhs.len();
    let mut pos1 = 0;
    let mut pos2 = 0;
    let mut pos = 0;
    let mut indexes = vec![0u16; size1 + size2];
    let mut values = vec![F32::zero(); size1 + size2];
    let lhs = lhs.data();
    let rhs = rhs.data();
    while pos1 < size1 && pos2 < size2 {
        let lhs_index = lhs.indexes[pos1];
        let rhs_index = rhs.indexes[pos2];
        let lhs_value = lhs.values[pos1];
        let rhs_value = rhs.values[pos2];
        indexes[pos] = lhs_index.min(rhs_index);
        values[pos] = F32((lhs_index <= rhs_index) as u32 as f32) * lhs_value
            - F32((lhs_index >= rhs_index) as u32 as f32) * rhs_value;
        pos1 += (lhs_index <= rhs_index) as usize;
        pos2 += (lhs_index >= rhs_index) as usize;
        pos += (!values[pos].is_zero()) as usize;
    }
    for i in pos1..size1 {
        indexes[pos] = lhs.indexes[i];
        values[pos] = lhs.values[i];
        pos += 1;
    }
    for i in pos2..size2 {
        indexes[pos] = rhs.indexes[i];
        values[pos] = -rhs.values[i];
        pos += 1;
    }
    indexes.truncate(pos);
    values.truncate(pos);

    SVecf32::new_in_postgres(SparseF32Ref {
        dims: lhs.dims(),
        indexes: &indexes,
        values: &values,
    })
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_lt(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() < rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_lte(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() <= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_gt(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() > rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_gte(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() >= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_eq(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() == rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_neq(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    lhs.deref() != rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_cosine(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    SparseF32Cos::distance(lhs.data(), rhs.data()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_dot(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    SparseF32Dot::distance(lhs.data(), rhs.data()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_l2(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    check_matched_dimensions(lhs.dims() as _, rhs.dims() as _);
    SparseF32L2::distance(lhs.data(), rhs.data()).to_f32()
}
