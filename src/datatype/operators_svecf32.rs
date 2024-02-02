use crate::{
    datatype::svecf32::{SVecf32, SVecf32Input, SVecf32Output},
    prelude::{FriendlyError, SessionError},
};
use service::prelude::*;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_add(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> SVecf32Output {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    let mut indexes = Vec::<u16>::with_capacity(std::cmp::max(lhs.len(), rhs.len()));
    let mut values = Vec::<F32>::with_capacity(std::cmp::max(lhs.len(), rhs.len()));
    let mut lhs_iter = lhs.data().iter().peekable();
    let mut rhs_iter = rhs.data().iter().peekable();
    while let (Some(&lhs), Some(&rhs)) = (lhs_iter.peek(), rhs_iter.peek()) {
        match lhs.index.cmp(&rhs.index) {
            std::cmp::Ordering::Less => {
                indexes.push(lhs.index);
                values.push(lhs.value);
                lhs_iter.next();
            }
            std::cmp::Ordering::Equal => {
                let value = lhs.value + rhs.value;
                if !value.is_zero() {
                    indexes.push(lhs.index);
                    values.push(value);
                }
                lhs_iter.next();
                rhs_iter.next();
            }
            std::cmp::Ordering::Greater => {
                indexes.push(rhs.index);
                values.push(rhs.value);
                rhs_iter.next();
            }
        }
    }
    for lhs in lhs_iter {
        indexes.push(lhs.index);
        values.push(lhs.value);
    }
    for rhs in rhs_iter {
        indexes.push(rhs.index);
        values.push(rhs.value);
    }

    SVecf32::new_in_postgres(SparseF32Ref {
        dims: lhs.dims(),
        indexes: &indexes,
        values: &values,
    })
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_minus(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> SVecf32Output {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    let mut indexes = Vec::<u16>::with_capacity(std::cmp::max(lhs.len(), rhs.len()));
    let mut values = Vec::<F32>::with_capacity(std::cmp::max(lhs.len(), rhs.len()));
    let mut lhs_iter = lhs.data().iter().peekable();
    let mut rhs_iter = rhs.data().iter().peekable();
    while let (Some(&lhs), Some(&rhs)) = (lhs_iter.peek(), rhs_iter.peek()) {
        match lhs.index.cmp(&rhs.index) {
            std::cmp::Ordering::Less => {
                indexes.push(lhs.index);
                values.push(lhs.value);
                lhs_iter.next();
            }
            std::cmp::Ordering::Equal => {
                let value = lhs.value - rhs.value;
                if !value.is_zero() {
                    indexes.push(lhs.index);
                    values.push(value);
                }
                lhs_iter.next();
                rhs_iter.next();
            }
            std::cmp::Ordering::Greater => {
                indexes.push(rhs.index);
                values.push(-rhs.value);
                rhs_iter.next();
            }
        }
    }
    for lhs in lhs_iter {
        indexes.push(lhs.index);
        values.push(lhs.value);
    }
    for rhs in rhs_iter {
        indexes.push(rhs.index);
        values.push(-rhs.value);
    }

    SVecf32::new_in_postgres(SparseF32Ref {
        dims: lhs.dims(),
        indexes: &indexes,
        values: &values,
    })
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_lt(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    lhs.deref() < rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_lte(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    lhs.deref() <= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_gt(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    lhs.deref() > rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_gte(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    lhs.deref() >= rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_eq(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    lhs.deref() == rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_neq(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> bool {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    lhs.deref() != rhs.deref()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_cosine(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    SparseF32Cos::distance(lhs.data(), rhs.data()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_dot(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    SparseF32Dot::distance(lhs.data(), rhs.data()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_svecf32_operator_l2(lhs: SVecf32Input<'_>, rhs: SVecf32Input<'_>) -> f32 {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    SparseF32L2::distance(lhs.data(), rhs.data()).to_f32()
}
