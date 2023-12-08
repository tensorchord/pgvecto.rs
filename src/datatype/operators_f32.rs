use crate::datatype::vecf32::{Vecf32, Vecf32Input, Vecf32Output};
use service::prelude::*;
use std::ops::Deref;

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(+)]
#[pgrx::commutator(+)]
fn vecf32_operator_add(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> Vecf32Output {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    let n = lhs.len();
    let mut v = vec![F32::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] + rhs[i];
    }
    Vecf32::new_in_postgres(&v)
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(-)]
fn vecf32_operator_minus(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> Vecf32Output {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    let n = lhs.len();
    let mut v = vec![F32::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] - rhs[i];
    }
    Vecf32::new_in_postgres(&v)
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(<)]
#[pgrx::negator(>=)]
#[pgrx::commutator(>)]
#[pgrx::restrict(scalarltsel)]
#[pgrx::join(scalarltjoinsel)]
fn vecf32_operator_lt(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() < rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(<=)]
#[pgrx::negator(>)]
#[pgrx::commutator(>=)]
#[pgrx::restrict(scalarltsel)]
#[pgrx::join(scalarltjoinsel)]
fn vecf32_operator_lte(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() <= rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(>)]
#[pgrx::negator(<=)]
#[pgrx::commutator(<)]
#[pgrx::restrict(scalargtsel)]
#[pgrx::join(scalargtjoinsel)]
fn vecf32_operator_gt(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() > rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(>=)]
#[pgrx::negator(<)]
#[pgrx::commutator(<=)]
#[pgrx::restrict(scalargtsel)]
#[pgrx::join(scalargtjoinsel)]
fn vecf32_operator_gte(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() >= rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(=)]
#[pgrx::negator(<>)]
#[pgrx::commutator(=)]
#[pgrx::restrict(eqsel)]
#[pgrx::join(eqjoinsel)]
fn vecf32_operator_eq(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() == rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(<>)]
#[pgrx::negator(=)]
#[pgrx::commutator(<>)]
#[pgrx::restrict(eqsel)]
#[pgrx::join(eqjoinsel)]
fn vecf32_operator_neq(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() != rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(<=>)]
#[pgrx::commutator(<=>)]
fn vecf32_operator_cosine(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> f32 {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    F32Cos::distance(&lhs, &rhs).to_f32()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(<#>)]
#[pgrx::commutator(<#>)]
fn vecf32_operator_dot(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> f32 {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    F32Dot::distance(&lhs, &rhs).to_f32()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf32"])]
#[pgrx::opname(<->)]
#[pgrx::commutator(<->)]
fn vecf32_operator_l2(lhs: Vecf32Input<'_>, rhs: Vecf32Input<'_>) -> f32 {
    if lhs.len() != rhs.len() {
        FriendlyError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    F32L2::distance(&lhs, &rhs).to_f32()
}
