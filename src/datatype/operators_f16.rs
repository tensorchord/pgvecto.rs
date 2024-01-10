use crate::datatype::vecf16::{Vecf16, Vecf16Input, Vecf16Output};
use crate::prelude::*;
use service::prelude::*;
use std::ops::Deref;

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(+)]
#[pgrx::commutator(+)]
fn vecf16_operator_add(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> Vecf16Output {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    let n = lhs.len();
    let mut v = vec![F16::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] + rhs[i];
    }
    Vecf16::new_in_postgres(&v)
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(-)]
fn vecf16_operator_minus(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> Vecf16Output {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    let n = lhs.len();
    let mut v = vec![F16::zero(); n];
    for i in 0..n {
        v[i] = lhs[i] - rhs[i];
    }
    Vecf16::new_in_postgres(&v)
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(<)]
#[pgrx::negator(>=)]
#[pgrx::commutator(>)]
#[pgrx::restrict(scalarltsel)]
#[pgrx::join(scalarltjoinsel)]
fn vecf16_operator_lt(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() < rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(<=)]
#[pgrx::negator(>)]
#[pgrx::commutator(>=)]
#[pgrx::restrict(scalarltsel)]
#[pgrx::join(scalarltjoinsel)]
fn vecf16_operator_lte(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() <= rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(>)]
#[pgrx::negator(<=)]
#[pgrx::commutator(<)]
#[pgrx::restrict(scalargtsel)]
#[pgrx::join(scalargtjoinsel)]
fn vecf16_operator_gt(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() > rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(>=)]
#[pgrx::negator(<)]
#[pgrx::commutator(<=)]
#[pgrx::restrict(scalargtsel)]
#[pgrx::join(scalargtjoinsel)]
fn vecf16_operator_gte(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() >= rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(=)]
#[pgrx::negator(<>)]
#[pgrx::commutator(=)]
#[pgrx::restrict(eqsel)]
#[pgrx::join(eqjoinsel)]
fn vecf16_operator_eq(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() == rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(<>)]
#[pgrx::negator(=)]
#[pgrx::commutator(<>)]
#[pgrx::restrict(eqsel)]
#[pgrx::join(eqjoinsel)]
fn vecf16_operator_neq(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> bool {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() != rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(<=>)]
#[pgrx::commutator(<=>)]
fn vecf16_operator_cosine(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> f32 {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    F16Cos::distance(&lhs, &rhs).to_f32()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(<#>)]
#[pgrx::commutator(<#>)]
fn vecf16_operator_dot(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> f32 {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    F16Dot::distance(&lhs, &rhs).to_f32()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vecf16"])]
#[pgrx::opname(<->)]
#[pgrx::commutator(<->)]
fn vecf16_operator_l2(lhs: Vecf16Input<'_>, rhs: Vecf16Input<'_>) -> f32 {
    if lhs.len() != rhs.len() {
        SessionError::Unmatched {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    F16L2::distance(&lhs, &rhs).to_f32()
}
