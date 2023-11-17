use crate::postgres::datatype::{Vector, VectorInput, VectorOutput};
use crate::prelude::*;
use std::ops::Deref;

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(+)]
#[pgrx::commutator(+)]
fn operator_add(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> VectorOutput {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    let n = lhs.len();
    let mut v = Vector::new_zeroed(n);
    for i in 0..n {
        v[i] = lhs[i] + rhs[i];
    }
    v.copy_into_postgres()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(-)]
fn operator_minus(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> VectorOutput {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    let n = lhs.len();
    let mut v = Vector::new_zeroed(n);
    for i in 0..n {
        v[i] = lhs[i] - rhs[i];
    }
    v.copy_into_postgres()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<)]
#[pgrx::negator(>=)]
#[pgrx::commutator(>)]
#[pgrx::restrict(scalarltsel)]
#[pgrx::join(scalarltjoinsel)]
fn operator_lt(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() < rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<=)]
#[pgrx::negator(>)]
#[pgrx::commutator(>=)]
#[pgrx::restrict(scalarltsel)]
#[pgrx::join(scalarltjoinsel)]
fn operator_lte(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() <= rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(>)]
#[pgrx::negator(<=)]
#[pgrx::commutator(<)]
#[pgrx::restrict(scalargtsel)]
#[pgrx::join(scalargtjoinsel)]
fn operator_gt(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() > rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(>=)]
#[pgrx::negator(<)]
#[pgrx::commutator(<=)]
#[pgrx::restrict(scalargtsel)]
#[pgrx::join(scalargtjoinsel)]
fn operator_gte(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() >= rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(=)]
#[pgrx::negator(<>)]
#[pgrx::commutator(=)]
#[pgrx::restrict(eqsel)]
#[pgrx::join(eqjoinsel)]
fn operator_eq(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() == rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<>)]
#[pgrx::negator(=)]
#[pgrx::commutator(<>)]
#[pgrx::restrict(eqsel)]
#[pgrx::join(eqjoinsel)]
fn operator_neq(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> bool {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    lhs.deref() != rhs.deref()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<=>)]
#[pgrx::commutator(<=>)]
fn operator_cosine(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> Scalar {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    Distance::Cosine.distance(&lhs, &rhs)
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<#>)]
#[pgrx::commutator(<#>)]
fn operator_dot(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> Scalar {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    Distance::Dot.distance(&lhs, &rhs)
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<->)]
#[pgrx::commutator(<->)]
fn operator_l2(lhs: VectorInput<'_>, rhs: VectorInput<'_>) -> Scalar {
    if lhs.len() != rhs.len() {
        FriendlyError::DifferentVectorDims {
            left_dimensions: lhs.len() as _,
            right_dimensions: rhs.len() as _,
        }
        .friendly();
    }
    Distance::L2.distance(&lhs, &rhs)
}
