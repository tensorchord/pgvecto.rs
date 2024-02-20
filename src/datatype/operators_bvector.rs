use crate::{
    datatype::bvector::{BVector, BVectorInput, BVectorOutput},
    prelude::{FriendlyError, SessionError},
};
use service::prelude::*;
use std::ops::Deref;

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_and(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> BVectorOutput {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    let results = lhs.data().values.to_bitvec() & rhs.data().values;
    BVector::new_in_postgres(BinaryVecRef { values: &results })
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_or(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> BVectorOutput {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    let results = lhs.data().values.to_bitvec() | rhs.data().values;
    BVector::new_in_postgres(BinaryVecRef { values: &results })
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_xor(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> BVectorOutput {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    let results = lhs.data().values.to_bitvec() ^ rhs.data().values;
    BVector::new_in_postgres(BinaryVecRef { values: &results })
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_lt(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
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
fn _vectors_bvector_operator_lte(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
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
fn _vectors_bvector_operator_gt(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
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
fn _vectors_bvector_operator_gte(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
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
fn _vectors_bvector_operator_eq(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
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
fn _vectors_bvector_operator_neq(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> bool {
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
fn _vectors_bvector_operator_cosine(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> f32 {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    BinaryCos::distance(lhs.data(), rhs.data()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_dot(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> f32 {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    BinaryDot::distance(lhs.data(), rhs.data()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_bvector_operator_l2(lhs: BVectorInput<'_>, rhs: BVectorInput<'_>) -> f32 {
    if lhs.dims() != rhs.dims() {
        SessionError::Unmatched {
            left_dimensions: lhs.dims() as _,
            right_dimensions: rhs.dims() as _,
        }
        .friendly();
    }

    BinaryL2::distance(lhs.data(), rhs.data()).to_f32()
}
