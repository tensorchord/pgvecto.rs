use crate::datatype::veci8::Veci8Input;
use crate::prelude::*;
use service::prelude::*;

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_cosine(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    cosine_distance(&lhs.to_ref(), &rhs.to_ref()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_dot(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    dot_distance(&lhs.to_ref(), &rhs.to_ref()).to_f32()
}

#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_veci8_operator_l2(lhs: Veci8Input<'_>, rhs: Veci8Input<'_>) -> f32 {
    check_matched_dimensions(lhs.len(), rhs.len());
    l2_distance(&lhs.to_ref(), &rhs.to_ref()).to_f32()
}
