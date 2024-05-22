use crate::datatype::memory_vecf32::*;
use base::scalar::*;
use base::vector::*;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_dims(vector: Vecf32Input<'_>) -> i32 {
    vector.for_borrow().dims() as i32
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_norm(vector: Vecf32Input<'_>) -> f32 {
    vector.for_borrow().length().to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_normalize(vector: Vecf32Input<'_>) -> Vecf32Output {
    Vecf32Output::new(vector.for_borrow().normalize().for_borrow())
}
