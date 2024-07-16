use crate::datatype::memory_vecf16::*;
use base::scalar::*;
use base::vector::*;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_dims(vector: Vecf16Input<'_>) -> i32 {
    vector.as_borrowed().dims() as i32
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_norm(vector: Vecf16Input<'_>) -> f32 {
    vector.as_borrowed().length().to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf16_normalize(vector: Vecf16Input<'_>) -> Vecf16Output {
    Vecf16Output::new(vector.as_borrowed().function_normalize().as_borrowed())
}
