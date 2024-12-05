use crate::datatype::memory_vecf32::*;
use base::simd::*;
use base::vector::*;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_dims(vector: Vecf32Input<'_>) -> i32 {
    vector.as_borrowed().dims() as i32
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_norm(vector: Vecf32Input<'_>) -> f32 {
    vector.as_borrowed().norm().to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_normalize(vector: Vecf32Input<'_>) -> Vecf32Output {
    Vecf32Output::new(vector.as_borrowed().function_normalize().as_borrowed())
}
