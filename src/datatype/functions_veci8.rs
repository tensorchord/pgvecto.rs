use crate::datatype::memory_veci8::*;
use crate::error::*;
use base::scalar::*;
use base::vector::*;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_dims(vector: Veci8Input<'_>) -> i32 {
    vector.as_borrowed().dims() as i32
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_norm(vector: Veci8Input<'_>) -> f32 {
    vector.as_borrowed().length().to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_veci8_normalize(vector: Veci8Input<'_>) -> Veci8Output {
    Veci8Output::new(vector.as_borrowed().function_normalize().as_borrowed())
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_to_veci8(len: i32, alpha: f32, offset: f32, values: pgrx::Array<i32>) -> Veci8Output {
    check_value_dims_65535(len as u32);
    if (len as usize) != values.len() {
        bad_literal("Lengths of values and len are not matched.");
    }
    if values.contains_nulls() {
        bad_literal("Index or value contains nulls.");
    }
    let data = values
        .iter()
        .map(|x| I8(x.unwrap() as i8))
        .collect::<Vec<_>>();
    let alpha = F32(alpha);
    let offset = F32(offset);
    Veci8Output::new(Veci8Borrowed::new(&data, alpha, offset))
}
