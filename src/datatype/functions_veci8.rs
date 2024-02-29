use crate::datatype::memory_veci8::Veci8Output;
use crate::prelude::*;
use base::vector::Veci8Borrowed;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_to_veci8(len: i32, alpha: f32, offset: f32, values: pgrx::Array<i32>) -> Veci8Output {
    check_value_dims(len as usize);
    if (len as usize) != values.len() {
        bad_literal("Lengths of values and len are not matched.");
    }
    if values.contains_nulls() {
        bad_literal("Index or value contains nulls.");
    }
    let values = values
        .iter()
        .map(|x| I8(x.unwrap() as i8))
        .collect::<Vec<_>>();
    let (sum, l2_norm) = i8_precompute(&values, F32(alpha), F32(offset));
    Veci8Output::new(
        Veci8Borrowed::new_checked(
            values.len() as u16,
            &values,
            F32(alpha),
            F32(offset),
            sum,
            l2_norm,
        )
        .unwrap(),
    )
}
