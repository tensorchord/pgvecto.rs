use super::memory_svecf32::SVecf32Output;
use crate::prelude::*;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_to_svector(
    dims: i32,
    index: pgrx::Array<i32>,
    value: pgrx::Array<f32>,
) -> SVecf32Output {
    let dims = check_value_dims_1048575(dims as usize);
    if index.len() != value.len() {
        bad_literal("Lengths of index and value are not matched.");
    }
    if index.contains_nulls() || value.contains_nulls() {
        bad_literal("Index or value contains nulls.");
    }
    let mut vector: Vec<(u32, F32)> = index
        .iter_deny_null()
        .zip(value.iter_deny_null())
        .map(|(index, value)| {
            if index < 0 || index >= dims.get() as i32 {
                bad_literal("Index out of bound.");
            }
            (index as u32, F32(value))
        })
        .collect();
    vector.sort_unstable_by_key(|x| x.0);
    if vector.len() > 1 {
        for i in 0..vector.len() - 1 {
            if vector[i].0 == vector[i + 1].0 {
                bad_literal("Duplicated index.");
            }
        }
    }

    let mut indexes = Vec::<u32>::with_capacity(vector.len());
    let mut values = Vec::<F32>::with_capacity(vector.len());
    for x in vector {
        indexes.push(x.0);
        values.push(x.1);
    }
    SVecf32Output::new(SVecf32Borrowed::new(dims.get(), &indexes, &values))
}
