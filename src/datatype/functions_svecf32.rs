use super::memory_svecf32::*;
use crate::error::*;
use base::scalar::*;
use base::vector::*;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_dims(vector: SVecf32Input<'_>) -> i32 {
    vector.as_borrowed().dims() as i32
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_norm(vector: SVecf32Input<'_>) -> f32 {
    vector.as_borrowed().norm().to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_normalize(vector: SVecf32Input<'_>) -> SVecf32Output {
    SVecf32Output::new(vector.as_borrowed().function_normalize().as_borrowed())
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_to_svector(
    dims: i32,
    index: pgrx::datum::Array<i32>,
    value: pgrx::datum::Array<f32>,
) -> SVecf32Output {
    let dims = dims as u32;
    check_value_dims_1048575(dims);
    if index.len() != value.len() {
        bad_literal("Lengths of index and value are not matched.");
    }
    if index.contains_nulls() || value.contains_nulls() {
        bad_literal("Index or value contains nulls.");
    }
    let mut vector: Vec<(u32, f32)> = index
        .iter_deny_null()
        .zip(value.iter_deny_null())
        .map(|(index, value)| {
            if index < 0 || index as u32 >= dims {
                bad_literal("Index out of bound.");
            }
            (index as u32, value)
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
    let mut values = Vec::<f32>::with_capacity(vector.len());
    for x in vector {
        indexes.push(x.0);
        values.push(x.1);
    }
    SVecf32Output::new(SVectBorrowed::new(dims, &indexes, &values))
}
