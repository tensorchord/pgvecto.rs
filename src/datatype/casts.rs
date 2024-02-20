use crate::datatype::memory_svecf32::{SVecf32Input, SVecf32Output};
use crate::datatype::memory_vecf16::{Vecf16Input, Vecf16Output};
use crate::datatype::memory_vecf32::{Vecf32Input, Vecf32Output};
use crate::prelude::*;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_array_to_vecf32(
    array: pgrx::Array<f32>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    check_value_dims(array.len());
    let mut slice = vec![F32::zero(); array.len()];
    for (i, x) in array.iter().enumerate() {
        slice[i] = F32(x.unwrap_or(f32::NAN));
    }
    Vecf32Output::new(Vecf32Borrowed::new(&slice))
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_vecf32_to_array(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vec<f32> {
    vector.slice().iter().map(|x| x.to_f32()).collect()
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_vecf32_to_vecf16(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf16Output {
    let slice: Vec<F16> = vector.slice().iter().map(|&x| F16::from_f(x)).collect();

    Vecf16Output::new(Vecf16Borrowed::new(&slice))
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_vecf16_to_vecf32(
    vector: Vecf16Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let slice: Vec<F32> = vector.slice().iter().map(|&x| x.to_f()).collect();

    Vecf32Output::new(Vecf32Borrowed::new(&slice))
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_vecf32_to_svecf32(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> SVecf32Output {
    let mut indexes = Vec::new();
    let mut values = Vec::new();
    vector
        .slice()
        .iter()
        .enumerate()
        .filter(|(_, x)| !x.is_zero())
        .for_each(|(i, &x)| {
            indexes.push(i as u16);
            values.push(x);
        });

    SVecf32Output::new(SVecf32Borrowed::new(
        vector.dims() as u16,
        &indexes,
        &values,
    ))
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_svecf32_to_vecf32(
    vector: SVecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let slice = vector.for_borrow().to_vec();

    Vecf32Output::new(Vecf32Borrowed::new(&slice))
}
