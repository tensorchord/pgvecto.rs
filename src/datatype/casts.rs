use crate::datatype::memory_bvecf32::{BVecf32Input, BVecf32Output};
use crate::datatype::memory_svecf32::{SVecf32Input, SVecf32Output};
use crate::datatype::memory_vecf16::{Vecf16Input, Vecf16Output};
use crate::datatype::memory_vecf32::{Vecf32Input, Vecf32Output};
use crate::datatype::memory_veci8::{Veci8Input, Veci8Output};
use crate::prelude::*;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_array_to_vecf32(
    array: pgrx::Array<f32>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    check_value_dims_65535(array.len());
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
            indexes.push(i as u32);
            values.push(x);
        });

    SVecf32Output::new(SVecf32Borrowed::new(
        vector.dims() as u32,
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

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_vecf32_to_bvecf32(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> BVecf32Output {
    let mut values = BVecf32Owned::new_zeroed(vector.len() as u16);
    for (i, &x) in vector.slice().iter().enumerate() {
        match x.to_f32() {
            x if x == 0. => {}
            x if x == 1. => values.set(i, true),
            _ => bad_literal("The vector contains a non-binary value."),
        }
    }

    BVecf32Output::new(values.for_borrow())
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_bvecf32_to_vecf32(
    vector: BVecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let data: Vec<F32> = vector
        .for_borrow()
        .iter()
        .map(|x| F32(x as u32 as f32))
        .collect();
    Vecf32Output::new(Vecf32Borrowed::new(&data))
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_veci8_to_vecf32(
    vector: Veci8Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let data = (0..vector.len())
        .map(|i| vector.index(i))
        .collect::<Vec<_>>();
    Vecf32Output::new(Vecf32Borrowed::new(&data))
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_vecf32_to_veci8(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Veci8Output {
    let (data, alpha, offset) = i8_quantization(vector.slice());
    let (sum, l2_norm) = i8_precompute(&data, alpha, offset);
    Veci8Output::new(
        Veci8Borrowed::new_checked(data.len() as u32, &data, alpha, offset, sum, l2_norm).unwrap(),
    )
}
