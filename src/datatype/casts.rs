use crate::datatype::memory_bvector::{BVectorInput, BVectorOutput};
use crate::datatype::memory_svecf32::{SVecf32Input, SVecf32Output};
use crate::datatype::memory_vecf16::{Vecf16Input, Vecf16Output};
use crate::datatype::memory_vecf32::{Vecf32Input, Vecf32Output};
use crate::error::*;
use base::simd::*;
use base::vector::*;
use half::f16;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_array_to_vecf32(
    array: pgrx::datum::Array<f32>,
    typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let _ = typmod;
    let n = u32::try_from(array.len()).expect("array is too large");
    check_value_dims_65535(n);
    let mut slice = vec![0.0f32; n as _];
    for (i, x) in array.iter().enumerate() {
        slice[i] = x.unwrap_or(f32::NAN);
    }
    Vecf32Output::new(VectBorrowed::new(&slice))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_vecf32_to_array(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vec<f32> {
    vector.slice().iter().map(|x| x.to_f32()).collect()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_vecf32_to_vecf16(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf16Output {
    let slice: Vec<f16> = f16::vector_from_f32(vector.slice());
    Vecf16Output::new(VectBorrowed::new(&slice))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_vecf16_to_vecf32(
    vector: Vecf16Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let slice: Vec<f32> = f16::vector_to_f32(vector.slice());
    Vecf32Output::new(VectBorrowed::new(&slice))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_vecf32_to_svecf32(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> SVecf32Output {
    let mut indexes = Vec::new();
    let mut values = Vec::new();
    for i in 0..vector.dims() {
        let x = vector.slice()[i as usize];
        if x != 0.0 {
            indexes.push(i);
            values.push(x);
        }
    }
    SVecf32Output::new(SVectBorrowed::new(vector.dims(), &indexes, &values))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_svecf32_to_vecf32(
    vector: SVecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let vector = vector.as_borrowed();
    check_value_dims_65535(vector.dims());
    let mut slice = vec![0.0f32; vector.dims() as _];
    for i in 0..vector.len() {
        let (index, value) = (vector.indexes()[i as usize], vector.values()[i as usize]);
        slice[index as usize] = value;
    }
    Vecf32Output::new(VectBorrowed::new(&slice))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_vecf32_to_bvector(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> BVectorOutput {
    let n = vector.dims();
    let mut data = vec![0_u64; n.div_ceil(BVECTOR_WIDTH) as _];
    for i in 0..n {
        let x = vector.slice()[i as usize];
        match x.to_f32() {
            x if x == 0.0 => (),
            x if x == 1.0 => data[(i / BVECTOR_WIDTH) as usize] |= 1 << (i % BVECTOR_WIDTH),
            _ => bad_literal("The vector contains a non-binary value."),
        }
    }
    BVectorOutput::new(BVectBorrowed::new(n as _, &data))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_bvector_to_vecf32(
    vector: BVectorInput<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let data: Vec<f32> = vector
        .as_borrowed()
        .iter()
        .map(|x| x as u32 as f32)
        .collect();
    Vecf32Output::new(VectBorrowed::new(&data))
}
