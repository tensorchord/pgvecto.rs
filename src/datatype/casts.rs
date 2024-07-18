use crate::datatype::memory_bvecf32::{BVecf32Input, BVecf32Output};
use crate::datatype::memory_svecf32::{SVecf32Input, SVecf32Output};
use crate::datatype::memory_vecf16::{Vecf16Input, Vecf16Output};
use crate::datatype::memory_vecf32::{Vecf32Input, Vecf32Output};
use crate::datatype::memory_veci8::{Veci8Input, Veci8Output};
use crate::error::*;
use base::scalar::*;
use base::vector::*;
use num_traits::Zero;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_array_to_vecf32(
    array: pgrx::Array<f32>,
    typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let _ = typmod;
    let n = u32::try_from(array.len()).expect("array is too large");
    check_value_dims_65535(n);
    let mut slice = vec![F32::zero(); n as _];
    for (i, x) in array.iter().enumerate() {
        slice[i] = F32(x.unwrap_or(f32::NAN));
    }
    Vecf32Output::new(Vecf32Borrowed::new(&slice))
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
    let slice: Vec<F16> = vector.slice().iter().map(|&x| F16::from_f(x)).collect();
    Vecf16Output::new(Vecf16Borrowed::new(&slice))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_vecf16_to_vecf32(
    vector: Vecf16Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let slice: Vec<F32> = vector.slice().iter().map(|&x| x.to_f()).collect();
    Vecf32Output::new(Vecf32Borrowed::new(&slice))
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
        if !x.is_zero() {
            indexes.push(i);
            values.push(x);
        }
    }
    SVecf32Output::new(SVecf32Borrowed::new(vector.dims(), &indexes, &values))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_svecf32_to_vecf32(
    vector: SVecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let slice = vector.as_borrowed().to_vec();
    Vecf32Output::new(Vecf32Borrowed::new(&slice))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_vecf32_to_bvecf32(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> BVecf32Output {
    let n = vector.dims();
    let mut data = vec![0_u64; n.div_ceil(BVECF32_WIDTH) as _];
    for i in 0..n {
        let x = vector.slice()[i as usize];
        match x.to_f32() {
            x if x == 0.0 => (),
            x if x == 1.0 => data[(i / BVECF32_WIDTH) as usize] |= 1 << (i % BVECF32_WIDTH),
            _ => bad_literal("The vector contains a non-binary value."),
        }
    }
    BVecf32Output::new(BVecf32Borrowed::new(n as _, &data))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_bvecf32_to_vecf32(
    vector: BVecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let data: Vec<F32> = vector
        .as_borrowed()
        .iter()
        .map(|x| F32(x as u32 as f32))
        .collect();
    Vecf32Output::new(Vecf32Borrowed::new(&data))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_veci8_to_vecf32(
    vector: Veci8Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let data = (0..vector.dims())
        .map(|i| vector.get(i))
        .collect::<Vec<_>>();
    Vecf32Output::new(Vecf32Borrowed::new(&data))
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_cast_vecf32_to_veci8(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Veci8Output {
    let (data, alpha, offset) = veci8::i8_quantization(vector.slice());
    Veci8Output::new(Veci8Borrowed::new(&data, alpha, offset))
}
