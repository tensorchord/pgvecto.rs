use crate::datatype::svecf32::{SVecf32, SVecf32Input, SVecf32Output};
use crate::datatype::vecf16::{Vecf16, Vecf16Input, Vecf16Output};
use crate::datatype::vecf32::{Vecf32, Vecf32Input, Vecf32Output};
use crate::prelude::check_value_dimensions;
use base::scalar::FloatCast;
use service::prelude::*;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_array_to_vecf32(
    array: pgrx::Array<f32>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    check_value_dimensions(array.len());
    let mut data = vec![F32::zero(); array.len()];
    for (i, x) in array.iter().enumerate() {
        data[i] = F32(x.unwrap_or(f32::NAN));
    }
    Vecf32::new_in_postgres(&data)
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_vecf32_to_array(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vec<f32> {
    vector.data().iter().map(|x| x.to_f32()).collect()
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_vecf32_to_vecf16(
    vector: Vecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf16Output {
    let data: Vec<F16> = vector.data().iter().map(|&x| F16::from_f(x)).collect();

    Vecf16::new_in_postgres(&data)
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_vecf16_to_vecf32(
    vector: Vecf16Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let data: Vec<F32> = vector.data().iter().map(|&x| x.to_f()).collect();

    Vecf32::new_in_postgres(&data)
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
        .data()
        .iter()
        .enumerate()
        .filter(|(_, x)| !x.is_zero())
        .for_each(|(i, &x)| {
            indexes.push(i as u16);
            values.push(x);
        });

    SVecf32::new_in_postgres(SparseF32Ref {
        dims: vector.len() as u16,
        indexes: &indexes,
        values: &values,
    })
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_svecf32_to_vecf32(
    vector: SVecf32Input<'_>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    let data = vector.data().to_dense();
    Vecf32::new_in_postgres(&data)
}
