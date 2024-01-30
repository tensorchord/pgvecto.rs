use crate::datatype::vecf16::{Vecf16, Vecf16Output};
use crate::datatype::vecf32::{Vecf32, Vecf32Input, Vecf32Output};
use crate::prelude::{FriendlyError, SessionError};
use half::f16;
use service::prelude::*;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_cast_array_to_vecf32(
    array: pgrx::Array<f32>,
    _typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    if array.is_empty() || array.len() > 65535 {
        SessionError::BadValueDimensions.friendly();
    }
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
    let data: Vec<F16> = vector
        .data()
        .iter()
        .map(|x| x.to_f32())
        .map(f16::from_f32)
        .map(F16::from)
        .collect();

    Vecf16::new_in_postgres(&data)
}
