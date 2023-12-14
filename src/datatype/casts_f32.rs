use crate::datatype::typmod::Typmod;
use crate::datatype::vecf32::{Vecf32, Vecf32Input, Vecf32Output};
use service::prelude::*;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vecf32_cast_array_to_vector(
    array: pgrx::Array<f32>,
    typmod: i32,
    _explicit: bool,
) -> Vecf32Output {
    assert!(!array.is_empty());
    assert!(array.len() <= 65535);
    assert!(!array.contains_nulls());
    let typmod = Typmod::parse_from_i32(typmod).unwrap();
    let len = typmod.dims().unwrap_or(array.len() as u16);
    let mut data = vec![F32::zero(); len as usize];
    for (i, x) in array.iter().enumerate() {
        data[i] = F32(x.unwrap_or(f32::NAN));
    }
    Vecf32::new_in_postgres(&data)
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vecf32_cast_vector_to_array(vector: Vecf32Input<'_>, _typmod: i32, _explicit: bool) -> Vec<f32> {
    vector.data().iter().map(|x| x.to_f32()).collect()
}
