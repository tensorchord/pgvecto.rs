use super::datatype::{Vector, VectorInput, VectorOutput, VectorTypmod};
use crate::prelude::Scalar;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn cast_array_to_vector(array: pgrx::Array<Scalar>, typmod: i32, _explicit: bool) -> VectorOutput {
    assert!(!array.is_empty());
    assert!(array.len() <= 65535);
    assert!(!array.contains_nulls());
    let typmod = VectorTypmod::parse_from_i32(typmod).unwrap();
    let len = typmod.dims().unwrap_or(array.len() as u16);
    let mut data = Vector::new_zeroed_in_postgres(len as usize);
    for (i, x) in array.iter().enumerate() {
        data[i] = x.unwrap_or(Scalar::NAN);
    }
    data
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn cast_vector_to_array(vector: VectorInput<'_>, _typmod: i32, _explicit: bool) -> Vec<Scalar> {
    vector.data().to_vec()
}
