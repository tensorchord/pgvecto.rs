use super::datatype::{Vector, VectorInput, VectorOutput, VectorTypmod};
use crate::prelude::Scalar;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn cast_array_to_vector(array: pgrx::Array<Scalar>, typmod: i32, _explicit: bool) -> VectorOutput {
    assert!(!array.is_empty());
    assert!(!array.contains_nulls());
    let typmod = VectorTypmod::parse_from_i32(typmod).unwrap();
    let mut data = Vec::with_capacity(typmod.dims().unwrap_or_default() as usize);
    for x in array.iter_deny_null() {
        data.push(x);
    }
    Vector::new_in_postgres(&data)
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn cast_vector_to_array<'a>(vector: VectorInput<'a>, _typmod: i32, _explicit: bool) -> Vec<Scalar> {
    vector.data().to_vec()
}
