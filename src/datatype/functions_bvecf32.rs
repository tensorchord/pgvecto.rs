use super::memory_bvecf32::BVecf32Output;
use super::memory_vecf32::Vecf32Input;
use crate::prelude::*;

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_binarize(vector: Vecf32Input<'_>) -> BVecf32Output {
    let mut values = BVecf32Owned::new_zeroed(vector.len() as u16);
    for (i, &F32(x)) in vector.slice().iter().enumerate() {
        if x > 0. {
            values.set(i, true);
        }
    }

    BVecf32Output::new(values.for_borrow())
}
