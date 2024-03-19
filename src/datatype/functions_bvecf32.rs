use super::memory_bvecf32::BVecf32Output;
use super::memory_vecf32::Vecf32Input;
use base::scalar::*;
use base::vector::*;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_binarize(vector: Vecf32Input<'_>) -> BVecf32Output {
    let mut values = BVecf32Owned::new_zeroed(vector.len() as u16);
    for (i, &F32(x)) in vector.slice().iter().enumerate() {
        if x > 0. {
            values.set(i, true);
        }
    }
    BVecf32Output::new(values.for_borrow())
}
