use crate::datatype::memory_bvecf32::*;
use crate::datatype::memory_vecf32::*;
use base::scalar::*;
use base::vector::*;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_dims(vector: BVecf32Input<'_>) -> i32 {
    vector.for_borrow().dims() as i32
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_norm(vector: BVecf32Input<'_>) -> f32 {
    vector.for_borrow().length().to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_binarize(vector: Vecf32Input<'_>) -> BVecf32Output {
    let n = vector.len();
    let mut data = vec![0_usize; n.div_ceil(usize::BITS as _)];
    for (i, &F32(x)) in vector.slice().iter().enumerate() {
        if x > 0.0 {
            data[i / BVEC_WIDTH] |= 1 << (i % BVEC_WIDTH);
        }
    }
    BVecf32Output::new(BVecf32Borrowed::new(n as _, &data))
}
