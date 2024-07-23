use crate::datatype::memory_bvecf32::*;
use crate::datatype::memory_vecf32::*;
use base::scalar::*;
use base::vector::*;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_dims(vector: BVecf32Input<'_>) -> i32 {
    vector.as_borrowed().dims() as i32
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvecf32_norm(vector: BVecf32Input<'_>) -> f32 {
    vector.as_borrowed().length().to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_binarize(vector: Vecf32Input<'_>) -> BVecf32Output {
    let n = vector.dims();
    let mut data = vec![0_u64; n.div_ceil(BVECF32_WIDTH) as _];
    for i in 0..n {
        let x = vector.slice()[i as usize];
        if x > F32(0.0) {
            data[(i / BVECF32_WIDTH) as usize] |= 1 << (i % BVECF32_WIDTH);
        }
    }
    BVecf32Output::new(BVecf32Borrowed::new(n as _, &data))
}
