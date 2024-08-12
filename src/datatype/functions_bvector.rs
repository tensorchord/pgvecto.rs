use crate::datatype::memory_bvector::*;
use crate::datatype::memory_vecf32::*;
use base::scalar::*;
use base::vector::*;

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_dims(vector: BVectorInput<'_>) -> i32 {
    vector.as_borrowed().dims() as i32
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_bvector_norm(vector: BVectorInput<'_>) -> f32 {
    vector.as_borrowed().norm().to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_binarize(vector: Vecf32Input<'_>) -> BVectorOutput {
    let n = vector.dims();
    let mut data = vec![0_u64; n.div_ceil(BVECTOR_WIDTH) as _];
    for i in 0..n {
        let x = vector.slice()[i as usize];
        if x > F32(0.0) {
            data[(i / BVECTOR_WIDTH) as usize] |= 1 << (i % BVECTOR_WIDTH);
        }
    }
    BVectorOutput::new(BVectorBorrowed::new(n as _, &data))
}
