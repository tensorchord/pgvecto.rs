#[repr(C, align(64))]
pub struct AlignedArray<T, const N: usize>(pub [T; N]);
