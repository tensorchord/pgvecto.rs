#[derive(Debug, Clone, Copy)]
#[repr(C, align(32))]
pub struct Aligned32<T>(pub T);
