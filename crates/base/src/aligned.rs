#[derive(Debug, Clone, Copy)]
#[repr(C, align(16))]
pub struct Aligned16<T>(pub T);

#[derive(Debug, Clone, Copy)]
#[repr(C, align(32))]
pub struct Aligned32<T>(pub T);
