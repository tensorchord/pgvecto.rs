use crate::scalar::F32;

pub type Payload = u64;

pub trait Filter: Clone {
    fn check(&mut self, payload: Payload) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Element {
    pub distance: F32,
    pub payload: Payload,
}
