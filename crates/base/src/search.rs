use crate::operator::{Borrowed, Operator};
use crate::scalar::F32;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Handle {
    newtype: u128,
}

impl Handle {
    pub fn new(newtype: u128) -> Self {
        Self { newtype }
    }
    pub fn as_u128(self) -> u128 {
        self.newtype
    }
}

impl Display for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}", self.as_u128())
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Pointer {
    newtype: u64,
}

impl Pointer {
    pub fn new(value: u64) -> Self {
        Self { newtype: value }
    }
    pub fn as_u64(self) -> u64 {
        self.newtype
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(C)]
pub struct Payload {
    pointer: Pointer,
    time: u64,
}

impl Payload {
    pub fn new(pointer: Pointer, time: u64) -> Self {
        Self { pointer, time }
    }
    pub fn pointer(&self) -> Pointer {
        self.pointer
    }
    pub fn time(&self) -> u64 {
        self.time
    }
}

unsafe impl bytemuck::Zeroable for Payload {}
unsafe impl bytemuck::Pod for Payload {}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Element {
    pub distance: F32,
    pub payload: Payload,
}

pub trait Filter: Clone {
    fn check(&mut self, payload: Payload) -> bool;
}

pub trait Collection<O: Operator> {
    fn dims(&self) -> u32;
    fn len(&self) -> u32;
    fn vector(&self, i: u32) -> Borrowed<'_, O>;
    fn payload(&self, i: u32) -> Payload;
}

pub trait Source<O: Operator>: Collection<O> {
    // ..
}
