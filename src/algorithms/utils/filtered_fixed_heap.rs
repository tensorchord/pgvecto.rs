use crate::prelude::*;
use std::collections::BinaryHeap;

type T = (Scalar, u64);

#[derive(Debug, Clone)]
pub struct FilteredFixedHeap<F> {
    size: usize,
    heap: BinaryHeap<T>,
    f: F,
}

impl<F> FilteredFixedHeap<F>
where
    F: FnMut(u64) -> bool,
{
    pub fn new(size: usize, f: F) -> Self {
        Self {
            size,
            heap: BinaryHeap::<T>::with_capacity(size),
            f,
        }
    }
    pub fn push(&mut self, item: T) {
        if self.heap.len() < self.size {
            if (self.f)(item.1) {
                self.heap.push(item);
            }
        } else if self.heap.peek().unwrap() > &item {
            if (self.f)(item.1) {
                self.heap.pop();
                self.heap.push(item);
            }
        }
    }
    pub fn bound(&mut self) -> Scalar {
        if self.heap.len() < self.size {
            Scalar::INFINITY
        } else {
            self.heap.peek().unwrap().0
        }
    }
    pub fn into_sorted_vec(self) -> Vec<T> {
        let mut vec = self.heap.into_vec();
        vec.sort();
        vec
    }
}
