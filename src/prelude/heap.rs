use crate::prelude::Scalar;
use std::{cmp::Reverse, collections::BinaryHeap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct HeapElement {
    pub distance: Scalar,
    pub data: u64,
}

pub struct Heap {
    binary_heap: BinaryHeap<HeapElement>,
    k: usize,
}

impl Heap {
    pub fn new(k: usize) -> Self {
        assert!(k != 0);
        Self {
            binary_heap: BinaryHeap::new(),
            k,
        }
    }
    pub fn check(&self, distance: Scalar) -> bool {
        self.binary_heap.len() < self.k || distance < self.binary_heap.peek().unwrap().distance
    }
    pub fn push(&mut self, element: HeapElement) {
        self.binary_heap.push(element);
        if self.binary_heap.len() == self.k + 1 {
            self.binary_heap.pop();
        }
    }
    pub fn into_reversed_heap(self) -> BinaryHeap<Reverse<HeapElement>> {
        self.binary_heap.into_iter().map(Reverse).collect()
    }
    pub fn into_sorted_vec(self) -> Vec<HeapElement> {
        self.binary_heap.into_sorted_vec()
    }
}
