use crate::prelude::{Element, F32};
use std::{cmp::Reverse, collections::BinaryHeap};

pub struct ElementHeap {
    binary_heap: BinaryHeap<Element>,
    k: usize,
}

impl ElementHeap {
    pub fn new(k: usize) -> Self {
        assert!(k != 0);
        Self {
            binary_heap: BinaryHeap::new(),
            k,
        }
    }
    pub fn check(&self, distance: F32) -> bool {
        self.binary_heap.len() < self.k || distance < self.binary_heap.peek().unwrap().distance
    }
    pub fn push(&mut self, element: Element) -> Option<Element> {
        self.binary_heap.push(element);
        if self.binary_heap.len() == self.k + 1 {
            self.binary_heap.pop()
        } else {
            None
        }
    }
    pub fn into_reversed_heap(self) -> BinaryHeap<Reverse<Element>> {
        self.binary_heap.into_iter().map(Reverse).collect()
    }
    pub fn into_sorted_vec(self) -> Vec<Element> {
        self.binary_heap.into_sorted_vec()
    }
}
