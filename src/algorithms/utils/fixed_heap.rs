use std::collections::BinaryHeap;

#[derive(Debug, Clone)]
pub struct FixedHeap<T> {
    size: usize,
    heap: BinaryHeap<T>,
}

impl<T: Ord> FixedHeap<T> {
    pub fn new(size: usize) -> Self {
        Self {
            size,
            heap: BinaryHeap::<T>::with_capacity(1 + size),
        }
    }
    pub fn push(&mut self, item: T) {
        self.heap.push(item);
        if self.heap.len() > self.size {
            self.heap.pop();
        }
    }
    pub fn into_vec(self) -> Vec<T> {
        self.heap.into_vec()
    }
}
