use base::scalar::F32;
use base::search::*;
use common::always_equal::AlwaysEqual;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct WindowFlatReranker<T, R> {
    rerank: R,
    size: u32,
    heap: BinaryHeap<(Reverse<F32>, u32)>,
    cache: BinaryHeap<(Reverse<F32>, u32, AlwaysEqual<T>)>,
}

impl<T, R> WindowFlatReranker<T, R>
where
    R: Fn(u32) -> (F32, T),
{
    pub fn new(heap: Vec<(Reverse<F32>, u32)>, rerank: R, size: u32) -> Self {
        Self {
            heap: heap.into(),
            rerank,
            size: size.max(1),
            cache: BinaryHeap::new(),
        }
    }
}

impl<T, R> RerankerPop<T> for WindowFlatReranker<T, R>
where
    R: Fn(u32) -> (F32, T),
{
    fn pop(&mut self) -> Option<(F32, u32, T)> {
        while !self.heap.is_empty() && self.cache.len() < self.size as _ {
            let (_, u) = self.heap.pop().unwrap();
            let (accu_u, t) = (self.rerank)(u);
            self.cache.push((Reverse(accu_u), u, AlwaysEqual(t)));
        }
        let (Reverse(accu_u), u, AlwaysEqual(t)) = self.cache.pop()?;
        Some((accu_u, u, t))
    }
}
