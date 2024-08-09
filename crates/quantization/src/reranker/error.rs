use base::scalar::F32;
use base::search::RerankerPop;
use common::always_equal::AlwaysEqual;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct ErrorFlatReranker<T, R> {
    rerank: R,
    heap: BinaryHeap<(Reverse<F32>, u32)>,
    cache: BinaryHeap<(Reverse<F32>, u32, AlwaysEqual<T>)>,
}

impl<T, R> ErrorFlatReranker<T, R> {
    pub fn new(heap: Vec<(Reverse<F32>, u32)>, rerank: R) -> Self {
        Self {
            rerank,
            heap: heap.into(),
            cache: BinaryHeap::new(),
        }
    }
}

impl<T, R> RerankerPop<T> for ErrorFlatReranker<T, R>
where
    R: Fn(u32) -> (F32, T),
{
    fn pop(&mut self) -> Option<(F32, u32, T)> {
        while !self.heap.is_empty()
            && (self.cache.is_empty() || self.heap.peek().unwrap().0 > self.cache.peek().unwrap().0)
        {
            let (_, u) = self.heap.pop().unwrap();
            let (accu_u, t) = (self.rerank)(u);
            self.cache.push((Reverse(accu_u), u, AlwaysEqual(t)));
        }
        let (Reverse(accu_u), u, AlwaysEqual(t)) = self.cache.pop()?;
        Some((accu_u, u, t))
    }
}
