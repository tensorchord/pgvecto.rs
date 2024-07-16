use base::scalar::F32;
use base::search::Reranker;
use common::always_equal::AlwaysEqual;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct WindowReranker<T, C, R> {
    compute: C,
    rerank: R,
    size: u32,
    heap: BinaryHeap<(Reverse<F32>, u32)>,
    cache: BinaryHeap<(Reverse<F32>, u32, AlwaysEqual<T>)>,
}

impl<T, C, R> WindowReranker<T, C, R> {
    pub fn new(size: u32, compute: C, rerank: R) -> Self {
        Self {
            compute,
            rerank,
            size: size.max(1),
            heap: BinaryHeap::new(),
            cache: BinaryHeap::new(),
        }
    }
}

impl<T, C, R, E> Reranker<T, E> for WindowReranker<T, C, R>
where
    C: Fn(u32, E) -> F32,
    R: Fn(u32) -> (F32, T),
    E: 'static,
{
    fn push(&mut self, u: u32, extra: E) {
        let rough_u = (self.compute)(u, extra);
        self.heap.push((Reverse(rough_u), u));
    }

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
