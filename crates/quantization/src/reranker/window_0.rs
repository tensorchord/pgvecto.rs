use base::scalar::F32;
use base::search::Reranker;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct Window0Reranker<C, R> {
    compute: C,
    rerank: R,
    heap: BinaryHeap<(Reverse<F32>, u32)>,
}

impl<C, R> Window0Reranker<C, R> {
    pub fn new(compute: C, rerank: R) -> Self {
        Self {
            compute,
            rerank,
            heap: BinaryHeap::new(),
        }
    }
}

impl<T, C, R, E> Reranker<T, E> for Window0Reranker<C, R>
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
        let (_, u) = self.heap.pop()?;
        let (accu_u, t) = (self.rerank)(u);
        Some((accu_u, u, t))
    }
}
