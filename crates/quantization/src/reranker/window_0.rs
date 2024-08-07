use base::scalar::F32;
use base::search::{RerankerPop, RerankerPush};
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct Window0GraphReranker<C, R> {
    compute: C,
    rerank: R,
    heap: BinaryHeap<(Reverse<F32>, u32)>,
}

impl<C, R> Window0GraphReranker<C, R> {
    pub fn new(compute: C, rerank: R) -> Self {
        Self {
            compute,
            rerank,
            heap: BinaryHeap::new(),
        }
    }
}

impl<T, C, R> RerankerPop<T> for Window0GraphReranker<C, R>
where
    R: Fn(u32) -> (F32, T),
{
    fn pop(&mut self) -> Option<(F32, u32, T)> {
        let (_, u) = self.heap.pop()?;
        let (accu_u, t) = (self.rerank)(u);
        Some((accu_u, u, t))
    }
}

impl<T, C, R> RerankerPush for Window0GraphReranker<C, R>
where
    C: Fn(u32) -> F32,
    R: Fn(u32) -> (F32, T),
{
    fn push(&mut self, u: u32) {
        let rough_u = (self.compute)(u);
        self.heap.push((Reverse(rough_u), u));
    }
}
