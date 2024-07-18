use base::scalar::F32;
use base::search::Reranker;
use common::always_equal::AlwaysEqual;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct DisabledReranker<T, R> {
    rerank: R,
    heap: BinaryHeap<(Reverse<F32>, u32, AlwaysEqual<T>)>,
}

impl<T, R> DisabledReranker<T, R> {
    pub fn new(rerank: R) -> Self {
        Self {
            rerank,
            heap: BinaryHeap::new(),
        }
    }
}

impl<T, R, E> Reranker<T, E> for DisabledReranker<T, R>
where
    R: Fn(u32) -> (F32, T),
    E: 'static,
{
    fn push(&mut self, u: u32, _: E) {
        let (accu_u, t) = (self.rerank)(u);
        self.heap.push((Reverse(accu_u), u, AlwaysEqual(t)));
    }

    fn pop(&mut self) -> Option<(F32, u32, T)> {
        let (Reverse(accu_u), u, AlwaysEqual(t)) = self.heap.pop()?;
        Some((accu_u, u, t))
    }
}
