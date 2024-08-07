use base::scalar::F32;
use base::search::*;
use common::always_equal::AlwaysEqual;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct DisabledFlatReranker<T> {
    heap: BinaryHeap<(Reverse<F32>, u32, AlwaysEqual<T>)>,
}

impl<T> DisabledFlatReranker<T> {
    pub fn new<R>(heap: Vec<(Reverse<F32>, u32)>, rerank: R) -> Self
    where
        R: Fn(u32) -> (F32, T),
    {
        Self {
            heap: heap
                .into_iter()
                .map(|(_, u)| {
                    let (dis_u, pay_u) = rerank(u);
                    (Reverse(dis_u), u, AlwaysEqual(pay_u))
                })
                .collect(),
        }
    }
}

impl<T> RerankerPop<T> for DisabledFlatReranker<T> {
    fn pop(&mut self) -> Option<(F32, u32, T)> {
        let (Reverse(accu_u), u, AlwaysEqual(t)) = self.heap.pop()?;
        Some((accu_u, u, t))
    }
}

pub struct DisabledGraphReranker<T, R> {
    rerank: R,
    heap: BinaryHeap<(Reverse<F32>, u32, AlwaysEqual<T>)>,
}

impl<T, R> DisabledGraphReranker<T, R> {
    pub fn new(rerank: R) -> Self {
        Self {
            rerank,
            heap: BinaryHeap::new(),
        }
    }
}

impl<T, R> RerankerPush for DisabledGraphReranker<T, R>
where
    R: Fn(u32) -> (F32, T),
{
    fn push(&mut self, u: u32) {
        let (accu_u, t) = (self.rerank)(u);
        self.heap.push((Reverse(accu_u), u, AlwaysEqual(t)));
    }
}

impl<T, R> RerankerPop<T> for DisabledGraphReranker<T, R> {
    fn pop(&mut self) -> Option<(F32, u32, T)> {
        let (Reverse(accu_u), u, AlwaysEqual(t)) = self.heap.pop()?;
        Some((accu_u, u, t))
    }
}
