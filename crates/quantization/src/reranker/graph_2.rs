use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::search::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct Graph2Reranker<F, R> {
    compute: F,
    rerank: R,
    heap: BinaryHeap<(Reverse<Distance>, AlwaysEqual<u32>)>,
}

impl<F, R> Graph2Reranker<F, R> {
    pub fn new(compute: F, rerank: R) -> Self {
        Self {
            compute,
            rerank,
            heap: BinaryHeap::new(),
        }
    }
}

impl<T, F, R> RerankerPop<T> for Graph2Reranker<F, R>
where
    R: Fn(u32) -> (Distance, T),
{
    fn pop(&mut self) -> Option<(Distance, u32, T)> {
        let (_, AlwaysEqual(u)) = self.heap.pop()?;
        let (dis_u, pay_u) = (self.rerank)(u);
        Some((dis_u, u, pay_u))
    }
}

impl<T, F, R> RerankerPush for Graph2Reranker<F, R>
where
    F: Fn(u32) -> Distance,
    R: Fn(u32) -> (Distance, T),
{
    fn push(&mut self, u: u32) {
        let rough_u = (self.compute)(u);
        self.heap.push((Reverse(rough_u), AlwaysEqual(u)));
    }
}
