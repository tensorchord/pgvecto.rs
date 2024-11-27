use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::search::RerankerPop;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct ErrorlessFlatReranker<R> {
    rerank: R,
    heap: BinaryHeap<(Reverse<Distance>, AlwaysEqual<u32>)>,
}

impl<T, R> ErrorlessFlatReranker<R>
where
    R: Fn(u32) -> (Distance, T),
{
    pub fn new(heap: Vec<(Reverse<Distance>, AlwaysEqual<u32>)>, rerank: R) -> Self {
        Self {
            rerank,
            heap: heap.into(),
        }
    }
}

impl<T, R> RerankerPop<T> for ErrorlessFlatReranker<R>
where
    R: Fn(u32) -> (Distance, T),
{
    fn pop(&mut self) -> Option<(Distance, u32, T)> {
        let (_, AlwaysEqual(u)) = self.heap.pop()?;
        let (dis_u, pay_u) = (self.rerank)(u);
        Some((dis_u, u, pay_u))
    }
}
