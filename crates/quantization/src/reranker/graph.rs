use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::search::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct GraphReranker<T, R> {
    rerank: R,
    cache: BinaryHeap<(Reverse<Distance>, AlwaysEqual<u32>, AlwaysEqual<T>)>,
}

impl<T, R> GraphReranker<T, R> {
    pub fn new(rerank: R) -> Self {
        Self {
            rerank,
            cache: BinaryHeap::new(),
        }
    }
}

impl<T, R> RerankerPop<T> for GraphReranker<T, R>
where
    R: Fn(u32) -> (Distance, T),
{
    fn pop(&mut self) -> Option<(Distance, u32, T)> {
        let (Reverse(dis_u), AlwaysEqual(u), AlwaysEqual(pay_u)) = self.cache.pop()?;
        Some((dis_u, u, pay_u))
    }
}

impl<T, R> RerankerPush for GraphReranker<T, R>
where
    R: Fn(u32) -> (Distance, T),
{
    fn push(&mut self, u: u32) {
        let (dis_u, pay_u) = (self.rerank)(u);
        self.cache
            .push((Reverse(dis_u), AlwaysEqual(u), AlwaysEqual(pay_u)));
    }
}
