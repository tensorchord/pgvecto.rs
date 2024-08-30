use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::search::RerankerPop;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct ErrorFlatReranker<T, R> {
    rerank: R,
    heap: BinaryHeap<(Reverse<Distance>, AlwaysEqual<u32>)>,
    cache: BinaryHeap<(Reverse<Distance>, AlwaysEqual<u32>, AlwaysEqual<T>)>,
}

impl<T, R> ErrorFlatReranker<T, R> {
    pub fn new(heap: Vec<(Reverse<Distance>, AlwaysEqual<u32>)>, rerank: R) -> Self
    where
        R: Fn(u32) -> (Distance, T),
    {
        Self {
            rerank,
            heap: heap.into(),
            cache: BinaryHeap::new(),
        }
    }
}

impl<T, R> RerankerPop<T> for ErrorFlatReranker<T, R>
where
    R: Fn(u32) -> (Distance, T),
{
    fn pop(&mut self) -> Option<(Distance, u32, T)> {
        while !self.heap.is_empty()
            && self.heap.peek().map(|x| x.0) > self.cache.peek().map(|x| x.0)
        {
            let (_, AlwaysEqual(u)) = self.heap.pop().unwrap();
            let (dis_u, pay_u) = (self.rerank)(u);
            self.cache
                .push((Reverse(dis_u), AlwaysEqual(u), AlwaysEqual(pay_u)));
        }
        let (Reverse(dist), AlwaysEqual(u), AlwaysEqual(pay_u)) = self.cache.pop()?;
        Some((dist, u, pay_u))
    }
}
