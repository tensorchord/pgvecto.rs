use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::search::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct GraphReranker<'a, T, R> {
    compute: Option<Box<dyn Fn(u32) -> Distance + 'a>>,
    rerank: R,
    heap: BinaryHeap<(Reverse<Distance>, AlwaysEqual<u32>)>,
    cache: BinaryHeap<(Reverse<Distance>, AlwaysEqual<u32>, AlwaysEqual<T>)>,
}

impl<'a, T, R> GraphReranker<'a, T, R> {
    pub fn new(compute: Option<Box<dyn Fn(u32) -> Distance + 'a>>, rerank: R) -> Self {
        Self {
            compute,
            rerank,
            heap: BinaryHeap::new(),
            cache: BinaryHeap::new(),
        }
    }
}

impl<'a, T, R> RerankerPop<T> for GraphReranker<'a, T, R>
where
    R: Fn(u32) -> (Distance, T),
{
    fn pop(&mut self) -> Option<(Distance, u32, T)> {
        if self.compute.is_some() {
            let (_, AlwaysEqual(u)) = self.heap.pop()?;
            let (dis_u, pay_u) = (self.rerank)(u);
            Some((dis_u, u, pay_u))
        } else {
            let (Reverse(dis_u), AlwaysEqual(u), AlwaysEqual(pay_u)) = self.cache.pop()?;
            Some((dis_u, u, pay_u))
        }
    }
}

impl<'a, T, R> RerankerPush for GraphReranker<'a, T, R>
where
    R: Fn(u32) -> (Distance, T),
{
    fn push(&mut self, u: u32) {
        if let Some(compute) = self.compute.as_ref() {
            let rough_u = (compute)(u);
            self.heap.push((Reverse(rough_u), AlwaysEqual(u)));
        } else {
            let (dis_u, pay_u) = (self.rerank)(u);
            self.cache
                .push((Reverse(dis_u), AlwaysEqual(u), AlwaysEqual(pay_u)));
        }
    }
}
