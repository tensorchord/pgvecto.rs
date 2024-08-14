use base::always_equal::AlwaysEqual;
use base::scalar::F32;
use base::search::*;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct WindowFlatReranker<T, R> {
    rerank: R,
    size: u32,
    heap: BinaryHeap<(Reverse<F32>, AlwaysEqual<u32>)>,
    cache: BinaryHeap<(Reverse<F32>, AlwaysEqual<u32>, AlwaysEqual<T>)>,
}

impl<T, R> WindowFlatReranker<T, R>
where
    R: Fn(u32) -> (F32, T),
{
    pub fn new(heap: Vec<(Reverse<F32>, AlwaysEqual<u32>)>, rerank: R, size: u32) -> Self {
        Self {
            heap: heap.into(),
            rerank,
            size: size.max(1),
            cache: BinaryHeap::new(),
        }
    }
}

impl<T, R> RerankerPop<T> for WindowFlatReranker<T, R>
where
    R: Fn(u32) -> (F32, T),
{
    fn pop(&mut self) -> Option<(F32, u32, T)> {
        while !self.heap.is_empty() && self.cache.len() < self.size as _ {
            let (_, AlwaysEqual(u)) = self.heap.pop().unwrap();
            let (accu_u, pay_u) = (self.rerank)(u);
            self.cache
                .push((Reverse(accu_u), AlwaysEqual(u), AlwaysEqual(pay_u)));
        }
        let (Reverse(accu_u), AlwaysEqual(u), AlwaysEqual(pay_u)) = self.cache.pop()?;
        Some((accu_u, u, pay_u))
    }
}
