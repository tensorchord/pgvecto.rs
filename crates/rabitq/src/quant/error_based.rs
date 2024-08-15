use base::always_equal::AlwaysEqual;
use base::scalar::F32;
use base::search::RerankerPop;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct ErrorBasedFlatReranker<T, R> {
    rerank: R,
    heap: BinaryHeap<(Reverse<F32>, AlwaysEqual<u32>)>,
    cache: BinaryHeap<(Reverse<F32>, AlwaysEqual<u32>, AlwaysEqual<T>)>,
}

impl<T, R> ErrorBasedFlatReranker<T, R> {
    pub fn new(heap: Vec<(Reverse<F32>, AlwaysEqual<u32>)>, rerank: R) -> Self {
        Self {
            rerank,
            heap: heap.into(),
            cache: BinaryHeap::new(),
        }
    }
}

impl<T, R> RerankerPop<T> for ErrorBasedFlatReranker<T, R>
where
    R: Fn(u32) -> (F32, T),
{
    fn pop(&mut self) -> Option<(F32, u32, T)> {
        while !self.heap.is_empty()
            && (self.cache.is_empty() || self.heap.peek().unwrap().0 > self.cache.peek().unwrap().0)
        {
            let (_, AlwaysEqual(u)) = self.heap.pop().unwrap();
            let (accu_u, pay_u) = (self.rerank)(u);
            self.cache
                .push((Reverse(accu_u), AlwaysEqual(u), AlwaysEqual(pay_u)));
        }
        let (Reverse(accu_u), AlwaysEqual(u), AlwaysEqual(t)) = self.cache.pop()?;
        Some((accu_u, u, t))
    }
}
