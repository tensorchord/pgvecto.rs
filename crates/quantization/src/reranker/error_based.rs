use base::scalar::F32;
use base::search::Reranker;
use common::always_equal::AlwaysEqual;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct ErrorBasedReranker<T, C, R> {
    compute: C,
    rerank: R,
    heap: BinaryHeap<(
        Reverse<F32>,  /* lowerbound_u */
        u32,
    )>,
    cache: BinaryHeap<(Reverse<F32>, u32, AlwaysEqual<T>)>,
    array: Vec<(Reverse<F32>, u32)>,
}

impl<T, C, R> ErrorBasedReranker<T, C, R> {
    pub fn new(compute: C, rerank: R) -> Self {
        Self {
            compute,
            rerank,
            heap: BinaryHeap::new(),
            cache: BinaryHeap::new(),
            array: Vec::new(),
        }
    }
}

impl<T, C, R, E> Reranker<T, E> for ErrorBasedReranker<T, C, R>
where
    C: Fn(u32, E) -> F32, /* lower bound */
    R: Fn(u32) -> (F32, T),
    E: 'static,
{
    fn push(&mut self, u: u32, extra: E) {
        let lowerbound = (self.compute)(u, extra);
        self.array.push((Reverse(lowerbound), u));
    }

    fn pop(&mut self) -> Option<(F32, u32, T)> {
        if !self.array.is_empty() {
            self.heap.extend(&self.array);
            self.array.clear();
        }
        while !self.heap.is_empty() {
            let accu = self.cache.peek().map(|(Reverse(x), ..)| *x);
            let Reverse(lowerbound) = self.heap.peek().unwrap().0;
            if accu.is_none() || accu > Some(lowerbound) {
                let (_, u) = self.heap.pop().unwrap();
                let (accu_u, t) = (self.rerank)(u);
                self.cache.push((Reverse(accu_u), u, AlwaysEqual(t)));
            } else {
                break;
            }
        }
        let (Reverse(accu_u), u, AlwaysEqual(t)) = self.cache.pop()?;
        Some((accu_u, u, t))
    }
}
