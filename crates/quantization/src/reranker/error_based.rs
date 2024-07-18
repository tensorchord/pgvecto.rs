use base::scalar::F32;
use base::search::Reranker;
use common::always_equal::AlwaysEqual;
use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::collections::BinaryHeap;

pub struct ErrorBasedReranker<T, C, R> {
    compute: C,
    rerank: R,
    heap: BinaryHeap<(
        Reverse<F32>, /* rough_u */
        F32,          /* lowerbound_u */
        u32,
    )>,
    lowb: BTreeSet<(F32 /* lowerbound_u */, u32)>,
    cache: BinaryHeap<(Reverse<F32>, u32, AlwaysEqual<T>)>,
}

impl<T, C, R> ErrorBasedReranker<T, C, R> {
    pub fn new(compute: C, rerank: R) -> Self {
        Self {
            compute,
            rerank,
            heap: BinaryHeap::new(),
            lowb: BTreeSet::new(),
            cache: BinaryHeap::new(),
        }
    }
}

impl<T, C, R, E> Reranker<T, E> for ErrorBasedReranker<T, C, R>
where
    C: Fn(u32, E) -> (F32 /* rough_u */, F32 /* error_u */),
    R: Fn(u32) -> (F32, T),
    E: 'static,
{
    fn push(&mut self, u: u32, extra: E) {
        let (rough_u, error_u) = (self.compute)(u, extra);
        let lowerbound_u = rough_u - error_u;
        self.heap.push((Reverse(rough_u), lowerbound_u, u));
        self.lowb.insert((lowerbound_u, u));
    }

    fn pop(&mut self) -> Option<(F32, u32, T)> {
        while !self.heap.is_empty() {
            let accu = self.cache.peek().map(|(Reverse(x), ..)| *x);
            let lowerbound = self.lowb.first().unwrap().0;
            if accu.is_none() || accu > Some(lowerbound) {
                let (_, lowerbound_u, u) = self.heap.pop().unwrap();
                self.lowb.remove(&(lowerbound_u, u));
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
