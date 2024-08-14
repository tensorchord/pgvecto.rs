use base::always_equal::AlwaysEqual;
use base::scalar::F32;
use base::search::RerankerPop;
use num_traits::Float;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

const WINDOW_SIZE: usize = 16;

pub struct ErrorBasedReranker<T, R> {
    rerank: R,
    cache: BinaryHeap<(Reverse<F32>, AlwaysEqual<u32>, AlwaysEqual<T>)>,
    distance_threshold: F32,
    heap: Vec<(Reverse<F32>, AlwaysEqual<u32>)>,
    ranked: bool,
}

impl<T, R> ErrorBasedReranker<T, R> {
    pub fn new(heap: Vec<(Reverse<F32>, AlwaysEqual<u32>)>, rerank: R) -> Self {
        Self {
            rerank,
            cache: BinaryHeap::new(),
            distance_threshold: F32::infinity(),
            heap,
            ranked: false,
        }
    }
}

impl<T, R> RerankerPop<T> for ErrorBasedReranker<T, R>
where
    R: Fn(u32) -> (F32, T),
{
    fn pop(&mut self) -> Option<(F32, u32, T)> {
        if !self.ranked {
            self.ranked = true;
            let mut recent_max_accurate = F32::neg_infinity();
            let mut count = 0;
            for &(Reverse(lowerbound), AlwaysEqual(u)) in self.heap.iter() {
                if lowerbound < self.distance_threshold {
                    let (accurate, pay_u) = (self.rerank)(u);
                    if accurate < self.distance_threshold {
                        self.cache
                            .push((Reverse(accurate), AlwaysEqual(u), AlwaysEqual(pay_u)));
                        count += 1;
                        recent_max_accurate = std::cmp::max(accurate, recent_max_accurate);
                        if count == WINDOW_SIZE {
                            self.distance_threshold = recent_max_accurate;
                            count = 0;
                            recent_max_accurate = F32::neg_infinity();
                        }
                    }
                }
            }
        }
        let (Reverse(dist), AlwaysEqual(u), AlwaysEqual(pay_u)) = self.cache.pop()?;
        Some((dist, u, pay_u))
    }
}
