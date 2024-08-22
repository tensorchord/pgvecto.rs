use base::always_equal::AlwaysEqual;
use base::distance::Distance;
use base::search::RerankerPop;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

const WINDOW_SIZE: usize = 16;

pub struct ErrorBasedReranker<T, R> {
    rerank: R,
    cache: BinaryHeap<(Reverse<Distance>, AlwaysEqual<u32>, AlwaysEqual<T>)>,
    distance_threshold: Distance,
    heap: Vec<(Reverse<Distance>, AlwaysEqual<u32>)>,
    ranked: bool,
}

impl<T, R> ErrorBasedReranker<T, R> {
    pub fn new(heap: Vec<(Reverse<Distance>, AlwaysEqual<u32>)>, rerank: R) -> Self {
        Self {
            rerank,
            cache: BinaryHeap::new(),
            distance_threshold: Distance::INFINITY,
            heap,
            ranked: false,
        }
    }
}

impl<T, R> RerankerPop<T> for ErrorBasedReranker<T, R>
where
    R: Fn(u32) -> (Distance, T),
{
    fn pop(&mut self) -> Option<(Distance, u32, T)> {
        if !self.ranked {
            self.ranked = true;
            let mut recent_max_accurate = Distance::NEG_INFINITY;
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
                            recent_max_accurate = Distance::NEG_INFINITY;
                        }
                    }
                }
            }
        }
        let (Reverse(dist), AlwaysEqual(u), AlwaysEqual(pay_u)) = self.cache.pop()?;
        Some((dist, u, pay_u))
    }
}
