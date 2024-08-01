use base::scalar::F32;
use base::search::Reranker;
use common::always_equal::AlwaysEqual;
use num_traits::Float;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

const WINDOW_SIZE: usize = 16;

pub struct ErrorBasedReranker<T, C, R> {
    compute: C,
    rerank: R,
    cache: BinaryHeap<(Reverse<F32>, u32, AlwaysEqual<T>)>,
    distance_threshold: F32,
    array: Vec<(F32, u32)>,
    ranked: bool,
}

impl<T, C, R> ErrorBasedReranker<T, C, R> {
    pub fn new(compute: C, rerank: R) -> Self {
        Self {
            compute,
            rerank,
            cache: BinaryHeap::new(),
            distance_threshold: F32::infinity(),
            array: Vec::new(),
            ranked: false,
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
        self.array.push((lowerbound, u));
    }

    fn pop(&mut self) -> Option<(F32, u32, T)> {
        if !self.ranked {
            self.ranked = true;
            let mut recent_max_accurate = F32::neg_infinity();
            let mut count = 0;
            for &(lowerbound, u) in self.array.iter() {
                if lowerbound < self.distance_threshold {
                    let (accurate, t) = (self.rerank)(u);
                    if accurate < self.distance_threshold {
                        self.cache.push((Reverse(accurate), u, AlwaysEqual(t)));
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
        let (Reverse(dist), u, AlwaysEqual(t)) = self.cache.pop()?;
        Some((dist, u, t))
    }
}
