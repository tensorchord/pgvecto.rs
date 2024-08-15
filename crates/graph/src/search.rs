use crate::visited::VisitedGuard;
use crate::visited::VisitedPool;
use base::always_equal::AlwaysEqual;
use base::scalar::F32;
use base::search::RerankerPop;
use base::search::RerankerPush;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

trait ResultsBound: Ord {
    type T: Ord + Copy;
    fn bound(&self) -> Self::T;
}

impl<T: Ord + Copy, U: Ord> ResultsBound for (T, U) {
    type T = T;

    fn bound(&self) -> T {
        self.0
    }
}

struct Results<T> {
    size: usize,
    heap: BinaryHeap<T>,
}

impl<T: ResultsBound> Results<T> {
    fn new(size: usize) -> Self {
        assert_ne!(size, 0, "size cannot be zero");
        Results {
            size,
            heap: BinaryHeap::with_capacity(size + 1),
        }
    }
    fn push(&mut self, element: T) {
        self.heap.push(element);
        if self.heap.len() > self.size {
            self.heap.pop();
        }
    }
    fn check(&self, value: T::T) -> bool {
        if self.heap.len() < self.size {
            true
        } else {
            Some(value) < self.heap.peek().map(T::bound)
        }
    }
    fn into_sorted_vec(self) -> Vec<T> {
        self.heap.into_sorted_vec()
    }
}

pub fn search<E>(
    dist: impl Fn(u32) -> F32,
    read_outs: impl Fn(u32) -> E,
    visited: &mut VisitedGuard,
    s: u32,
    ef_construction: u32,
) -> Vec<(F32, u32)>
where
    E: Iterator<Item = u32>,
{
    let mut visited = visited.fetch_checker();
    let mut candidates = BinaryHeap::<Reverse<(F32, u32)>>::new();
    let mut results = Results::new(ef_construction as _);
    {
        let dis_s = dist(s);
        visited.mark(s);
        candidates.push(Reverse((dis_s, s)));
    }
    while let Some(Reverse((dis_u, u))) = candidates.pop() {
        if !results.check(dis_u) {
            break;
        }
        results.push((dis_u, u));
        for v in read_outs(u) {
            if !visited.check(v) {
                continue;
            }
            visited.mark(v);
            let dis_v = dist(v);
            if results.check(dis_v) {
                candidates.push(Reverse((dis_v, v)));
            }
        }
    }
    results.into_sorted_vec()
}

pub fn search_returning_trace<E>(
    dist: impl Fn(u32) -> F32,
    read_outs: impl Fn(u32) -> E,
    visited: &mut VisitedGuard,
    s: u32,
    ef_construction: u32,
) -> (Vec<(F32, u32)>, Vec<(F32, u32)>)
where
    E: Iterator<Item = u32>,
{
    let mut visited = visited.fetch_checker();
    let mut reranker = BinaryHeap::<Reverse<(F32, u32)>>::new();
    let mut results = Results::new(ef_construction as _);
    let mut trace = Vec::new();
    {
        let dis_s = dist(s);
        visited.mark(s);
        reranker.push(Reverse((dis_s, s)));
    }
    while let Some(Reverse((dis_u, u))) = reranker.pop() {
        if !results.check(dis_u) {
            break;
        }
        trace.push((dis_u, u));
        results.push((dis_u, u));
        for v in read_outs(u) {
            if !visited.check(v) {
                continue;
            }
            visited.mark(v);
            let dis_v = dist(v);
            if results.check(dis_v) {
                reranker.push(Reverse((dis_v, v)));
            }
        }
    }
    (results.into_sorted_vec(), trace)
}

pub fn vbase_internal<'a, G, E, T>(
    visited: &'a VisitedPool,
    s: u32,
    mut reranker: G,
) -> impl Iterator<Item = (F32, u32, T)> + 'a
where
    G: RerankerPush + RerankerPop<(E, T)> + 'a,
    E: Iterator<Item = u32>,
    T: 'a,
{
    let mut visited = visited.fetch_guard_checker();
    {
        visited.mark(s);
        reranker.push(s);
    }
    std::iter::from_fn(move || {
        let (dis_u, u, (outs_u, pay_u)) = reranker.pop()?;
        for v in outs_u {
            if !visited.check(v) {
                continue;
            }
            visited.mark(v);
            reranker.push(v);
        }
        Some((dis_u, u, pay_u))
    })
}

pub fn vbase_generic<'a, G, E, T>(
    visited: &'a VisitedPool,
    s: u32,
    reranker: G,
    ef_search: u32,
) -> impl Iterator<Item = (F32, u32, T)> + 'a
where
    G: RerankerPush + RerankerPop<(E, T)> + 'a,
    E: Iterator<Item = u32>,
    T: 'a,
{
    let mut iter = vbase_internal(visited, s, reranker);
    let mut results = Results::new(ef_search as _);
    let mut stage1 = Vec::new();
    for (dis_u, u, pay_u) in &mut iter {
        if results.check(dis_u) {
            results.push((dis_u, AlwaysEqual(u)));
            stage1.push((dis_u, u, pay_u));
        } else {
            stage1.push((dis_u, u, pay_u));
            break;
        }
    }
    stage1.sort_unstable_by_key(|x| x.0);
    let mut stage1 = stage1.into_iter().peekable();
    let mut stage2 = iter.peekable();
    std::iter::from_fn(move || {
        if stage1.peek().is_none() {
            return stage2.next();
        }
        if stage2.peek().is_none() {
            return stage1.next();
        }
        if stage1.peek().map(|(dis_u, ..)| dis_u) < stage2.peek().map(|(dis_u, ..)| dis_u) {
            stage1.next()
        } else {
            stage2.next()
        }
    })
}
