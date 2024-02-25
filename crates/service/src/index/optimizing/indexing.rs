use crate::index::GrowingSegment;
use crate::index::Index;
use crate::index::SealedSegment;
use crate::prelude::*;
use std::cmp::Reverse;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use uuid::Uuid;

pub struct OptimizerIndexing<S: G> {
    index: Arc<Index<S>>,
}

impl<S: G> OptimizerIndexing<S> {
    pub fn new(index: Arc<Index<S>>) -> Self {
        Self { index }
    }
    pub fn spawn(self) {
        std::thread::spawn(move || {
            self.main();
        });
    }
    pub fn main(self) {
        let index = self.index;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(index.options.optimizing.optimizing_threads)
            .build()
            .unwrap();
        let weak_index = Arc::downgrade(&index);
        drop(index);
        loop {
            {
                let Some(index) = weak_index.upgrade() else {
                    return;
                };
                if let Ok(()) = pool.install(|| optimizing_indexing(index.clone())) {
                    continue;
                }
            }
            std::thread::sleep(std::time::Duration::from_secs(60));
        }
    }
}

enum Seg<S: G> {
    Sealed(Arc<SealedSegment<S>>),
    Growing(Arc<GrowingSegment<S>>),
}

impl<S: G> Seg<S> {
    fn uuid(&self) -> Uuid {
        use Seg::*;
        match self {
            Sealed(x) => x.uuid(),
            Growing(x) => x.uuid(),
        }
    }
    fn len(&self) -> u32 {
        use Seg::*;
        match self {
            Sealed(x) => x.len(),
            Growing(x) => x.len(),
        }
    }
    fn get_sealed(&self) -> Option<Arc<SealedSegment<S>>> {
        match self {
            Seg::Sealed(x) => Some(x.clone()),
            _ => None,
        }
    }
    fn get_growing(&self) -> Option<Arc<GrowingSegment<S>>> {
        match self {
            Seg::Growing(x) => Some(x.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Error)]
#[error("Interrupted, retry again.")]
pub struct RetryError;

pub fn optimizing_indexing<S: G>(index: Arc<Index<S>>) -> Result<(), RetryError> {
    use Seg::*;
    let segs = {
        let protect = index.protect.lock();
        let mut segs_0 = Vec::new();
        segs_0.extend(protect.growing.values().map(|x| Growing(x.clone())));
        segs_0.extend(protect.sealed.values().map(|x| Sealed(x.clone())));
        segs_0.sort_by_key(|case| Reverse(case.len()));
        let mut segs_1 = Vec::new();
        let mut total = 0u64;
        let mut count = 0;
        while let Some(seg) = segs_0.pop() {
            if total + seg.len() as u64 <= index.options.segment.max_sealed_segment_size as u64 {
                total += seg.len() as u64;
                if let Growing(_) = seg {
                    count += 1;
                }
                segs_1.push(seg);
            } else {
                break;
            }
        }
        if segs_1.is_empty() || (segs_1.len() == 1 && count == 0) {
            index.instant_index.store(Instant::now());
            return Err(RetryError);
        }
        segs_1
    };
    let sealed_segment = merge(&index, &segs);
    {
        let mut protect = index.protect.lock();
        for seg in segs.iter() {
            if protect.sealed.contains_key(&seg.uuid()) {
                continue;
            }
            if protect.growing.contains_key(&seg.uuid()) {
                continue;
            }
            return Ok(());
        }
        for seg in segs.iter() {
            protect.sealed.remove(&seg.uuid());
            protect.growing.remove(&seg.uuid());
        }
        protect.sealed.insert(sealed_segment.uuid(), sealed_segment);
        protect.maintain(index.options.clone(), index.delete.clone(), &index.view);
    }
    Ok(())
}

fn merge<S: G>(index: &Arc<Index<S>>, segs: &[Seg<S>]) -> Arc<SealedSegment<S>> {
    let sealed = segs.iter().filter_map(|x| x.get_sealed()).collect();
    let growing = segs.iter().filter_map(|x| x.get_growing()).collect();
    let sealed_segment_uuid = Uuid::new_v4();
    SealedSegment::create(
        index._tracker.clone(),
        index
            .path
            .join("segments")
            .join(sealed_segment_uuid.to_string()),
        sealed_segment_uuid,
        index.options.clone(),
        sealed,
        growing,
    )
}
