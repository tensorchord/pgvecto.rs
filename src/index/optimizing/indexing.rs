use crate::index::GrowingSegment;
use crate::index::Index;
use crate::index::SealedSegment;
use std::cmp::Reverse;
use std::sync::Arc;
use uuid::Uuid;

enum Seg {
    Sealed(Arc<SealedSegment>),
    Growing(Arc<GrowingSegment>),
}

impl Seg {
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
    fn get_sealed(&self) -> Option<Arc<SealedSegment>> {
        match self {
            Seg::Sealed(x) => Some(x.clone()),
            _ => None,
        }
    }
    fn get_growing(&self) -> Option<Arc<GrowingSegment>> {
        match self {
            Seg::Growing(x) => Some(x.clone()),
            _ => None,
        }
    }
}

pub fn optimizing_indexing(index: Arc<Index>) -> bool {
    use Seg::*;
    let segs = {
        let mut all_segs = {
            let protect = index.protect.lock();
            let mut all_segs = Vec::new();
            all_segs.extend(protect.growing.values().map(|x| Growing(x.clone())));
            all_segs.extend(protect.sealed.values().map(|x| Sealed(x.clone())));
            all_segs.sort_by_key(|case| Reverse(case.len()));
            all_segs
        };
        let mut segs = Vec::new();
        let mut segs_len = 0u64;
        while let Some(seg) = all_segs.pop() {
            if segs_len + seg.len() as u64 <= index.options.segment.max_sealed_segment_size as u64 {
                segs_len += seg.len() as u64;
                segs.push(seg);
            } else {
                break;
            }
        }
        if segs_len < index.options.segment.min_sealed_segment_size as u64 || segs.len() < 3 {
            return true;
        }
        segs
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
            return false;
        }
        for seg in segs.iter() {
            protect.sealed.remove(&seg.uuid());
            protect.growing.remove(&seg.uuid());
        }
        protect.sealed.insert(sealed_segment.uuid(), sealed_segment);
        protect.maintain(index.options.clone(), index.delete.clone(), &index.view);
    }
    false
}

fn merge(index: &Arc<Index>, segs: &[Seg]) -> Arc<SealedSegment> {
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
