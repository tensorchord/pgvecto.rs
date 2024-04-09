use crate::optimizing::index_source::IndexSource;
use crate::Index;
use crate::Op;
use crate::SealedSegment;
pub use base::distance::*;
pub use base::index::*;
pub use base::search::*;
pub use base::vector::*;
use std::sync::Arc;
use uuid::Uuid;

pub fn scan<O: Op>(index: Arc<Index<O>>) -> Option<IndexSource<O>> {
    let (sealed, growing) = 'a: {
        let protect = index.protect.lock();
        // task 1 approach 1: merge small segments to a big segment
        {
            let mut counter = 0u64;
            let base_segment = {
                let mut sealed_segments = protect.sealed.values().collect::<Vec<_>>();
                sealed_segments.sort_by_key(|s| s.len());
                let base_segment = sealed_segments.first().cloned();
                counter += base_segment.map(|x| x.len() as u64).unwrap_or_default();
                base_segment.cloned()
            };
            let delta_segments = {
                let mut growing_segments = protect.growing.values().collect::<Vec<_>>();
                growing_segments.sort_by_key(|s| s.len());
                let mut delta_segments = Vec::new();
                for growing_segment in growing_segments.iter().cloned().cloned() {
                    if counter + growing_segment.len() as u64
                        <= index.options.segment.max_sealed_segment_size as u64
                    {
                        counter += growing_segment.len() as u64;
                        delta_segments.push(growing_segment);
                    } else {
                        break;
                    }
                }
                delta_segments
            };
            if !delta_segments.is_empty() {
                break 'a (base_segment, delta_segments);
            }
        }
        // task 1 approach 2: merge small segments
        {
            let mut counter = 0u64;
            let delta_segments = {
                let mut growing_segments = protect.growing.values().collect::<Vec<_>>();
                growing_segments.sort_by_key(|s| s.len());
                let mut delta_segments = Vec::new();
                for growing_segment in growing_segments.iter().cloned().cloned() {
                    if counter + growing_segment.len() as u64
                        <= index.options.segment.max_sealed_segment_size as u64
                    {
                        counter += growing_segment.len() as u64;
                        delta_segments.push(growing_segment);
                    } else {
                        break;
                    }
                }
                delta_segments
            };
            if !delta_segments.is_empty() {
                break 'a (None, delta_segments);
            }
        }
        return None;
    };
    Some(IndexSource::new(
        index.options().clone(),
        sealed.clone(),
        growing.clone(),
    ))
}

pub fn make<O: Op>(index: Arc<Index<O>>, source: IndexSource<O>) {
    let next = {
        let uuid = Uuid::new_v4();
        SealedSegment::create(
            index._tracker.clone(),
            index.path.join("segments").join(uuid.to_string()),
            uuid,
            index.options.clone(),
            &source,
        )
    };
    let mut protect = index.protect.lock();
    for sealed in source.sealed.iter() {
        if protect.sealed.contains_key(&sealed.uuid()) {
            continue;
        }
        return;
    }
    for growing in source.growing.iter() {
        if protect.growing.contains_key(&growing.uuid()) {
            continue;
        }
        return;
    }
    for sealed in source.sealed.iter() {
        protect.sealed.remove(&sealed.uuid());
    }
    for growing in source.growing.iter() {
        protect.growing.remove(&growing.uuid());
    }
    protect.sealed.insert(next.uuid(), next);
    protect.maintain(index.options.clone(), index.delete.clone(), &index.view);
}
