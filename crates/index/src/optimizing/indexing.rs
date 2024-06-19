use crate::optimizing::index_source::IndexSource;
use crate::Index;
use crate::Op;
use crate::SealedSegment;
use std::sync::Arc;
use uuid::Uuid;

pub fn scan<O: Op>(
    index: Arc<Index<O>>,
    capacity: u32,
    delete_threshold: f64,
) -> Option<IndexSource<O>> {
    let (sealed, growing) = 'a: {
        let protect = index.protect.lock();
        // approach 1: merge small segments to a big segment
        {
            let mut counter = 0u64;
            let base_segment = {
                let mut sealed_segments = protect.sealed_segments.values().collect::<Vec<_>>();
                sealed_segments.sort_by_key(|s| s.len());
                let base_segment = sealed_segments.first().cloned();
                counter += base_segment.map(|x| x.len() as u64).unwrap_or_default();
                base_segment.cloned()
            };
            let delta_segments = {
                let mut growing_segments = protect.read_segments.values().collect::<Vec<_>>();
                growing_segments.sort_by_key(|s| s.len());
                let mut delta_segments = Vec::new();
                for growing_segment in growing_segments.iter().cloned().cloned() {
                    if counter + growing_segment.len() as u64 <= capacity as u64 {
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
        // approach 2: merge small segments
        {
            let mut counter = 0u64;
            let delta_segments = {
                let mut growing_segments = protect.read_segments.values().collect::<Vec<_>>();
                growing_segments.sort_by_key(|s| s.len());
                let mut delta_segments = Vec::new();
                for growing_segment in growing_segments.iter().cloned().cloned() {
                    if counter + growing_segment.len() as u64 <= capacity as u64 {
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
        // approach 3: vacuum sealed segment
        if !index.get_check_deleted_flag() {
            let sealed_segments = protect.sealed_segments.values().collect::<Vec<_>>();
            for sealed_segment in sealed_segments {
                let mut counter = 0u64;
                for i in 0..sealed_segment.len() {
                    if !index.check_existing(sealed_segment.payload(i)) {
                        counter += 1;
                    }
                }
                let value = counter as f64 / sealed_segment.len() as f64;
                if value >= delete_threshold {
                    break 'a (Some(sealed_segment.clone()), Vec::new());
                }
            }
            index.set_check_deleted_flag();
        }
        return None;
    };
    Some(IndexSource::new(
        index.options().clone(),
        sealed.clone(),
        growing.clone(),
        index.delete.clone(),
    ))
}

pub fn make<O: Op>(index: Arc<Index<O>>, source: IndexSource<O>) {
    let next = {
        let id = Uuid::new_v4();
        SealedSegment::create(
            index._tracker.clone(),
            index.path.join("segments").join(id.to_string()),
            id,
            index.options.clone(),
            &source,
        )
    };
    let mut protect = index.protect.lock();
    for sealed_segment in source.sealed.iter() {
        if protect.sealed_segments.contains_key(&sealed_segment.id()) {
            continue;
        }
        return;
    }
    for growing_segment in source.growing.iter() {
        if protect.read_segments.contains_key(&growing_segment.id()) {
            continue;
        }
        return;
    }
    for sealed_segment in source.sealed.iter() {
        protect.sealed_segments.remove(&sealed_segment.id());
    }
    for growing_segment in source.growing.iter() {
        protect.read_segments.remove(&growing_segment.id());
    }
    protect.sealed_segments.insert(next.id(), next);
    protect.maintain(index.options.clone(), index.delete.clone(), &index.view);
}
