use crate::GrowingSegment;
use crate::Index;
use crate::Op;
use crate::SealedSegment;
pub use base::distance::*;
pub use base::index::*;
use base::operator::Borrowed;
pub use base::search::*;
pub use base::vector::*;
use crossbeam::channel::TryRecvError;
use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender};
use std::cmp::Reverse;
use std::convert::Infallible;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Instant;
use thiserror::Error;
use uuid::Uuid;

pub struct IndexSource<O: Op> {
    sealed: Vec<Arc<SealedSegment<O>>>,
    growing: Vec<Arc<GrowingSegment<O>>>,
    dims: u32,
}

impl<O: Op> IndexSource<O> {
    pub fn new(
        options: IndexOptions,
        sealed: Vec<Arc<SealedSegment<O>>>,
        growing: Vec<Arc<GrowingSegment<O>>>,
    ) -> Self {
        IndexSource {
            sealed,
            growing,
            dims: options.vector.dims,
        }
    }
}

impl<O: Op> Collection<O> for IndexSource<O> {
    fn dims(&self) -> u32 {
        self.dims
    }

    fn len(&self) -> u32 {
        self.sealed.iter().map(|x| x.len()).sum::<u32>()
            + self.growing.iter().map(|x| x.len()).sum::<u32>()
    }

    fn vector(&self, mut index: u32) -> Borrowed<'_, O> {
        for x in self.sealed.iter() {
            if index < x.len() {
                return x.vector(index);
            }
            index -= x.len();
        }
        for x in self.growing.iter() {
            if index < x.len() {
                return x.vector(index);
            }
            index -= x.len();
        }
        panic!("Out of bound.")
    }

    fn payload(&self, mut index: u32) -> Payload {
        for x in self.sealed.iter() {
            if index < x.len() {
                return x.payload(index);
            }
            index -= x.len();
        }
        for x in self.growing.iter() {
            if index < x.len() {
                return x.payload(index);
            }
            index -= x.len();
        }
        panic!("Out of bound.")
    }
}

impl<O: Op> Source<O> for IndexSource<O> {}

pub struct OptimizerIndexing<O: Op> {
    index: Arc<Index<O>>,
}

impl<O: Op> OptimizerIndexing<O> {
    pub fn new(index: Arc<Index<O>>) -> Self {
        Self { index }
    }
    pub fn spawn(self) -> (Sender<Infallible>, JoinHandle<()>) {
        let (tx, rx) = bounded(1);
        (
            tx,
            std::thread::spawn(move || {
                self.main(rx);
            }),
        )
    }
    fn main(self, shutdown_rx: Receiver<Infallible>) {
        let index = self.index;
        loop {
            let view = index.view();
            let threads = view.flexible.optimizing_threads;
            let (finish_tx, finish_rx) = bounded::<Infallible>(1);
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads as usize)
                .build_scoped(|pool| {
                    std::thread::scope(|scope| {
                        let handler = scope.spawn(|| {
                            let status = monitor(&finish_rx, &shutdown_rx);
                            match status {
                                MonitorStatus::Finished => (),
                                MonitorStatus::Shutdown => pool.stop(),
                            }
                        });
                        pool.install(|| {
                            let _finish_tx = finish_tx;
                            let _ = optimizing_indexing(index.clone());
                        });
                        let _ = handler.join();
                    })
                })
                .unwrap();
            match shutdown_rx.recv_timeout(std::time::Duration::from_secs(60)) {
                Ok(never) => match never {},
                Err(RecvTimeoutError::Disconnected) => return,
                Err(RecvTimeoutError::Timeout) => (),
            }
        }
    }
}

pub enum MonitorStatus {
    Finished,
    Shutdown,
}

/// Monitor the internal finish and the external shutdown of `optimizing_indexing`
fn monitor(finish_rx: &Receiver<Infallible>, shutdown_rx: &Receiver<Infallible>) -> MonitorStatus {
    let timeout = std::time::Duration::from_secs(1);
    loop {
        match finish_rx.try_recv() {
            Ok(never) => match never {},
            Err(TryRecvError::Disconnected) => {
                return MonitorStatus::Finished;
            }
            Err(TryRecvError::Empty) => (),
        }
        match shutdown_rx.recv_timeout(timeout) {
            Ok(never) => match never {},
            Err(RecvTimeoutError::Disconnected) => {
                return MonitorStatus::Shutdown;
            }
            Err(RecvTimeoutError::Timeout) => (),
        }
    }
}

enum Seg<O: Op> {
    Sealed(Arc<SealedSegment<O>>),
    Growing(Arc<GrowingSegment<O>>),
}

impl<O: Op> Seg<O> {
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
    fn get_sealed(&self) -> Option<Arc<SealedSegment<O>>> {
        match self {
            Seg::Sealed(x) => Some(x.clone()),
            _ => None,
        }
    }
    fn get_growing(&self) -> Option<Arc<GrowingSegment<O>>> {
        match self {
            Seg::Growing(x) => Some(x.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, Error)]
#[error("Interrupted, retry again.")]
pub struct RetryError;

pub fn optimizing_indexing<O: Op>(index: Arc<Index<O>>) -> Result<(), RetryError> {
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

fn merge<O: Op>(index: &Arc<Index<O>>, segs: &[Seg<O>]) -> Arc<SealedSegment<O>> {
    let sealed = segs.iter().filter_map(|x| x.get_sealed()).collect();
    let growing = segs.iter().filter_map(|x| x.get_growing()).collect();
    let sealed_segment_uuid = Uuid::new_v4();
    let collection = IndexSource::new(index.options().clone(), sealed, growing);
    SealedSegment::create(
        index._tracker.clone(),
        index
            .path
            .join("segments")
            .join(sealed_segment_uuid.to_string()),
        sealed_segment_uuid,
        index.options.clone(),
        &collection,
    )
}
