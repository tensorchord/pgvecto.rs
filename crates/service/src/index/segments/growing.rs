use super::SegmentTracker;
use crate::index::IndexOptions;
use crate::index::IndexTracker;
use crate::index::SearchOptions;
use crate::index::SegmentStat;
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use crate::utils::file_wal::FileWal;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
#[error("`GrowingSegment` stopped growing.")]
pub struct GrowingSegmentInsertError;

pub struct GrowingSegment<S: G> {
    uuid: Uuid,
    vec: Vec<UnsafeCell<MaybeUninit<Log<S>>>>,
    wal: Mutex<FileWal>,
    len: AtomicUsize,
    pro: Mutex<Protect>,
    _tracker: Arc<SegmentTracker>,
}

impl<S: G> GrowingSegment<S> {
    pub fn create(
        _tracker: Arc<IndexTracker>,
        path: PathBuf,
        uuid: Uuid,
        options: IndexOptions,
    ) -> Arc<Self> {
        std::fs::create_dir(&path).unwrap();
        let wal = FileWal::create(path.join("wal"));
        let capacity = options.segment.max_growing_segment_size;
        sync_dir(&path);
        Arc::new(Self {
            uuid,
            #[allow(clippy::uninit_vec)]
            vec: unsafe {
                let mut vec = Vec::with_capacity(capacity as usize);
                vec.set_len(capacity as usize);
                vec
            },
            wal: Mutex::new(wal),
            len: AtomicUsize::new(0),
            pro: Mutex::new(Protect {
                inflight: 0,
                capacity: capacity as usize,
            }),
            _tracker: Arc::new(SegmentTracker { path, _tracker }),
        })
    }
    pub fn open(_tracker: Arc<IndexTracker>, path: PathBuf, uuid: Uuid) -> Arc<Self> {
        let mut wal = FileWal::open(path.join("wal"));
        let mut vec = Vec::new();
        while let Some(log) = wal.read() {
            let log = bincode::deserialize::<Log<S>>(&log).unwrap();
            vec.push(UnsafeCell::new(MaybeUninit::new(log)));
        }
        wal.truncate();
        let n = vec.len();
        Arc::new(Self {
            uuid,
            vec,
            wal: { Mutex::new(wal) },
            len: AtomicUsize::new(n),
            pro: Mutex::new(Protect {
                inflight: n,
                capacity: n,
            }),
            _tracker: Arc::new(SegmentTracker { path, _tracker }),
        })
    }
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
    pub fn is_full(&self) -> bool {
        let n;
        {
            let pro = self.pro.lock();
            if pro.inflight < pro.capacity {
                return false;
            }
            n = pro.inflight;
        }
        while self.len.load(Ordering::Acquire) != n {
            std::hint::spin_loop();
        }
        true
    }
    pub fn seal(&self) {
        let n;
        {
            let mut pro = self.pro.lock();
            n = pro.inflight;
            pro.capacity = n;
        }
        while self.len.load(Ordering::Acquire) != n {
            std::hint::spin_loop();
        }
        self.wal.lock().sync_all();
    }
    pub fn flush(&self) {
        self.wal.lock().sync_all();
    }
    pub fn insert(
        &self,
        vector: Vec<S::Scalar>,
        payload: Payload,
    ) -> Result<(), GrowingSegmentInsertError> {
        let log = Log { vector, payload };
        let i;
        {
            let mut pro = self.pro.lock();
            if pro.inflight == pro.capacity {
                return Err(GrowingSegmentInsertError);
            }
            i = pro.inflight;
            pro.inflight += 1;
        }
        unsafe {
            (*self.vec[i].get()).write(log.clone());
        }
        while self.len.load(Ordering::Acquire) != i {
            std::hint::spin_loop();
        }
        self.len.store(1 + i, Ordering::Release);
        self.wal
            .lock()
            .write(&bincode::serialize::<Log<S>>(&log).unwrap());
        Ok(())
    }
    pub fn len(&self) -> u32 {
        self.len.load(Ordering::Acquire) as u32
    }
    pub fn stat_growing(&self) -> SegmentStat {
        SegmentStat {
            id: self.uuid,
            typ: "growing".to_string(),
            length: self.len() as usize,
            size: (self.len() as u64) * (std::mem::size_of::<Log<S>>() as u64),
        }
    }
    pub fn stat_write(&self) -> SegmentStat {
        SegmentStat {
            id: self.uuid,
            typ: "write".to_string(),
            length: self.len() as usize,
            size: (self.len() as u64) * (std::mem::size_of::<Log<S>>() as u64),
        }
    }
    pub fn vector(&self, i: u32) -> &[S::Scalar] {
        let i = i as usize;
        if i >= self.len.load(Ordering::Acquire) {
            panic!("Out of bound.");
        }
        let log = unsafe { (*self.vec[i].get()).assume_init_ref() };
        log.vector.as_ref()
    }
    pub fn payload(&self, i: u32) -> Payload {
        let i = i as usize;
        if i >= self.len.load(Ordering::Acquire) {
            panic!("Out of bound.");
        }
        let log = unsafe { (*self.vec[i].get()).assume_init_ref() };
        log.payload
    }
    pub fn search(
        &self,
        vector: &[S::Scalar],
        opts: &SearchOptions,
        filter: &mut impl Filter,
    ) -> Heap {
        let n = self.len.load(Ordering::Acquire);
        let mut heap = Heap::new(opts.search_k);
        for i in 0..n {
            let log = unsafe { (*self.vec[i].get()).assume_init_ref() };
            let distance = S::distance(vector, &log.vector);
            if heap.check(distance) && filter.check(log.payload) {
                heap.push(HeapElement {
                    distance,
                    payload: log.payload,
                });
            }
        }
        heap
    }
    pub fn vbase<'a>(
        &'a self,
        vector: &'a [S::Scalar],
    ) -> (Vec<HeapElement>, Box<dyn Iterator<Item = HeapElement> + 'a>) {
        let n = self.len.load(Ordering::Acquire);
        let mut result = Vec::new();
        for i in 0..n {
            let log = unsafe { (*self.vec[i].get()).assume_init_ref() };
            let distance = S::distance(vector, &log.vector);
            result.push(HeapElement {
                distance,
                payload: log.payload,
            });
        }
        (result, Box::new(std::iter::empty()))
    }
}

unsafe impl<S: G> Send for GrowingSegment<S> {}
unsafe impl<S: G> Sync for GrowingSegment<S> {}

impl<S: G> Drop for GrowingSegment<S> {
    fn drop(&mut self) {
        let n = *self.len.get_mut();
        for i in 0..n {
            unsafe {
                self.vec[i].get_mut().assume_init_drop();
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Log<S: G> {
    vector: Vec<S::Scalar>,
    payload: Payload,
}

#[derive(Debug, Clone)]
struct Protect {
    inflight: usize,
    capacity: usize,
}
