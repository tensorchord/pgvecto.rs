use super::SegmentTracker;
use crate::utils::file_wal::FileWal;
use crate::IndexTracker;
use crate::Op;
use base::index::*;
use base::operator::*;
use base::search::*;
use base::vector::*;
use common::dir_ops::sync_dir;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cell::UnsafeCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::mem::MaybeUninit;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
#[error("`GrowingSegment` stopped growing.")]
pub struct GrowingSegmentInsertError;

pub struct GrowingSegment<O: Op> {
    uuid: Uuid,
    vec: Vec<MaybeUninit<UnsafeCell<Log<O>>>>,
    wal: Mutex<FileWal>,
    len: AtomicUsize,
    pro: Mutex<Protect>,
    _tracker: Arc<SegmentTracker>,
}

impl<O: Op> GrowingSegment<O> {
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

    pub fn open(
        _tracker: Arc<IndexTracker>,
        path: PathBuf,
        uuid: Uuid,
        _: IndexOptions,
    ) -> Arc<Self> {
        let mut wal = FileWal::open(path.join("wal"));
        let mut vec = Vec::new();
        while let Some(log) = wal.read() {
            let log = bincode::deserialize::<Log<O>>(&log).unwrap();
            vec.push(MaybeUninit::new(UnsafeCell::new(log)));
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
        vector: O::VectorOwned,
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
            UnsafeCell::raw_get(self.vec[i].as_ptr()).write(log.clone());
        }
        while self.len.load(Ordering::Acquire) != i {
            std::hint::spin_loop();
        }
        self.len.store(1 + i, Ordering::Release);
        self.wal
            .lock()
            .write(&bincode::serialize::<Log<O>>(&log).unwrap());
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
            size: (self.len() as u64) * (std::mem::size_of::<Log<O>>() as u64),
        }
    }

    pub fn stat_write(&self) -> SegmentStat {
        SegmentStat {
            id: self.uuid,
            typ: "write".to_string(),
            length: self.len() as usize,
            size: (self.len() as u64) * (std::mem::size_of::<Log<O>>() as u64),
        }
    }

    pub fn vector(&self, i: u32) -> Borrowed<'_, O> {
        let i = i as usize;
        if i >= self.len.load(Ordering::Acquire) {
            panic!("Out of bound.");
        }
        let log = unsafe { &*self.vec[i].assume_init_ref().get().cast_const() };
        log.vector.for_borrow()
    }

    pub fn payload(&self, i: u32) -> Payload {
        let i = i as usize;
        if i >= self.len.load(Ordering::Acquire) {
            panic!("Out of bound.");
        }
        let log = unsafe { &*self.vec[i].assume_init_ref().get().cast_const() };
        log.payload
    }

    pub fn basic(
        &self,
        vector: Borrowed<'_, O>,
        _opts: &SearchOptions,
        mut filter: impl Filter,
    ) -> BinaryHeap<Reverse<Element>> {
        let n = self.len.load(Ordering::Acquire);
        let mut result = BinaryHeap::new();
        for i in 0..n {
            let log = unsafe { &*self.vec[i].assume_init_ref().get().cast_const() };
            if filter.check(log.payload) {
                let distance = O::distance(vector, log.vector.for_borrow());
                result.push(Reverse(Element {
                    distance,
                    payload: log.payload,
                }));
            }
        }
        result
    }

    pub fn vbase<'a>(
        &'a self,
        vector: Borrowed<'a, O>,
        _opts: &SearchOptions,
        mut filter: impl Filter + 'a,
    ) -> (Vec<Element>, Box<dyn Iterator<Item = Element> + 'a>) {
        let n = self.len.load(Ordering::Acquire);
        let mut result = Vec::new();
        for i in 0..n {
            let log = unsafe { &*self.vec[i].assume_init_ref().get().cast_const() };
            if filter.check(log.payload) {
                let distance = O::distance(vector, log.vector.for_borrow());
                result.push(Element {
                    distance,
                    payload: log.payload,
                });
            }
        }
        (result, Box::new(std::iter::empty()))
    }
}

unsafe impl<O: Op> Send for GrowingSegment<O> {}
unsafe impl<O: Op> Sync for GrowingSegment<O> {}

impl<O: Op> Drop for GrowingSegment<O> {
    fn drop(&mut self) {
        let n = *self.len.get_mut();
        for i in 0..n {
            unsafe {
                self.vec[i].assume_init_drop();
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Log<O: Op> {
    vector: O::VectorOwned,
    payload: Payload,
}

#[derive(Debug, Clone)]
struct Protect {
    inflight: usize,
    capacity: usize,
}
