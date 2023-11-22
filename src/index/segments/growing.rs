use super::SegmentTracker;
use crate::index::IndexOptions;
use crate::index::IndexTracker;
use crate::index::VectorOptions;
use crate::prelude::*;
use crate::utils::dir_ops::sync_dir;
use crate::utils::file_wal::FileWal;
use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
#[error("`GrowingSegment` stopped growing.")]
pub struct GrowingSegmentInsertError;

pub struct GrowingSegment {
    uuid: Uuid,
    options: VectorOptions,
    vec: Vec<UnsafeCell<MaybeUninit<Log>>>,
    wal: Mutex<FileWal>,
    len: AtomicUsize,
    pro: Mutex<Protect>,
    _tracker: Arc<SegmentTracker>,
}

impl GrowingSegment {
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
            options: options.vector,
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
        options: IndexOptions,
    ) -> Arc<Self> {
        let mut wal = FileWal::open(path.join("wal"));
        let mut vec = Vec::new();
        while let Some(log) = wal.read() {
            let log = bincode::deserialize::<Log>(&log).unwrap();
            vec.push(UnsafeCell::new(MaybeUninit::new(log)));
        }
        wal.truncate();
        let n = vec.len();
        Arc::new(Self {
            uuid,
            options: options.vector,
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
        vector: Vec<Scalar>,
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
            .write(&bincode::serialize::<Log>(&log).unwrap());
        Ok(())
    }
    pub fn len(&self) -> u32 {
        self.len.load(Ordering::Acquire) as u32
    }
    pub fn vector(&self, i: u32) -> &[Scalar] {
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
    pub fn search(&self, k: usize, vector: &[Scalar], filter: &mut impl Filter) -> Heap {
        let n = self.len.load(Ordering::Acquire);
        let mut heap = Heap::new(k);
        for i in 0..n {
            let log = unsafe { (*self.vec[i].get()).assume_init_ref() };
            let distance = self.options.d.distance(vector, &log.vector);
            if heap.check(distance) && filter.check(log.payload) {
                heap.push(HeapElement {
                    distance,
                    payload: log.payload,
                });
            }
        }
        heap
    }
}

unsafe impl Send for GrowingSegment {}
unsafe impl Sync for GrowingSegment {}

impl Drop for GrowingSegment {
    fn drop(&mut self) {
        let n = *self.len.get_mut();
        for i in 0..n {
            unsafe {
                self.vec[i].get_mut().assume_init_drop();
            }
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Log {
    vector: Vec<Scalar>,
    payload: Payload,
}

#[derive(Debug, Clone)]
struct Protect {
    inflight: usize,
    capacity: usize,
}
