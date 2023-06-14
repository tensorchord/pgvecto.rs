use std::ops::{Deref, DerefMut};

pub struct Semaphore<T> {
    tx: crossbeam::channel::Sender<T>,
    rx: crossbeam::channel::Receiver<T>,
}

impl<T> Semaphore<T> {
    pub fn new() -> Self {
        let (tx, rx) = crossbeam::channel::unbounded();
        Self { tx, rx }
    }
    pub fn push(&self, data: T) {
        let _ = self.tx.send(data);
    }
    pub fn acquire(&self) -> SemaphoreGuard<T> {
        SemaphoreGuard {
            data: Some(self.rx.recv().unwrap()),
            tx: self.tx.clone(),
        }
    }
}

pub struct SemaphoreGuard<T> {
    data: Option<T>,
    tx: crossbeam::channel::Sender<T>,
}

impl<T> Deref for SemaphoreGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.data.as_ref().unwrap()
    }
}

impl<T> DerefMut for SemaphoreGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data.as_mut().unwrap()
    }
}

impl<T> Drop for SemaphoreGuard<T> {
    fn drop(&mut self) {
        let _ = self.tx.send(self.data.take().unwrap());
    }
}
