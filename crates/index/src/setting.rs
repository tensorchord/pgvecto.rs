use super::Index;
use crate::optimizing::indexing::OptimizerIndexing;
use crate::Op;
use base::index::IndexFlexibleOptions;
use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender};
use std::convert::Infallible;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct FlexibleOptionSyncing<O: Op> {
    index: Arc<Index<O>>,
}

impl<O: Op> FlexibleOptionSyncing<O> {
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
    pub fn main(self, shutdown: Receiver<Infallible>) {
        let index = &self.index;
        let mut old_options = index.flexible.clone();
        let dur = Duration::from_secs(5);
        loop {
            let new_options = index.flexible.clone();
            if new_options != old_options {
                std::fs::write(
                    index.path.join("flexible"),
                    serde_json::to_string::<IndexFlexibleOptions>(&new_options).unwrap(),
                )
                .unwrap();
                old_options = new_options.clone();
            }
            self.update_optimizing_threads(new_options.clone(), old_options.clone());
            match shutdown.recv_timeout(dur) {
                Ok(never) => match never {},
                Err(RecvTimeoutError::Disconnected) => return,
                Err(RecvTimeoutError::Timeout) => continue,
            }
        }
    }
    fn update_optimizing_threads(&self, new: IndexFlexibleOptions, old: IndexFlexibleOptions) {
        if !new.optimizing_threads_eq(&old) {
            let mut background = self.index.background_indexing.lock();
            if let Some((sender, join_handle)) = background.take() {
                drop(sender);
                let _ = join_handle.join();
                *background = Some(OptimizerIndexing::new(self.index.clone()).spawn());
            }
        }
    }
}
