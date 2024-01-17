use crate::index::Index;
use crate::prelude::*;
use std::sync::Arc;
use std::time::Duration;

pub struct OptimizerSealing<S: G> {
    index: Arc<Index<S>>,
}

impl<S: G> OptimizerSealing<S> {
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
        let dur = Duration::from_secs(index.options.optimizing.sealing_secs);
        let least = index.options.optimizing.sealing_size;
        let weak_index = Arc::downgrade(&index);
        drop(index);
        let mut check = None;
        loop {
            {
                let Some(index) = weak_index.upgrade() else {
                    return;
                };
                let view = index.view();
                let stamp = view
                    .write
                    .as_ref()
                    .map(|(uuid, segment)| (*uuid, segment.len()));
                if stamp == check {
                    if let Some((uuid, len)) = stamp {
                        if len >= least {
                            index.seal(uuid);
                        }
                    }
                } else {
                    check = stamp;
                }
            }
            std::thread::sleep(dur);
        }
    }
}
