use crate::Index;
use crate::Op;
pub use base::distance::*;
pub use base::index::*;
pub use base::search::*;
pub use base::vector::*;
use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender};
use std::convert::Infallible;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

pub struct OptimizerSealing<O: Op> {
    index: Arc<Index<O>>,
}

impl<O: Op> OptimizerSealing<O> {
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
    fn main(self, shutdown: Receiver<Infallible>) {
        let index = self.index;
        let dur = Duration::from_secs(index.options.optimizing.sealing_secs);
        let least = index.options.optimizing.sealing_size;
        let mut check = None;
        loop {
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
            match shutdown.recv_timeout(dur) {
                Ok(never) => match never {},
                Err(RecvTimeoutError::Disconnected) => return,
                Err(RecvTimeoutError::Timeout) => continue,
            }
        }
    }
}
