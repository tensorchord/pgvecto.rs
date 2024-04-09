pub mod index_source;
pub mod indexing;

use self::indexing::{make, scan};
use crate::Index;
use crate::Op;
use crossbeam::channel::{bounded, Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

pub struct Optimizing<O: Op> {
    index: Arc<Index<O>>,
}

impl<O: Op> Optimizing<O> {
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
        let mut tasks = BTreeMap::<Instant, Box<dyn FnMut() -> Instant>>::new();
        tasks.insert(Instant::now(), {
            let index = index.clone();
            let mut check = None;
            let mut first = true;
            Box::new(move || {
                let view = index.view();
                let stamp = view
                    .write
                    .as_ref()
                    .map(|(uuid, segment)| (*uuid, segment.len()));
                if first || stamp == check {
                    if let Some((uuid, len)) = stamp {
                        if len >= view.alterable_options.optimizing.sealing_size {
                            index.seal(uuid);
                        }
                    }
                } else {
                    check = stamp;
                }
                first = false;
                Instant::now() + Duration::from_secs(view.alterable_options.optimizing.sealing_secs)
            })
        });
        tasks.insert(
            Instant::now(),
            Box::new(|| {
                let view = index.view();
                if let Some(source) = scan(index.clone()) {
                    rayon::ThreadPoolBuilder::new()
                        .num_threads(view.alterable_options.optimizing.optimizing_threads as usize)
                        .build_scoped(|pool| {
                            let (stop_tx, stop_rx) = bounded::<Infallible>(0);
                            std::thread::scope(|scope| {
                                scope.spawn(|| {
                                    let stop_rx = stop_rx;
                                    loop {
                                        match stop_rx.try_recv() {
                                            Ok(never) => match never {},
                                            Err(TryRecvError::Empty) => (),
                                            Err(TryRecvError::Disconnected) => return,
                                        }
                                        match shutdown.recv_timeout(Duration::from_secs(1)) {
                                            Ok(never) => match never {},
                                            Err(RecvTimeoutError::Timeout) => (),
                                            Err(RecvTimeoutError::Disconnected) => {
                                                pool.stop();
                                                return;
                                            }
                                        }
                                    }
                                });
                                scope.spawn(|| {
                                    let _stop_tx = stop_tx;
                                    pool.install(|| make(index.clone(), source));
                                });
                            })
                        })
                        .unwrap();
                    Instant::now()
                } else {
                    index.instant_indexed.store(Instant::now());
                    Instant::now() + Duration::from_secs(60)
                }
            }),
        );
        loop {
            while let Some(e) = tasks.first_entry() {
                if *e.key() < Instant::now() {
                    let mut task = e.remove();
                    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(&mut task)) {
                        Ok(instant) => {
                            tasks.insert(instant, task);
                        }
                        Err(e) => {
                            log::error!("index task panickied: {:?}", e);
                        }
                    }
                } else {
                    break;
                }
            }
            if let Some(e) = tasks.first_entry() {
                match shutdown.recv_deadline(*e.key()) {
                    Ok(never) => match never {},
                    Err(RecvTimeoutError::Disconnected) => return,
                    Err(RecvTimeoutError::Timeout) => (),
                }
            } else {
                break;
            }
        }
    }
}
