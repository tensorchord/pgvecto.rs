pub mod worker;

use self::worker::Worker;
use crate::ipc::server::RpcHandler;
use crate::ipc::IpcError;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn main() {
    {
        let mut builder = env_logger::builder();
        builder.target(env_logger::Target::Stderr);
        #[cfg(not(debug_assertions))]
        {
            builder.filter(None, log::LevelFilter::Info);
        }
        #[cfg(debug_assertions)]
        {
            builder.filter(None, log::LevelFilter::Trace);
        }
        builder.init();
    }
    std::panic::set_hook(Box::new(|info| {
        let backtrace;
        #[cfg(not(debug_assertions))]
        {
            backtrace = std::backtrace::Backtrace::capture();
        }
        #[cfg(debug_assertions)]
        {
            backtrace = std::backtrace::Backtrace::force_capture();
        }
        log::error!("Panickied. Info: {:?}. Backtrace: {}.", info, backtrace);
    }));
    let worker = if Path::new("pg_vectors").try_exists().unwrap() {
        Worker::open(PathBuf::from("pg_vectors"))
    } else {
        Worker::create(PathBuf::from("pg_vectors"))
    };
    std::thread::spawn({
        let worker = worker.clone();
        move || listen(crate::ipc::listen_unix(), worker)
    });
    std::thread::spawn({
        let worker = worker.clone();
        move || listen(crate::ipc::listen_mmap(), worker)
    });
    loop {
        let mut sig: i32 = 0;
        unsafe {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGHUP);
            libc::sigaddset(&mut set, libc::SIGTERM);
            libc::sigwait(&set, &mut sig);
        }
        match sig {
            libc::SIGHUP => {
                std::process::exit(0);
            }
            libc::SIGTERM => {
                std::process::exit(0);
            }
            _ => (),
        }
    }
}

fn listen(listen: impl Iterator<Item = RpcHandler>, worker: Arc<Worker>) {
    for rpc_handler in listen {
        let worker = worker.clone();
        std::thread::spawn({
            move || {
                log::trace!("Session established.");
                let _ = session(worker, rpc_handler);
                log::trace!("Session closed.");
            }
        });
    }
}

fn session(worker: Arc<Worker>, mut handler: RpcHandler) -> Result<(), IpcError> {
    use crate::ipc::server::RpcHandle;
    loop {
        match handler.handle()? {
            RpcHandle::Create { id, options, x } => {
                worker.call_create(id, options);
                handler = x.leave()?;
            }
            RpcHandle::Insert { id, insert, x } => {
                let res = worker.call_insert(id, insert);
                handler = x.leave(res)?;
            }
            RpcHandle::Delete { id, mut x } => {
                let res = worker.call_delete(id, |p| x.next(p).unwrap());
                handler = x.leave(res)?;
            }
            RpcHandle::Search {
                id,
                search,
                prefilter,
                mut x,
            } => {
                if prefilter {
                    let res = worker.call_search(id, search, |p| x.check(p).unwrap());
                    handler = x.leave(res)?;
                } else {
                    let res = worker.call_search(id, search, |_| true);
                    handler = x.leave(res)?;
                }
            }
            RpcHandle::Flush { id, x } => {
                let result = worker.call_flush(id);
                handler = x.leave(result)?;
            }
            RpcHandle::Destory { id, x } => {
                worker.call_destory(id);
                handler = x.leave()?;
            }
            RpcHandle::Stat { id, x } => {
                let result = worker.call_stat(id);
                handler = x.leave(result)?;
            }
            RpcHandle::Leave {} => {
                log::debug!("Handle leave rpc.");
                break;
            }
        }
    }
    Ok(())
}
