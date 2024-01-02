use crate::ipc::{server::RpcHandler, IpcError};
use service::worker::Worker;
use std::sync::Arc;

pub fn normal(worker: Arc<Worker>) {
    std::thread::scope(|scope| {
        scope.spawn({
            let worker = worker.clone();
            move || {
                for rpc_handler in crate::ipc::listen_unix() {
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
        });
        scope.spawn({
            let worker = worker.clone();
            move || {
                for rpc_handler in crate::ipc::listen_mmap() {
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
    });
}

fn session(worker: Arc<Worker>, mut handler: RpcHandler) -> Result<(), IpcError> {
    use crate::ipc::server::RpcHandle;
    loop {
        match handler.handle()? {
            RpcHandle::Create { handle, options, x } => {
                worker.call_create(handle, options);
                handler = x.leave()?;
            }
            RpcHandle::Insert { handle, insert, x } => match worker.call_insert(handle, insert) {
                Ok(()) => handler = x.leave()?,
                Err(res) => x.reset(res)?,
            },
            RpcHandle::Delete { handle, mut x } => {
                match worker.call_delete(handle, |p| x.next(p).unwrap()) {
                    Ok(()) => handler = x.leave()?,
                    Err(res) => x.reset(res)?,
                }
            }
            RpcHandle::Search {
                handle,
                search,
                prefilter: true,
                gucs,
                mut x,
            } => match worker.call_search(handle, search, gucs, |p| x.check(p).unwrap()) {
                Ok(res) => handler = x.leave(res)?,
                Err(e) => x.reset(e)?,
            },
            RpcHandle::Search {
                handle,
                search,
                prefilter: false,
                gucs,
                x,
            } => match worker.call_search(handle, search, gucs, |_| true) {
                Ok(res) => handler = x.leave(res)?,
                Err(e) => x.reset(e)?,
            },
            RpcHandle::Flush { handle, x } => match worker.call_flush(handle) {
                Ok(()) => handler = x.leave()?,
                Err(e) => x.reset(e)?,
            },
            RpcHandle::Destory { handle, x } => {
                worker.call_destory(handle);
                handler = x.leave()?;
            }
            RpcHandle::Stat { handle, x } => match worker.call_stat(handle) {
                Ok(res) => handler = x.leave(res)?,
                Err(e) => x.reset(e)?,
            },
            RpcHandle::Vbase { handle, vbase, x } => {
                use crate::ipc::server::VbaseHandle::*;
                let instance = match worker.get_instance(handle) {
                    Ok(x) => x,
                    Err(e) => x.reset(e)?,
                };
                let view = match instance.view() {
                    Ok(x) => x,
                    Err(e) => x.reset(e)?,
                };
                let mut it = match view.vbase(vbase.0, vbase.1) {
                    Ok(x) => x,
                    Err(e) => x.reset(e)?,
                };
                let mut x = x.error()?;
                loop {
                    match x.handle()? {
                        Next { x: y } => {
                            x = y.leave(it.next())?;
                        }
                        Leave { x } => {
                            handler = x;
                            break;
                        }
                    }
                }
            }
        }
    }
}
