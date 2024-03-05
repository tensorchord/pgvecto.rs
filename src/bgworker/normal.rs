use crate::ipc::ConnectionError;
use crate::ipc::{listen_mmap, listen_unix};
use crate::ipc::{ServerRpcHandle, ServerRpcHandler};
use service::Worker;
use std::convert::Infallible;
use std::sync::Arc;

pub fn normal(worker: Arc<Worker>) {
    std::thread::scope(|scope| {
        scope.spawn({
            let worker = worker.clone();
            move || {
                for rpc_handler in listen_unix() {
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
                for rpc_handler in listen_mmap() {
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
                libc::sigaddset(&mut set, libc::SIGQUIT);
                libc::sigaddset(&mut set, libc::SIGTERM);
                libc::sigwait(&set, &mut sig);
            }
            match sig {
                libc::SIGQUIT => {
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

fn session(worker: Arc<Worker>, handler: ServerRpcHandler) -> Result<Infallible, ConnectionError> {
    use base::worker::*;
    let mut handler = handler;
    loop {
        match handler.handle()? {
            // control plane
            ServerRpcHandle::Create { handle, options, x } => {
                handler = x.leave(WorkerOperations::create(worker.as_ref(), handle, options))?;
            }
            ServerRpcHandle::Drop { handle, x } => {
                handler = x.leave(WorkerOperations::drop(worker.as_ref(), handle))?;
            }
            // data plane
            ServerRpcHandle::Flush { handle, x } => {
                handler = x.leave(worker.flush(handle))?;
            }
            ServerRpcHandle::Insert {
                handle,
                vector,
                pointer,
                x,
            } => {
                handler = x.leave(worker.insert(handle, vector, pointer))?;
            }
            ServerRpcHandle::Delete { handle, pointer, x } => {
                handler = x.leave(worker.delete(handle, pointer))?;
            }
            ServerRpcHandle::Stat { handle, x } => {
                handler = x.leave(worker.stat(handle))?;
            }
            ServerRpcHandle::Basic {
                handle,
                vector,
                opts,
                x,
            } => {
                let v = match worker.view_basic(handle) {
                    Ok(x) => x,
                    Err(e) => {
                        handler = x.error_err(e)?;
                        continue;
                    }
                };
                match v.basic(&vector, &opts, |_| true) {
                    Ok(mut iter) => {
                        use crate::ipc::ServerBasicHandle;
                        let mut x = x.error_ok()?;
                        loop {
                            match x.handle()? {
                                ServerBasicHandle::Next { x: y } => {
                                    x = y.leave(iter.next())?;
                                }
                                ServerBasicHandle::Leave { x } => {
                                    handler = x;
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => handler = x.error_err(e)?,
                };
            }
            ServerRpcHandle::Vbase {
                handle,
                vector,
                opts,
                x,
            } => {
                let v = match worker.view_vbase(handle) {
                    Ok(x) => x,
                    Err(e) => {
                        handler = x.error_err(e)?;
                        continue;
                    }
                };
                match v.vbase(&vector, &opts, |_| true) {
                    Ok(mut iter) => {
                        use crate::ipc::ServerVbaseHandle;
                        let mut x = x.error_ok()?;
                        loop {
                            match x.handle()? {
                                ServerVbaseHandle::Next { x: y } => {
                                    x = y.leave(iter.next())?;
                                }
                                ServerVbaseHandle::Leave { x } => {
                                    handler = x;
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => handler = x.error_err(e)?,
                };
            }
            ServerRpcHandle::List { handle, x } => {
                let v = match worker.view_list(handle) {
                    Ok(x) => x,
                    Err(e) => {
                        handler = x.error_err(e)?;
                        continue;
                    }
                };
                match v.list() {
                    Ok(mut iter) => {
                        use crate::ipc::ServerListHandle;
                        let mut x = x.error_ok()?;
                        loop {
                            match x.handle()? {
                                ServerListHandle::Next { x: y } => {
                                    x = y.leave(iter.next())?;
                                }
                                ServerListHandle::Leave { x } => {
                                    handler = x;
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => handler = x.error_err(e)?,
                };
            }
        }
    }
}
