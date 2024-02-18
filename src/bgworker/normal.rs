use crate::ipc::ConnectionError;
use crate::ipc::ServerRpcHandler;
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

fn session(worker: Arc<Worker>, handler: ServerRpcHandler) -> Result<!, ConnectionError> {
    use crate::ipc::ServerRpcHandle;
    let mut handler = handler;
    loop {
        match handler.handle()? {
            // control plane
            ServerRpcHandle::Create { handle, options, x } => {
                handler = x.leave(worker._create(handle, options))?;
            }
            ServerRpcHandle::Drop { handle, x } => {
                handler = x.leave(worker._drop(handle))?;
            }
            // data plane
            ServerRpcHandle::Flush { handle, x } => {
                handler = x.leave(worker._flush(handle))?;
            }
            ServerRpcHandle::Insert {
                handle,
                vector,
                pointer,
                x,
            } => {
                handler = x.leave(worker._insert(handle, vector, pointer))?;
            }
            ServerRpcHandle::Delete { handle, pointer, x } => {
                handler = x.leave(worker._delete(handle, pointer))?;
            }
            ServerRpcHandle::Stat { handle, x } => {
                handler = x.leave(worker._stat(handle))?;
            }
            ServerRpcHandle::Basic {
                handle,
                vector,
                opts,
                x,
            } => {
                let v = match worker._basic_view(handle) {
                    Ok(x) => x,
                    Err(e) => {
                        handler = x.error_err(e)?;
                        continue;
                    }
                };
                match v._basic(&vector, &opts, |_| true) {
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
                let v = match worker._vbase_view(handle) {
                    Ok(x) => x,
                    Err(e) => {
                        handler = x.error_err(e)?;
                        continue;
                    }
                };
                match v._vbase(&vector, &opts, |_| true) {
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
                let v = match worker._list_view(handle) {
                    Ok(x) => x,
                    Err(e) => {
                        handler = x.error_err(e)?;
                        continue;
                    }
                };
                match v._list() {
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
