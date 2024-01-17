use crate::ipc::server::RpcHandler;
use crate::ipc::ConnectionError;
use service::index::OutdatedError;
use service::prelude::ServiceError;
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

fn session(worker: Arc<Worker>, handler: RpcHandler) -> Result<!, ConnectionError> {
    use crate::ipc::server::RpcHandle;
    let mut handler = handler;
    loop {
        match handler.handle()? {
            // transaction
            RpcHandle::Commit {
                pending_deletes,
                pending_dirty,
                x,
            } => {
                let view = worker.view();
                for handle in pending_deletes {
                    worker.instance_destroy(handle);
                }
                for handle in pending_dirty {
                    if let Some(instance) = view.get(handle) {
                        if let Some(view) = instance.view() {
                            view.flush();
                        }
                    }
                }
                handler = x.leave()?;
            }
            RpcHandle::Abort { pending_deletes, x } => {
                for handle in pending_deletes {
                    worker.instance_destroy(handle);
                }
                handler = x.leave()?;
            }
            RpcHandle::Create { handle, options, x } => {
                match worker.instance_create(handle, options) {
                    Ok(()) => (),
                    Err(e) => x.reset(e)?,
                };
                handler = x.leave()?;
            }
            // instance
            RpcHandle::Insert {
                handle,
                vector,
                pointer,
                x,
            } => {
                let view = worker.view();
                let Some(instance) = view.get(handle) else {
                    x.reset(ServiceError::UnknownIndex)?;
                };
                loop {
                    let instance_view = match instance.view() {
                        Some(x) => x,
                        None => x.reset(ServiceError::Upgrade2)?,
                    };
                    match instance_view.insert(vector.clone(), pointer) {
                        Ok(Ok(())) => break,
                        Ok(Err(OutdatedError)) => instance.refresh(),
                        Err(e) => x.reset(e)?,
                    }
                }
                handler = x.leave()?;
            }
            RpcHandle::Delete { handle, mut x } => {
                let view = worker.view();
                let Some(instance) = view.get(handle) else {
                    x.reset(ServiceError::UnknownIndex)?;
                };
                let instance_view = match instance.view() {
                    Some(x) => x,
                    None => x.reset(ServiceError::Upgrade2)?,
                };
                instance_view.delete(|p| x.next(p).expect("Panic in VACUUM."));
                handler = x.leave()?;
            }
            RpcHandle::Stat { handle, x } => {
                let view = worker.view();
                let Some(instance) = view.get(handle) else {
                    x.reset(ServiceError::UnknownIndex)?;
                };
                let r = instance.stat();
                handler = x.leave(r)?
            }
            RpcHandle::Basic {
                handle,
                vector,
                opts,
                x,
            } => {
                use crate::ipc::server::BasicHandle::*;
                let view = worker.view();
                let Some(instance) = view.get(handle) else {
                    x.reset(ServiceError::UnknownIndex)?;
                };
                let view = match instance.view() {
                    Some(x) => x,
                    None => x.reset(ServiceError::Upgrade2)?,
                };
                let mut it = match view.basic(&vector, &opts, |_| true) {
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
            RpcHandle::Vbase {
                handle,
                vector,
                opts,
                x,
            } => {
                use crate::ipc::server::VbaseHandle::*;
                let view = worker.view();
                let Some(instance) = view.get(handle) else {
                    x.reset(ServiceError::UnknownIndex)?;
                };
                let view = match instance.view() {
                    Some(x) => x,
                    None => x.reset(ServiceError::Upgrade2)?,
                };
                let mut it = match view.vbase(&vector, &opts, |_| true) {
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
