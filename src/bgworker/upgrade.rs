use crate::ipc::server::RpcHandler;
use crate::ipc::ConnectionError;
use service::prelude::*;

pub fn upgrade() {
    std::thread::scope(|scope| {
        scope.spawn({
            move || {
                for rpc_handler in crate::ipc::listen_unix() {
                    std::thread::spawn({
                        move || {
                            log::trace!("Session established.");
                            let _ = session(rpc_handler);
                            log::trace!("Session closed.");
                        }
                    });
                }
            }
        });
        scope.spawn({
            move || {
                for rpc_handler in crate::ipc::listen_mmap() {
                    std::thread::spawn({
                        move || {
                            log::trace!("Session established.");
                            let _ = session(rpc_handler);
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

fn session(handler: RpcHandler) -> Result<(), ConnectionError> {
    use crate::ipc::server::RpcHandle;
    let mut handler = handler;
    loop {
        match handler.handle()? {
            RpcHandle::Commit { x, .. } => {
                handler = x.leave()?;
            }
            RpcHandle::Abort { x, .. } => {
                handler = x.leave()?;
            }
            RpcHandle::Create { x, .. } => x.reset(ServiceError::Upgrade)?,
            RpcHandle::Insert { x, .. } => x.reset(ServiceError::Upgrade)?,
            RpcHandle::Delete { x, .. } => x.reset(ServiceError::Upgrade)?,
            RpcHandle::Stat { x, .. } => x.reset(ServiceError::Upgrade)?,
            RpcHandle::Basic { x, .. } => x.reset(ServiceError::Upgrade)?,
            RpcHandle::Vbase { x, .. } => x.reset(ServiceError::Upgrade)?,
            RpcHandle::Upgrade { x } => {
                let _ = std::fs::remove_dir_all("./pg_vectors");
                handler = x.leave()?;
            }
        }
    }
}
