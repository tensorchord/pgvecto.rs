use crate::ipc::server::RpcHandler;
use crate::ipc::IpcError;
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

fn session(handler: RpcHandler) -> Result<(), IpcError> {
    use crate::ipc::server::RpcHandle;
    match handler.handle()? {
        RpcHandle::Create { x, .. } => x.reset(FriendlyError::Upgrade)?,
        RpcHandle::Search { x, .. } => x.reset(FriendlyError::Upgrade)?,
        RpcHandle::Insert { x, .. } => x.reset(FriendlyError::Upgrade)?,
        RpcHandle::Delete { x, .. } => x.reset(FriendlyError::Upgrade)?,
        RpcHandle::Flush { x, .. } => x.reset(FriendlyError::Upgrade)?,
        RpcHandle::Destory { x, .. } => x.reset(FriendlyError::Upgrade)?,
        RpcHandle::Stat { x, .. } => x.reset(FriendlyError::Upgrade)?,
        RpcHandle::Vbase { x, .. } => x.reset(FriendlyError::Upgrade)?,
    }
}
