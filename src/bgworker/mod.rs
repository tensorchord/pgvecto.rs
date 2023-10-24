pub mod filter_delete;
pub mod index;
pub mod storage;
pub mod storage_mmap;
pub mod vectors;
pub mod wal;

use self::index::IndexError;
use crate::ipc::server::RpcHandler;
use crate::ipc::ServerIpcError;
use crate::prelude::*;
use dashmap::DashMap;
use index::Index;
use std::fs::OpenOptions;
use std::mem::MaybeUninit;
use thiserror::Error;

#[no_mangle]
extern "C" fn vectors_main(_arg: pgrx::pg_sys::Datum) -> ! {
    match std::panic::catch_unwind(thread_main) {
        Ok(never) => never,
        Err(_) => {
            log::error!("The background process crashed.");
            pgrx::PANIC!("The background process crashed.");
        }
    }
}

fn thread_main() -> ! {
    std::fs::create_dir_all("pg_vectors").expect("Failed to create the directory.");
    std::env::set_current_dir("pg_vectors").expect("Failed to set the current variable.");
    unsafe {
        INDEXES.as_mut_ptr().write(DashMap::new());
    }
    let logging = OpenOptions::new()
        .create(true)
        .append(true)
        .open("_log")
        .expect("The logging file is failed to open.");
    env_logger::builder()
        .target(env_logger::Target::Pipe(Box::new(logging)))
        .init();
    std::panic::set_hook(Box::new(|info| {
        let backtrace = std::backtrace::Backtrace::capture();
        log::error!("Process panickied. {:?}. Backtrace. {}.", info, backtrace);
    }));
    std::thread::spawn(|| thread_listening());
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
        std::thread::yield_now();
    }
}

static mut INDEXES: MaybeUninit<DashMap<Id, Index>> = MaybeUninit::uninit();

fn thread_listening() {
    let listener = crate::ipc::listen();
    for rpc_handler in listener {
        std::thread::spawn(move || {
            if let Err(e) = thread_session(rpc_handler) {
                log::error!("Session exited. {}.", e);
            }
        });
    }
}

#[derive(Debug, Clone, Error)]
pub enum SessionError {
    #[error("Ipc")]
    Ipc(#[from] ServerIpcError),
    #[error("Index")]
    Index(#[from] IndexError),
}

fn thread_session(mut rpc_handler: RpcHandler) -> Result<(), SessionError> {
    use crate::ipc::server::RpcHandle;
    loop {
        match rpc_handler.handle()? {
            RpcHandle::Build { id, options, mut x } => {
                log::debug!("Handle build rpc. id: {}, options: {:?}.", id, options);
                use dashmap::mapref::entry::Entry;
                let indexes = unsafe { INDEXES.assume_init_ref() };
                match indexes.entry(id) {
                    Entry::Occupied(entry) => entry.into_ref(),
                    Entry::Vacant(entry) => {
                        let index = Index::build(id, options, &mut x)?;
                        entry.insert(index)
                    }
                };
                rpc_handler = x.leave()?;
            }
            RpcHandle::Insert { id, insert, x } => {
                log::debug!("Handle insert rpc. id: {}, insert: {:?}.", id, insert);
                let indexes = unsafe { INDEXES.assume_init_ref() };
                let index = indexes.get(&id).expect("Not load.");
                index.insert(insert)?;
                rpc_handler = x.leave()?;
            }
            RpcHandle::Delete { id, mut x } => {
                log::debug!("Handle delete rpc. id: {}.", id);
                let indexes = unsafe { INDEXES.assume_init_ref() };
                let index = indexes.get(&id).expect("Not load.");
                index.delete(&mut x)?;
                rpc_handler = x.leave()?;
            }
            RpcHandle::Search {
                id,
                target,
                k,
                mut x,
            } => {
                log::debug!(
                    "Handle search rpc. id: {}, target: {:?}, k: {:?}.",
                    id,
                    target,
                    k
                );
                let indexes = unsafe { INDEXES.assume_init_ref() };
                let index = indexes.get(&id).expect("Not load.");
                let result = index.search(target, k, &mut x)?;
                rpc_handler = x.leave(result)?;
            }
            RpcHandle::Load { id, x } => {
                log::debug!("Handle load rpc. id: {}.", id);
                use dashmap::mapref::entry::Entry;
                let indexes: &DashMap<Id, Index> = unsafe { INDEXES.assume_init_ref() };
                match indexes.entry(id) {
                    Entry::Occupied(entry) => entry.into_ref(),
                    Entry::Vacant(entry) => {
                        let index = Index::load(id);
                        entry.insert(index)
                    }
                };
                rpc_handler = x.leave()?;
            }
            RpcHandle::Unload { id, x } => {
                log::debug!("Handle unload rpc. id: {}.", id);
                use dashmap::mapref::entry::Entry;
                let indexes: &DashMap<Id, Index> = unsafe { INDEXES.assume_init_ref() };
                match indexes.entry(id) {
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().shutdown();
                        entry.remove();
                    }
                    Entry::Vacant(_) => (),
                };
                rpc_handler = x.leave()?;
            }
            RpcHandle::Flush { id, x } => {
                log::debug!("Handle flush rpc. id: {}.", id);
                let indexes = unsafe { INDEXES.assume_init_ref() };
                let index = indexes.get(&id).expect("Not load.");
                index.flush();
                rpc_handler = x.leave()?;
            }
            RpcHandle::Clean { id, x } => {
                log::debug!("Handle clean rpc. id: {}.", id);
                use dashmap::mapref::entry::Entry;
                let indexes: &DashMap<Id, Index> = unsafe { INDEXES.assume_init_ref() };
                match indexes.entry(id) {
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().shutdown();
                        entry.remove();
                        Index::clean(id);
                    }
                    Entry::Vacant(_entry) => {
                        Index::clean(id);
                    }
                };
                rpc_handler = x.leave()?;
            }
            RpcHandle::Leave {} => {
                log::debug!("Handle leave rpc.");
                break;
            }
        }
    }
    Ok(())
}
