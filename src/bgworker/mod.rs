mod index;
mod session;
mod wal;

pub use session::Client;

use self::index::Load;
use crate::postgres::gucs::BGWORKER_PORT;
use crate::prelude::Id;
use dashmap::DashMap;
use index::Index;
use std::fs::OpenOptions;
use std::mem::MaybeUninit;
use tokio::sync::RwLock;

struct Global {
    indexes: DashMap<Id, &'static RwLock<Load<Index>>>,
}

static mut GLOBAL: MaybeUninit<Global> = MaybeUninit::uninit();

#[no_mangle]
extern "C" fn pgvectors_main(_arg: pgrx::pg_sys::Datum) -> ! {
    match std::panic::catch_unwind(|| {
        std::fs::create_dir_all("pg_vectors").expect("Failed to create the directory.");
        std::env::set_current_dir("pg_vectors").expect("Failed to set the current variable.");
        unsafe {
            let global = Global {
                indexes: DashMap::new(),
            };
            (GLOBAL.as_ptr() as *mut Global).write(global);
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
            log::error!("The background process panickied. {:?}", info);
        }));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("The tokio runtime is failed to build.");
        let listener = runtime
            .block_on(async {
                tokio::net::TcpListener::bind(("0.0.0.0", BGWORKER_PORT.get() as u16)).await
            })
            .expect("The listening port is failed to bind.");
        runtime.spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::task::spawn(async move {
                    if let Err(e) = session::server_main(stream).await {
                        log::error!("Session panickied. {}", e);
                    }
                });
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
            std::thread::yield_now();
        }
    }) {
        Ok(never) => never,
        Err(_) => {
            log::error!("The background process crashed.");
            pgrx::PANIC!("The background process crashed.");
        }
    }
}

fn global() -> &'static Global {
    unsafe { GLOBAL.assume_init_ref() }
}

async fn find_index(id: Id) -> anyhow::Result<&'static RwLock<Load<Index>>> {
    use dashmap::mapref::entry::Entry;
    match global().indexes.try_entry(id).unwrap() {
        Entry::Occupied(x) => Ok(x.get()),
        Entry::Vacant(x) => {
            let reference = Box::leak(Box::new(RwLock::new(Load::new())));
            x.insert(reference);
            Ok(reference)
        }
    }
}
