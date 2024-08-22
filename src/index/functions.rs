use super::utils::from_oid_to_handle;
use crate::error::check_client;
use crate::ipc::client;
use base::index::StatError;
use pgrx::pg_sys::Oid;

#[pgrx::pg_extern(volatile, strict, parallel_safe)]
fn _vectors_pgvectors_upgrade() {
    if crate::bgworker::is_started() {
        return;
    }
    let _ = std::fs::remove_dir_all("pg_vectors");
}

#[pgrx::pg_extern(volatile, strict, parallel_safe)]
fn _vectors_fence_vector_index(oid: Oid) {
    let handle = from_oid_to_handle(oid);
    let mut rpc = check_client(client());
    loop {
        pgrx::check_for_interrupts!();
        match rpc.stat(handle) {
            Ok(s) => {
                if !s.indexing {
                    break;
                }
            }
            Err(StatError::NotExist) => pgrx::error!("internal error"),
        }
        unsafe {
            pgrx::pg_sys::WaitLatch(
                pgrx::pg_sys::MyLatch,
                (pgrx::pg_sys::WL_LATCH_SET
                    | pgrx::pg_sys::WL_TIMEOUT
                    | pgrx::pg_sys::WL_EXIT_ON_PM_DEATH) as _,
                1000,
                pgrx::pg_sys::WaitEventTimeout::WAIT_EVENT_PG_SLEEP,
            );
            pgrx::pg_sys::ResetLatch(pgrx::pg_sys::MyLatch);
        }
    }
}
