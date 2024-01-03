use crate::prelude::*;
use service::prelude::*;

static mut PREV_EXECUTOR_START: pgrx::pg_sys::ExecutorStart_hook_type = None;

#[pgrx::pg_guard]
unsafe extern "C" fn vectors_executor_start(
    query_desc: *mut pgrx::pg_sys::QueryDesc,
    eflags: ::std::os::raw::c_int,
) {
    if let Some(prev_executor_start) = PREV_EXECUTOR_START {
        prev_executor_start(query_desc, eflags);
    } else {
        pgrx::pg_sys::standard_ExecutorStart(query_desc, eflags);
    }
    super::hook_executor::post_executor_start(query_desc);
}

#[pgrx::pg_guard]
unsafe extern "C" fn xact_callback(event: pgrx::pg_sys::XactEvent, _data: pgrx::void_mut_ptr) {
    match event {
        pgrx::pg_sys::XactEvent_XACT_EVENT_ABORT => {
            super::hook_transaction::aborting();
        }
        pgrx::pg_sys::XactEvent_XACT_EVENT_PRE_COMMIT => {
            xact_delete();
            super::hook_transaction::committing();
        }
        _ => {}
    }
}

pub unsafe fn init() {
    PREV_EXECUTOR_START = pgrx::pg_sys::ExecutorStart_hook;
    pgrx::pg_sys::ExecutorStart_hook = Some(vectors_executor_start);
    pgrx::pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
}

#[cfg(any(feature = "pg12", feature = "pg13", feature = "pg14", feature = "pg15"))]
unsafe fn xact_delete() {
    let mut ptr: *mut pgrx::pg_sys::RelFileNode = std::ptr::null_mut();
    let n = pgrx::pg_sys::smgrGetPendingDeletes(true, &mut ptr as *mut _);
    if n > 0 {
        let nodes = std::slice::from_raw_parts(ptr, n as usize);
        let handles = nodes
            .iter()
            .map(|node| Handle::from_sys(node.relNode))
            .collect::<Vec<_>>();
        let mut rpc = crate::ipc::client::borrow_mut();
        for handle in handles {
            rpc.destory(handle);
        }
    }
}

#[cfg(feature = "pg16")]
unsafe fn xact_delete() {
    let mut ptr: *mut pgrx::pg_sys::RelFileLocator = std::ptr::null_mut();
    let n = pgrx::pg_sys::smgrGetPendingDeletes(true, &mut ptr as *mut _);
    if n > 0 {
        let nodes = std::slice::from_raw_parts(ptr, n as usize);
        let handles = nodes
            .iter()
            .map(|node| Handle::from_sys(node.relNumber))
            .collect::<Vec<_>>();
        let mut rpc = crate::ipc::client::borrow_mut();
        for handle in handles {
            rpc.destory(handle);
        }
    }
}
