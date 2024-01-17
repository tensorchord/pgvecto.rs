static mut PREV_EXECUTOR_START: pgrx::pg_sys::ExecutorStart_hook_type = None;

#[pgrx::pg_guard]
unsafe extern "C" fn vectors_executor_start(
    query_desc: *mut pgrx::pg_sys::QueryDesc,
    eflags: ::std::os::raw::c_int,
) {
    unsafe {
        if let Some(prev_executor_start) = PREV_EXECUTOR_START {
            prev_executor_start(query_desc, eflags);
        } else {
            pgrx::pg_sys::standard_ExecutorStart(query_desc, eflags);
        }
    }
    unsafe {
        super::hook_executor::post_executor_start(query_desc);
    }
}

#[pgrx::pg_guard]
unsafe extern "C" fn xact_callback(event: pgrx::pg_sys::XactEvent, _data: pgrx::void_mut_ptr) {
    match event {
        pgrx::pg_sys::XactEvent_XACT_EVENT_PRE_COMMIT
        | pgrx::pg_sys::XactEvent_XACT_EVENT_PARALLEL_PRE_COMMIT => {
            super::hook_transaction::commit();
        }
        pgrx::pg_sys::XactEvent_XACT_EVENT_ABORT
        | pgrx::pg_sys::XactEvent_XACT_EVENT_PARALLEL_ABORT => {
            super::hook_transaction::abort();
        }
        _ => {}
    }
}

pub unsafe fn init() {
    unsafe {
        PREV_EXECUTOR_START = pgrx::pg_sys::ExecutorStart_hook;
        pgrx::pg_sys::ExecutorStart_hook = Some(vectors_executor_start);
    }
    unsafe {
        pgrx::pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
    }
}
