static mut PREV_EXECUTOR_START: pgrx::pg_sys::ExecutorStart_hook_type = None;
static mut PREV_PROCESS_UTILITY: pgrx::pg_sys::ProcessUtility_hook_type = None;

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
unsafe extern "C" fn vectors_process_utility(
    pstmt: *mut pgrx::pg_sys::PlannedStmt,
    query_string: *const ::std::os::raw::c_char,
    read_only_tree: bool,
    context: pgrx::pg_sys::ProcessUtilityContext,
    params: pgrx::pg_sys::ParamListInfo,
    query_env: *mut pgrx::pg_sys::QueryEnvironment,
    dest: *mut pgrx::pg_sys::DestReceiver,
    qc: *mut pgrx::pg_sys::QueryCompletion,
) {
    super::hook_executor::pre_process_utility(pstmt);
    if let Some(prev_process_utility) = PREV_PROCESS_UTILITY {
        prev_process_utility(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        );
    } else {
        pgrx::pg_sys::standard_ProcessUtility(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        );
    }
}

#[pgrx::pg_guard]
unsafe extern "C" fn xact_callback(event: pgrx::pg_sys::XactEvent, _data: pgrx::void_mut_ptr) {
    match event {
        pgrx::pg_sys::XactEvent_XACT_EVENT_ABORT => {
            super::hook_transaction::aborting();
        }
        pgrx::pg_sys::XactEvent_XACT_EVENT_PRE_COMMIT => {
            super::hook_transaction::committing();
        }
        _ => {}
    }
}

pub unsafe fn init() {
    PREV_EXECUTOR_START = pgrx::pg_sys::ExecutorStart_hook;
    pgrx::pg_sys::ExecutorStart_hook = Some(vectors_executor_start);
    PREV_PROCESS_UTILITY = pgrx::pg_sys::ProcessUtility_hook;
    pgrx::pg_sys::ProcessUtility_hook = Some(vectors_process_utility);
    pgrx::pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
}
