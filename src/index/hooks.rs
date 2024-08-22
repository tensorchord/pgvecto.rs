static mut PREV_EXECUTOR_START: pgrx::pg_sys::ExecutorStart_hook_type = None;
static mut PREV_PROCESS_UTILITY: pgrx::pg_sys::ProcessUtility_hook_type = None;
static mut NEXT_OBJECT_ACCESS_HOOK: pgrx::pg_sys::object_access_hook_type = None;

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
}

#[pgrx::pg_guard]
unsafe extern "C" fn vectors_process_utility(
    pstmt: *mut pgrx::pg_sys::PlannedStmt,
    query_string: *const ::std::os::raw::c_char,
    read_only_tree: bool,
    context: pgrx::pg_sys::ProcessUtilityContext::Type,
    params: pgrx::pg_sys::ParamListInfo,
    query_env: *mut pgrx::pg_sys::QueryEnvironment,
    dest: *mut pgrx::pg_sys::DestReceiver,
    completion_tag: *mut pgrx::pg_sys::QueryCompletion,
) {
    unsafe {
        super::compatibility::on_process_utility(pstmt);
    }
    unsafe {
        if let Some(prev_process_utility) = PREV_PROCESS_UTILITY {
            prev_process_utility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                completion_tag,
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
                completion_tag,
            );
        }
    }
}

#[pgrx::pg_guard]
unsafe extern "C" fn vectors_object_access(
    access: pgrx::pg_sys::ObjectAccessType::Type,
    class_id: pgrx::pg_sys::Oid,
    object_id: pgrx::pg_sys::Oid,
    sub_id: i32,
    arg: *mut libc::c_void,
) {
    unsafe {
        super::catalog::on_object_access(access, class_id, object_id, sub_id, arg);
        if let Some(next_object_access) = NEXT_OBJECT_ACCESS_HOOK {
            next_object_access(access, class_id, object_id, sub_id, arg);
        }
    }
}

#[pgrx::pg_guard]
unsafe extern "C" fn xact_callback(
    event: pgrx::pg_sys::XactEvent::Type,
    _data: pgrx::void_mut_ptr,
) {
    match event {
        pgrx::pg_sys::XactEvent::XACT_EVENT_PRE_COMMIT
        | pgrx::pg_sys::XactEvent::XACT_EVENT_PARALLEL_PRE_COMMIT => unsafe {
            super::catalog::on_commit();
        },
        pgrx::pg_sys::XactEvent::XACT_EVENT_ABORT
        | pgrx::pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT => unsafe {
            super::catalog::on_abort();
        },
        _ => {}
    }
}

pub unsafe fn init() {
    unsafe {
        PREV_EXECUTOR_START = pgrx::pg_sys::ExecutorStart_hook;
        pgrx::pg_sys::ExecutorStart_hook = Some(vectors_executor_start);
        PREV_PROCESS_UTILITY = pgrx::pg_sys::ProcessUtility_hook;
        pgrx::pg_sys::ProcessUtility_hook = Some(vectors_process_utility);
        NEXT_OBJECT_ACCESS_HOOK = pgrx::pg_sys::object_access_hook;
        pgrx::pg_sys::object_access_hook = Some(vectors_object_access);
    }
    unsafe {
        pgrx::pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
    }
}
