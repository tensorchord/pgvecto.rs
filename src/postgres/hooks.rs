use pgrx::PgHooks;

struct Hooks;

static mut HOOKS: Hooks = Hooks;

impl PgHooks for Hooks {
    fn executor_start(
        &mut self,
        query_desc: pgrx::PgBox<pgrx::pg_sys::QueryDesc>,
        eflags: i32,
        prev_hook: fn(
            query_desc: pgrx::PgBox<pgrx::pg_sys::QueryDesc>,
            eflags: i32,
        ) -> pgrx::HookResult<()>,
    ) -> pgrx::HookResult<()> {
        let pointer = query_desc.as_ptr();
        let result = prev_hook(query_desc, eflags);
        unsafe {
            super::hook_executor::post_executor_start(pointer);
        }
        result
    }

    fn process_utility_hook(
        &mut self,
        pstmt: pgrx::PgBox<pgrx::pg_sys::PlannedStmt>,
        query_string: &core::ffi::CStr,
        read_only_tree: Option<bool>,
        context: pgrx::pg_sys::ProcessUtilityContext,
        params: pgrx::PgBox<pgrx::pg_sys::ParamListInfoData>,
        query_env: pgrx::PgBox<pgrx::pg_sys::QueryEnvironment>,
        dest: pgrx::PgBox<pgrx::pg_sys::DestReceiver>,
        completion_tag: *mut pgrx::pg_sys::QueryCompletion,
        prev_hook: fn(
            pstmt: pgrx::PgBox<pgrx::pg_sys::PlannedStmt>,
            query_string: &core::ffi::CStr,
            read_only_tree: Option<bool>,
            context: pgrx::pg_sys::ProcessUtilityContext,
            params: pgrx::PgBox<pgrx::pg_sys::ParamListInfoData>,
            query_env: pgrx::PgBox<pgrx::pg_sys::QueryEnvironment>,
            dest: pgrx::PgBox<pgrx::pg_sys::DestReceiver>,
            completion_tag: *mut pgrx::pg_sys::QueryCompletion,
        ) -> pgrx::HookResult<()>,
    ) -> pgrx::HookResult<()> {
        unsafe {
            super::hook_executor::pre_process_utility(pstmt.as_ptr());
            prev_hook(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                completion_tag,
            )
        }
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
    pgrx::register_hook(&mut HOOKS);
    pgrx::pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
    super::hook_custom_scan::init();
}
