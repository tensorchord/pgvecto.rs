use crate::bgworker::Client;
use crate::postgres::gucs::BGWORKER_PORT;
use crate::prelude::*;
use pgrx::PgHooks;
use std::collections::BTreeSet;

struct State {
    flush_ids: BTreeSet<Id>,
    drop_ids: BTreeSet<Id>,
    client: Client,
}

struct Hooks;

static mut STATE: Option<State> = None;
static mut HOOKS: Hooks = Hooks;

unsafe fn state() -> &'static mut State {
    STATE.get_or_insert_with(|| {
        let flush_ids = BTreeSet::<Id>::new();
        let drop_ids = BTreeSet::<Id>::new();
        let stream = std::net::TcpStream::connect(("0.0.0.0", BGWORKER_PORT.get() as u16)).unwrap();
        let client = Client::new(stream).unwrap();
        State {
            flush_ids,
            drop_ids,
            client,
        }
    })
}

pub unsafe fn client() -> &'static mut Client {
    &mut state().client
}

impl PgHooks for Hooks {
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
            let utility_statement = pgrx::PgBox::from_pg(pstmt.utilityStmt);

            let is_drop = pgrx::is_a(utility_statement.as_ptr(), pgrx::pg_sys::NodeTag_T_DropStmt);

            if is_drop {
                let stat_drop =
                    pgrx::PgBox::from_pg(utility_statement.as_ptr() as *mut pgrx::pg_sys::DropStmt);

                match stat_drop.removeType {
                    pgrx::pg_sys::ObjectType_OBJECT_TABLE
                    | pgrx::pg_sys::ObjectType_OBJECT_INDEX => {
                        let objects =
                            pgrx::PgList::<pgrx::pg_sys::Node>::from_pg(stat_drop.objects);
                        for object in objects.iter_ptr() {
                            let mut rel = std::ptr::null_mut();
                            let address = pgrx::pg_sys::get_object_address(
                                stat_drop.removeType,
                                object,
                                &mut rel,
                                pgrx::pg_sys::AccessExclusiveLock as pgrx::pg_sys::LOCKMODE,
                                stat_drop.missing_ok,
                            );

                            if address.objectId == pgrx::pg_sys::InvalidOid {
                                continue;
                            }

                            match stat_drop.removeType {
                                pgrx::pg_sys::ObjectType_OBJECT_TABLE => {
                                    // Memory leak here?
                                    let list = pgrx::pg_sys::RelationGetIndexList(rel);
                                    let list = pgrx::PgList::<pgrx::pg_sys::Oid>::from_pg(list);
                                    for index in list.iter_oid() {
                                        hook_on_dropping(Id::from_sys(index));
                                    }
                                    pgrx::pg_sys::relation_close(
                                        rel,
                                        pgrx::pg_sys::AccessExclusiveLock as _,
                                    );
                                }
                                pgrx::pg_sys::ObjectType_OBJECT_INDEX => {
                                    hook_on_dropping(Id::from_sys((*rel).rd_id));
                                    pgrx::pg_sys::relation_close(
                                        rel,
                                        pgrx::pg_sys::AccessExclusiveLock as _,
                                    );
                                }
                                _ => unreachable!(),
                            }
                        }
                    }

                    _ => {}
                }
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
            } else {
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
    fn commit(&mut self) {
        unsafe {
            hook_on_committing();
        }
    }
    fn abort(&mut self) {
        unsafe {
            hook_on_aborting();
        }
    }
}

pub unsafe fn hook_on_dropping(id: Id) {
    state().drop_ids.insert(id);
}

pub unsafe fn hook_on_writing(id: Id) {
    state().flush_ids.insert(id);
}

pub unsafe fn hook_on_committing() {
    if let Some(mut state) = STATE.take() {
        for id in state.flush_ids.iter() {
            state.client.flush(*id).unwrap();
        }
        for id in state.drop_ids.iter() {
            state.client.drop(*id).unwrap();
        }
    }
}

pub unsafe fn hook_on_aborting() {
    STATE.take();
}

pub unsafe fn init() {
    pgrx::register_hook(&mut HOOKS);
}
