use crate::bgworker::Client;
use crate::postgres::gucs::PORT;
use crate::prelude::*;
use parking_lot::{Mutex, MutexGuard};
use pgrx::once_cell::sync::Lazy;
use pgrx::PgHooks;
use std::cell::RefCell;
use std::collections::BTreeSet;

struct Hooks;

static mut HOOKS: Hooks = Hooks;

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
                                        drop_if_commit(Id::from_sys(index));
                                    }
                                    pgrx::pg_sys::relation_close(
                                        rel,
                                        pgrx::pg_sys::AccessExclusiveLock as _,
                                    );
                                }
                                pgrx::pg_sys::ObjectType_OBJECT_INDEX => {
                                    drop_if_commit(Id::from_sys((*rel).rd_id));
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
}

pub fn drop_if_commit(id: Id) {
    DROP_IF_COMMIT.borrow_mut().insert(id);
}

pub fn flush_if_commit(id: Id) {
    FLUSH_IF_COMMIT.borrow_mut().insert(id);
}

pub fn client() -> MutexGuard<'static, Lazy<Client>> {
    CLIENT.lock()
}

#[thread_local]
static FLUSH_IF_COMMIT: RefCell<BTreeSet<Id>> = RefCell::new(BTreeSet::new());

#[thread_local]
static DROP_IF_COMMIT: RefCell<BTreeSet<Id>> = RefCell::new(BTreeSet::new());

static CLIENT: Mutex<Lazy<Client>> = Mutex::new(Lazy::new(lazy_client));

fn lazy_client() -> Client {
    let stream = std::net::TcpStream::connect(("0.0.0.0", PORT.get() as u16)).unwrap();
    Client::new(stream).unwrap()
}

#[pgrx::pg_guard]
unsafe extern "C" fn xact_callback(event: pgrx::pg_sys::XactEvent, _data: pgrx::void_mut_ptr) {
    match event {
        pgrx::pg_sys::XactEvent_XACT_EVENT_ABORT => {
            *FLUSH_IF_COMMIT.borrow_mut() = BTreeSet::new();
            *DROP_IF_COMMIT.borrow_mut() = BTreeSet::new();
            *CLIENT.lock() = Lazy::new(lazy_client);
        }
        pgrx::pg_sys::XactEvent_XACT_EVENT_PRE_COMMIT => {
            let mut client = CLIENT.lock();
            let client = &mut *client;

            for id in FLUSH_IF_COMMIT.borrow().iter().copied() {
                client.flush(id).unwrap();
            }

            for id in DROP_IF_COMMIT.borrow().iter().copied() {
                client.drop(id).unwrap();
            }

            *FLUSH_IF_COMMIT.borrow_mut() = BTreeSet::new();
            *DROP_IF_COMMIT.borrow_mut() = BTreeSet::new();
        }
        _ => {}
    }
}

pub unsafe fn init() {
    pgrx::register_hook(&mut HOOKS);
    pgrx::pg_sys::RegisterXactCallback(Some(xact_callback), std::ptr::null_mut());
}
