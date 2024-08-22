use crate::error::*;
use crate::index::utils::from_oid_to_handle;
use crate::ipc::client;
use crate::utils::cells::PgRefCell;
use base::search::Handle;
use pgrx::pg_sys::Oid;
use std::collections::BTreeMap;
use std::ptr::NonNull;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TransactionIndex {
    Create,
    Drop,
    Dirty,
}

#[derive(Debug)]
struct Transaction {
    pub index: BTreeMap<Handle, TransactionIndex>,
}

impl Transaction {
    const fn new() -> Self {
        Self {
            index: BTreeMap::new(),
        }
    }
}

static TRANSACTION: PgRefCell<Transaction> = unsafe { PgRefCell::new(Transaction::new()) };

pub fn on_index_build(handle: Handle) {
    let mut t = TRANSACTION.borrow_mut();
    match t.index.get(&handle) {
        Some(TransactionIndex::Create) => {
            // It's a reindex
        }
        Some(TransactionIndex::Dirty) => {
            // It's a reindex
            t.index.insert(handle, TransactionIndex::Create);
        }
        Some(TransactionIndex::Drop) => unreachable!("reused oid in a transaction."),
        None => {
            // It's an index or reindex
            t.index.insert(handle, TransactionIndex::Create);
        }
    }
}

pub fn on_index_write(handle: Handle) {
    let mut t = TRANSACTION.borrow_mut();
    match t.index.get(&handle) {
        Some(TransactionIndex::Create) => (),
        Some(TransactionIndex::Dirty) => (),
        Some(TransactionIndex::Drop) => unreachable!(),
        None => {
            // It's not created in this transaction and never modified in this transaction
            t.index.insert(handle, TransactionIndex::Dirty);
        }
    }
}

pub unsafe fn on_object_access(
    access: pgrx::pg_sys::ObjectAccessType::Type,
    class_id: Oid,
    object_id: Oid,
    sub_id: i32,
    _arg: *mut libc::c_void,
) {
    if class_id != pgrx::pg_sys::RelationRelationId {
        return;
    }
    if object_id.as_u32() == 0 {
        return;
    }
    if sub_id != 0 {
        return;
    }
    if access == pgrx::pg_sys::ObjectAccessType::OAT_DROP {
        let search = pgrx::pg_catalog::PgClass::search_reloid(object_id).unwrap();
        if let Some(pg_class) = search.get() {
            if let Some(()) = check_vector_index(pg_class) {
                let handle = from_oid_to_handle(object_id);
                let mut t = TRANSACTION.borrow_mut();
                match t.index.get(&handle) {
                    Some(TransactionIndex::Create) => {
                        // It's created in this transaction, so drop it immediately
                        let handle = from_oid_to_handle(object_id);
                        let mut rpc = check_client(client());
                        if let Err(e) = rpc.drop(handle) {
                            pgrx::warning!("Failed to drop {handle} for abortting: {e}.");
                        }
                        t.index.remove(&handle);
                    }
                    Some(TransactionIndex::Drop) => unreachable!(),
                    Some(TransactionIndex::Dirty) => {
                        // It's not created in this transaction but modified in this transaction
                        t.index.insert(handle, TransactionIndex::Drop);
                    }
                    None => {
                        // It's not created in this transaction and never modified in this transaction
                        t.index.insert(handle, TransactionIndex::Drop);
                    }
                }
            }
        }
    }
}

fn check_vector_index(pg_class: pgrx::pg_catalog::PgClass<'_>) -> Option<()> {
    if pg_class.relkind() != pgrx::pg_catalog::PgClassRelkind::Index {
        return None;
    }
    let relam = pg_class.relam();
    if relam.as_u32() == 0 {
        return None;
    }
    let search = pgrx::pg_catalog::PgAm::search_amoid(relam)?;
    let pg_am = search.get()?;
    if pg_am.amname() != crate::SCHEMA_C_STR {
        return None;
    }
    // probably a vector index, so enter a slow path to ensure it
    check_vector_index_slow_path(pg_am)
}

fn check_vector_index_slow_path(pg_am: pgrx::pg_catalog::PgAm<'_>) -> Option<()> {
    let amhandler = pg_am.amhandler();
    let mut flinfo = unsafe {
        let mut flinfo = pgrx::pg_sys::FmgrInfo::default();
        pgrx::pg_sys::fmgr_info(amhandler, &mut flinfo);
        flinfo
    };
    let func = flinfo.fn_addr?;
    let mut fcinfo = pgrx::pg_sys::FunctionCallInfoBaseData {
        flinfo: &mut flinfo,
        fncollation: pgrx::pg_sys::DEFAULT_COLLATION_OID,
        context: std::ptr::null_mut(),
        resultinfo: std::ptr::null_mut(),
        isnull: false,
        nargs: 0,
        ..pgrx::pg_sys::FunctionCallInfoBaseData::default()
    };
    unsafe {
        let result = scopeguard::guard(
            NonNull::new(
                pgrx::pg_sys::ffi::pg_guard_ffi_boundary(|| func(&mut fcinfo))
                    .cast_mut_ptr::<pgrx::pg_sys::IndexAmRoutine>(),
            )?,
            |p| {
                pgrx::pg_sys::pfree(p.as_ptr().cast());
            },
        );
        if result.as_ref().amvalidate == Some(super::am::amvalidate) {
            return Some(());
        }
    }
    Some(())
}

pub unsafe fn on_commit() {
    let t = std::mem::replace(&mut *TRANSACTION.borrow_mut(), Transaction::new());
    if let Err(e) = std::panic::catch_unwind(|| {
        if t.index.is_empty() {
            return;
        }
        let mut rpc = client().expect("Failed to create RPC client.");
        for (&handle, e) in t.index.iter() {
            match e {
                TransactionIndex::Create | TransactionIndex::Dirty => {
                    if let Err(e) = rpc.flush(handle) {
                        pgrx::warning!("Failed to flush {handle} for committing: {e}.");
                    }
                }
                TransactionIndex::Drop => {
                    if let Err(e) = rpc.drop(handle) {
                        pgrx::warning!("Failed to drop {handle} for committing: {e}.");
                    }
                }
            }
        }
    }) {
        if let Some(i) = e.downcast_ref::<&'static str>() {
            pgrx::warning!("Failed to maintain vector indexes: {i}");
        } else if let Some(i) = e.downcast_ref::<String>() {
            pgrx::warning!("Failed to maintain vector indexes: {i}");
        } else {
            pgrx::warning!("Failed to maintain vector indexes");
        }
    }
}

pub unsafe fn on_abort() {
    let t = std::mem::replace(&mut *TRANSACTION.borrow_mut(), Transaction::new());
    if let Err(e) = std::panic::catch_unwind(|| {
        if t.index.is_empty() {
            return;
        }
        let mut rpc = client().expect("Failed to create RPC client.");
        for (&handle, e) in t.index.iter() {
            match e {
                TransactionIndex::Create => {
                    if let Err(e) = rpc.drop(handle) {
                        pgrx::warning!("Failed to drop {handle} for abortting: {e}.");
                    }
                }
                TransactionIndex::Drop => (),
                TransactionIndex::Dirty => (),
            }
        }
    }) {
        if let Some(i) = e.downcast_ref::<&'static str>() {
            pgrx::warning!("Failed to maintain vector indexes: {i}");
        } else if let Some(i) = e.downcast_ref::<String>() {
            pgrx::warning!("Failed to maintain vector indexes: {i}");
        } else {
            pgrx::warning!("Failed to maintain vector indexes");
        }
    }
}
