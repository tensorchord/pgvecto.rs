use super::hook_transaction::flush_if_commit;
use crate::index::utils::from_datum;
use crate::ipc::client::ClientGuard;
use crate::prelude::*;
use crate::{index::am_setup::options, ipc::client::Rpc};
use pgrx::pg_sys::{IndexBuildResult, IndexInfo, RelationData};
use service::prelude::*;

pub struct Builder {
    pub rpc: ClientGuard<Rpc>,
    pub heap_relation: *mut RelationData,
    pub index_info: *mut IndexInfo,
    pub result: *mut IndexBuildResult,
}

pub unsafe fn build(
    index: pgrx::pg_sys::Relation,
    data: Option<(*mut RelationData, *mut IndexInfo, *mut IndexBuildResult)>,
) {
    #[cfg(any(feature = "pg12", feature = "pg13", feature = "pg14", feature = "pg15"))]
    let oid = (*index).rd_node.relNode;
    #[cfg(feature = "pg16")]
    let oid = (*index).rd_locator.relNumber;
    let id = Handle::from_sys(oid);
    flush_if_commit(id);
    let options = options(index);
    let mut rpc = crate::ipc::client::borrow_mut();
    rpc.create(id, options);
    if let Some((heap_relation, index_info, result)) = data {
        let mut builder = Builder {
            rpc,
            heap_relation,
            index_info,
            result,
        };
        pgrx::pg_sys::IndexBuildHeapScan(
            heap_relation,
            index,
            index_info,
            Some(callback),
            &mut builder,
        );
    }
}

#[cfg(feature = "pg12")]
#[pgrx::pg_guard]
unsafe extern "C" fn callback(
    index_relation: pgrx::pg_sys::Relation,
    htup: pgrx::pg_sys::HeapTuple,
    values: *mut pgrx::pg_sys::Datum,
    _is_null: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let ctid = &(*htup).t_self;
    let oid = (*index_relation).rd_node.relNode;
    let id = Handle::from_sys(oid);
    let state = &mut *(state as *mut Builder);
    let vector = from_datum(*values.add(0));
    let data = (vector, Pointer::from_sys(*ctid));
    state.rpc.insert(id, data);
    (*state.result).heap_tuples += 1.0;
    (*state.result).index_tuples += 1.0;
}

#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
#[pgrx::pg_guard]
unsafe extern "C" fn callback(
    index_relation: pgrx::pg_sys::Relation,
    ctid: pgrx::pg_sys::ItemPointer,
    values: *mut pgrx::pg_sys::Datum,
    _is_null: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
    let oid = (*index_relation).rd_node.relNode;
    #[cfg(feature = "pg16")]
    let oid = (*index_relation).rd_locator.relNumber;
    let id = Handle::from_sys(oid);
    let state = &mut *(state as *mut Builder);
    let vector = from_datum(*values.add(0));
    let data = (vector, Pointer::from_sys(*ctid));
    state.rpc.insert(id, data);
    (*state.result).heap_tuples += 1.0;
    (*state.result).index_tuples += 1.0;
}
