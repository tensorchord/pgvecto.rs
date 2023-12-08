use super::{client::ClientGuard, hook_transaction::flush_if_commit};
use crate::datatype::vecf32::Vecf32Input;
use crate::index::am_setup::options;
use crate::prelude::*;
use pgrx::pg_sys::{IndexBuildResult, IndexInfo, RelationData};
use service::prelude::*;

pub struct Builder {
    pub client: ClientGuard,
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
    let id = Id::from_sys(oid);
    flush_if_commit(id);
    let options = options(index);
    let mut client = super::client::borrow_mut();
    client.create(id, options);
    if let Some((heap_relation, index_info, result)) = data {
        let mut builder = Builder {
            client,
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
    is_null: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    use pgrx::FromDatum;
    let ctid = &(*htup).t_self;
    let oid = (*index_relation).rd_node.relNode;
    let id = Id::from_sys(oid);
    let state = &mut *(state as *mut Builder);
    let pgvector = Vecf32Input::from_datum(*values.add(0), *is_null.add(0)).unwrap();
    let data = (pgvector.to_vec().into(), Pointer::from_sys(*ctid));
    state.client.insert(id, data);
    (*state.result).heap_tuples += 1.0;
    (*state.result).index_tuples += 1.0;
}

#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
#[pgrx::pg_guard]
unsafe extern "C" fn callback(
    index_relation: pgrx::pg_sys::Relation,
    ctid: pgrx::pg_sys::ItemPointer,
    values: *mut pgrx::pg_sys::Datum,
    is_null: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    use pgrx::FromDatum;
    #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
    let oid = (*index_relation).rd_node.relNode;
    #[cfg(feature = "pg16")]
    let oid = (*index_relation).rd_locator.relNumber;
    let id = Id::from_sys(oid);
    let state = &mut *(state as *mut Builder);
    let pgvector = Vecf32Input::from_datum(*values.add(0), *is_null.add(0)).unwrap();
    let data = (pgvector.to_vec().into(), Pointer::from_sys(*ctid));
    state.client.insert(id, data);
    (*state.result).heap_tuples += 1.0;
    (*state.result).index_tuples += 1.0;
}
