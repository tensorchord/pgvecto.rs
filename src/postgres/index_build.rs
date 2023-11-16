use super::hook_transaction::{client, flush_if_commit};
use crate::ipc::client::Rpc;
use crate::postgres::index_setup::options;
use crate::prelude::*;
use pgrx::pg_sys::{IndexBuildResult, IndexInfo, RelationData};

pub struct Builder {
    pub rpc: Rpc,
    pub heap_relation: *mut RelationData,
    pub index_info: *mut IndexInfo,
    pub result: *mut IndexBuildResult,
}

pub unsafe fn build(
    index: pgrx::pg_sys::Relation,
    data: Option<(*mut RelationData, *mut IndexInfo, *mut IndexBuildResult)>,
) {
    let oid = (*index).rd_id;
    let id = Id::from_sys(oid);
    flush_if_commit(id);
    let options = options(index);
    client(|mut rpc| {
        rpc.create(id, options).unwrap();
        rpc
    });
    if let Some((heap_relation, index_info, result)) = data {
        client(|rpc| {
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
            builder.rpc
        });
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
    use super::datatype::VectorInput;
    use pgrx::FromDatum;

    let ctid = &(*htup).t_self;

    let oid = (*index_relation).rd_id;
    let id = Id::from_sys(oid);
    let state = &mut *(state as *mut Builder);
    let pgvector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
    let data = (pgvector.to_vec(), Pointer::from_sys(*ctid));
    state.rpc.insert(id, data).unwrap().friendly();
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
    use super::datatype::VectorInput;
    use pgrx::FromDatum;

    let oid = (*index_relation).rd_id;
    let id = Id::from_sys(oid);
    let state = &mut *(state as *mut Builder);
    let pgvector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
    let data = (pgvector.to_vec(), Pointer::from_sys(*ctid));
    state.rpc.insert(id, data).unwrap().friendly();
    (*state.result).heap_tuples += 1.0;
    (*state.result).index_tuples += 1.0;
}
