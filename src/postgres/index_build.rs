use super::hook_transaction::{client, flush_if_commit};
use crate::ipc::client::Rpc;
use crate::postgres::index_setup::options;
use crate::prelude::*;
use pgrx::pg_sys::{IndexInfo, RelationData};

pub struct Builder {
    pub rpc: Rpc,
    pub ntuples: f64,
}

pub unsafe fn build(
    index: pgrx::pg_sys::Relation,
    data: Option<(*mut RelationData, *mut IndexInfo)>,
) {
    let oid = (*index).rd_id;
    let id = Id::from_sys(oid);
    flush_if_commit(id);
    let options = options(index);
    client(|mut rpc| {
        rpc.create(id, options).unwrap();
        let mut builder = Builder { rpc, ntuples: 0.0 };
        if let Some((heap, index_info)) = data {
            pgrx::pg_sys::IndexBuildHeapScan(heap, index, index_info, Some(callback), &mut builder);
        }
        builder.rpc
    });
}

#[cfg(any(feature = "pg11", feature = "pg12"))]
#[pg_guard]
unsafe extern "C" fn callback(
    index_relation: pg_sys::Relation,
    htup: pg_sys::HeapTuple,
    values: *mut pg_sys::Datum,
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
    state.rpc.insert(id, data).unwrap();
    state.ntuples += 1.0;
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
    state
        .rpc
        .insert(id, data)
        .unwrap()
        .expect("Bgworker Error.");
    state.ntuples += 1.0;
}
