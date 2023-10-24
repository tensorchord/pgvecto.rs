use super::hook_transaction::{client, flush_if_commit};
use crate::ipc::client::{BuildHandle, BuildHandler};
use crate::postgres::index_setup::options;
use crate::prelude::*;

pub struct Builder {
    pub build_handler: Option<BuildHandler>,
    pub ntuples: f64,
}

pub struct BuildData<'a> {
    pub heap: pgrx::pg_sys::Relation,
    pub index_info: *mut pgrx::pg_sys::IndexInfo,
    pub result: &'a mut pgrx::pg_sys::IndexBuildResult,
}

pub unsafe fn build(index: pgrx::pg_sys::Relation, data: Option<BuildData>) {
    let oid = (*index).rd_id;
    let id = Id::from_sys(oid);
    flush_if_commit(id);
    let options = options(index);
    client(|rpc| {
        let build_handler = rpc.build(id, options).unwrap();
        let mut builder = Builder {
            build_handler: Some(build_handler),
            ntuples: 0.0,
        };
        if let Some(BuildData {
            heap,
            index_info,
            result,
        }) = data
        {
            let scan_fn = (*(*heap).rd_tableam).index_build_range_scan.unwrap();
            result.heap_tuples = scan_fn(
                heap,
                index,
                index_info,
                true,
                false,
                true,
                0,
                pgrx::pg_sys::InvalidBlockNumber,
                Some(callback),
                (&mut builder) as *mut Builder as *mut std::os::raw::c_void,
                std::ptr::null_mut(),
            );
            result.index_tuples = builder.ntuples;
        }
        let build_handler = builder.build_handler.take().unwrap();
        let BuildHandle::Next { x } = build_handler.handle().unwrap() else {
            panic!("Invaild state.")
        };
        let build_handler = x.leave(None).unwrap();
        let BuildHandle::Leave { x } = build_handler.handle().unwrap() else {
            panic!("Invaild state.")
        };
        x
    });
}

#[cfg(any(feature = "pg11", feature = "pg12"))]
#[pg_guard]
unsafe extern "C" fn callback(
    _index_relation: pg_sys::Relation,
    htup: pg_sys::HeapTuple,
    values: *mut pg_sys::Datum,
    is_null: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    use super::datatype::VectorInput;
    use pgrx::FromDatum;

    let ctid = &(*htup).t_self;
    let state = &mut *(state as *mut Builder);
    let pgvector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
    let data = (
        pgvector.to_vec().into_boxed_slice(),
        Pointer::from_sys(*ctid),
    );
    (*state.build).build.next(data).unwrap();
    state.ntuples += 1.0;
}

#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
#[pgrx::pg_guard]
unsafe extern "C" fn callback(
    _index_relation: pgrx::pg_sys::Relation,
    ctid: pgrx::pg_sys::ItemPointer,
    values: *mut pgrx::pg_sys::Datum,
    is_null: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    use super::datatype::VectorInput;
    use pgrx::FromDatum;

    let state = &mut *(state as *mut Builder);
    let pgvector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
    let data = (
        pgvector.to_vec().into_boxed_slice(),
        Pointer::from_sys(*ctid),
    );
    let BuildHandle::Next { x } = state.build_handler.take().unwrap().handle().unwrap() else {
        panic!("Invaild state.")
    };
    state.build_handler = Some(x.leave(Some(data)).unwrap());
    state.ntuples += 1.0;
}
