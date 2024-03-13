#![allow(unsafe_op_in_unsafe_fn)]

use crate::error::*;
use crate::index::am_setup::options;
use crate::index::utils::{from_datum, get_handle};
use crate::ipc::ClientRpc;
use crate::utils::sys::FromSys;
use base::index::*;
use base::search::*;
use pgrx::pg_sys::{IndexBuildResult, IndexInfo, RelationData};

pub struct Builder {
    pub rpc: ClientRpc,
    pub heap_relation: *mut RelationData,
    pub index_info: *mut IndexInfo,
    pub result: *mut IndexBuildResult,
}

pub unsafe fn build(
    index: pgrx::pg_sys::Relation,
    data: Option<(*mut RelationData, *mut IndexInfo, *mut IndexBuildResult)>,
) {
    let oid = (*index).rd_id;
    let id = get_handle(oid);
    let options = options(index);
    let mut rpc = check_client(crate::ipc::client());
    match rpc.create(id, options) {
        Ok(()) => (),
        Err(CreateError::InvalidIndexOptions { reason }) => {
            bad_service_invalid_index_options(&reason);
        }
    }
    super::hook_maintain::maintain_index_in_index_create(id);
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

#[pgrx::pg_guard]
unsafe extern "C" fn callback(
    index_relation: pgrx::pg_sys::Relation,
    ctid: pgrx::pg_sys::ItemPointer,
    values: *mut pgrx::pg_sys::Datum,
    is_null: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let state = &mut *(state as *mut Builder);
    if *is_null.add(0) {
        (*state.result).heap_tuples += 1.0;
        return;
    }
    let oid = (*index_relation).rd_id;
    let id = get_handle(oid);
    let vector = from_datum(*values.add(0), *is_null.add(0));
    let vector = match vector {
        Some(v) => v,
        None => unreachable!(),
    };
    let pointer = Pointer::from_sys(*ctid);
    match state.rpc.insert(id, vector, pointer) {
        Ok(()) => (),
        Err(InsertError::NotExist) => bad_service_not_exist(),
        Err(InsertError::InvalidVector) => bad_service_invalid_vector(),
    }
    (*state.result).heap_tuples += 1.0;
    (*state.result).index_tuples += 1.0;
}
