use crate::postgres::hook_transaction::{client, flush_if_commit};
use crate::prelude::*;

pub unsafe fn update_insert(id: Id, vector: Box<[Scalar]>, tid: pgrx::pg_sys::ItemPointer) {
    flush_if_commit(id);
    let p = Pointer::from_sys(*tid);
    client(|mut rpc| {
        rpc.insert(id, (vector, p)).unwrap();
        rpc
    })
}

pub fn update_delete(id: Id, deletes: Vec<Pointer>) {
    flush_if_commit(id);
    client(|mut rpc| {
        for message in deletes {
            rpc.delete(id, message).unwrap();
        }
        rpc
    })
}
