use crate::postgres::hook_transaction::client;
use crate::prelude::*;

#[pgrx::pg_extern(strict)]
unsafe fn vectors_load(oid: pgrx::pg_sys::Oid) {
    let id = Id::from_sys(oid);
    client(|mut rpc| {
        rpc.load(id).unwrap();
        rpc
    })
}

#[pgrx::pg_extern(strict)]
unsafe fn vectors_unload(oid: pgrx::pg_sys::Oid) {
    let id = Id::from_sys(oid);
    client(|mut rpc| {
        rpc.unload(id).unwrap();
        rpc
    })
}
