use super::hook_transaction::client;
use crate::prelude::{Friendly, Id};

#[pgrx::pg_extern(volatile, parallel_safe, strict)]
fn vector_stat_tuples_done(oid: pgrx::pg_sys::Oid) -> i32 {
    let id = Id::from_sys(oid);
    let mut res = 0;
    client(|mut rpc| {
        res = rpc.stat(id).unwrap().friendly();
        rpc
    });
    res.try_into().unwrap()
}
