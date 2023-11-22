use super::hook_transaction::client;
use crate::prelude::{Friendly, Id};
use std::mem::MaybeUninit;

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat_indexing(oid: pgrx::pg_sys::Oid) -> bool {
    let id = Id::from_sys(oid);
    let mut res = MaybeUninit::uninit();
    client(|mut rpc| {
        res.write(rpc.stat_indexing(id).unwrap().friendly());
        rpc
    });
    unsafe { res.assume_init() }
}

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat_tuples(oid: pgrx::pg_sys::Oid) -> i32 {
    let id = Id::from_sys(oid);
    let mut res = MaybeUninit::uninit();
    client(|mut rpc| {
        res.write(rpc.stat_tuples(id).unwrap().friendly());
        rpc
    });
    unsafe { res.assume_init() }.try_into().unwrap()
}

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat_tuples_done(oid: pgrx::pg_sys::Oid) -> i32 {
    let id = Id::from_sys(oid);
    let mut res = MaybeUninit::uninit();
    client(|mut rpc| {
        res.write(rpc.stat_tuples_done(id).unwrap().friendly());
        rpc
    });
    unsafe { res.assume_init() }.try_into().unwrap()
}

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat_config(oid: pgrx::pg_sys::Oid) -> String {
    let id = Id::from_sys(oid);
    let mut res = MaybeUninit::uninit();
    client(|mut rpc| {
        res.write(rpc.stat_config(id).unwrap().friendly());
        rpc
    });
    unsafe { res.assume_init() }
}
