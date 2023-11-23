use super::hook_transaction::client;
use crate::prelude::{Friendly, Id};

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat_tuples(oid: pgrx::pg_sys::Oid) -> i32 {
    let id = Id::from_sys(oid);
    let mut res = 0;
    client(|mut rpc| {
        res = rpc.stat_tuples(id).unwrap().friendly();
        rpc
    });
    res.try_into().unwrap()
}

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat_tuples_done(oid: pgrx::pg_sys::Oid) -> i32 {
    let id = Id::from_sys(oid);
    let mut res = 0;
    client(|mut rpc| {
        res = rpc.stat_tuples_done(id).unwrap().friendly();
        rpc
    });
    res.try_into().unwrap()
}

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat_sealed(oid: pgrx::pg_sys::Oid) -> Vec<i32> {
    let id = Id::from_sys(oid);
    let mut res = Vec::new();
    client(|mut rpc| {
        res = rpc.stat_sealed(id).unwrap().friendly();
        rpc
    });
    res.into_iter().map(|x| x.try_into().unwrap()).collect()
}

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat_growing(oid: pgrx::pg_sys::Oid) -> Vec<i32> {
    let id = Id::from_sys(oid);
    let mut res = Vec::new();
    client(|mut rpc| {
        res = rpc.stat_growing(id).unwrap().friendly();
        rpc
    });
    res.into_iter().map(|x| x.try_into().unwrap()).collect()
}

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat_config(oid: pgrx::pg_sys::Oid) -> String {
    let id = Id::from_sys(oid);
    let mut res = String::new();
    client(|mut rpc| {
        res = rpc.stat_config(id).unwrap().friendly();
        rpc
    });
    res
}
