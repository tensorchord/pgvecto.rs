use super::hook_transaction::client;
use crate::prelude::*;

pgrx::extension_sql!(
    "\
CREATE TYPE VectorIndexInfo AS (
    indexing BOOL,
    idx_tuples INT,
    idx_sealed_len INT,
    idx_growing_len INT,
    idx_write INT,
    idx_sealed INT[],
    idx_growing INT[],
    idx_config TEXT
);",
    name = "create_composites",
);

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat(oid: pgrx::pg_sys::Oid) -> pgrx::composite_type!("VectorIndexInfo") {
    let id = Id::from_sys(oid);
    let mut res = pgrx::prelude::PgHeapTuple::new_composite_type("VectorIndexInfo").unwrap();
    client(|mut rpc| {
        let rpc_res = rpc.stat(id).unwrap().friendly();
        res.set_by_name("indexing", rpc_res.indexing).unwrap();
        res.set_by_name("idx_tuples", rpc_res.idx_tuples).unwrap();
        res.set_by_name("idx_sealed_len", rpc_res.idx_sealed_len)
            .unwrap();
        res.set_by_name("idx_growing_len", rpc_res.idx_growing_len)
            .unwrap();
        res.set_by_name("idx_write", rpc_res.idx_write).unwrap();
        res.set_by_name("idx_sealed", rpc_res.idx_sealed).unwrap();
        res.set_by_name("idx_growing", rpc_res.idx_growing).unwrap();
        res.set_by_name("idx_config", rpc_res.idx_config).unwrap();
        rpc
    });
    res
}
