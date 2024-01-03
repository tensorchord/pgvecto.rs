use crate::prelude::*;
use service::prelude::*;

pgrx::extension_sql!(
    "\
CREATE TYPE VectorIndexStat AS (
    idx_indexing BOOL,
    idx_tuples BIGINT,
    idx_sealed BIGINT[],
    idx_growing BIGINT[],
    idx_write BIGINT,
    idx_size BIGINT,
    idx_options TEXT
);",
    name = "create_composites",
);

#[pgrx::pg_extern(volatile, strict)]
fn vector_stat(oid: pgrx::pg_sys::Oid) -> pgrx::composite_type!("VectorIndexStat") {
    let id = Id::from_sys(oid);
    let mut res = pgrx::prelude::PgHeapTuple::new_composite_type("VectorIndexStat").unwrap();
    let mut rpc = crate::ipc::client::borrow_mut();
    let stat = rpc.stat(id);
    res.set_by_name("idx_indexing", stat.indexing).unwrap();
    res.set_by_name("idx_tuples", {
        let mut tuples = 0;
        tuples += stat.sealed.iter().map(|x| *x as i64).sum::<i64>();
        tuples += stat.growing.iter().map(|x| *x as i64).sum::<i64>();
        tuples += stat.write as i64;
        tuples
    })
    .unwrap();
    res.set_by_name("idx_sealed", {
        let sealed = stat.sealed;
        sealed.into_iter().map(|x| x as i64).collect::<Vec<_>>()
    })
    .unwrap();
    res.set_by_name("idx_growing", {
        let growing = stat.growing;
        growing.into_iter().map(|x| x as i64).collect::<Vec<_>>()
    })
    .unwrap();
    res.set_by_name("idx_write", stat.write as i64).unwrap();
    res.set_by_name(
        "idx_size",
        stat.sizes.iter().map(|x| x.size as i64).sum::<i64>(),
    )
    .unwrap();
    res.set_by_name("idx_options", serde_json::to_string(&stat.options))
        .unwrap();
    res
}
