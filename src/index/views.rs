use crate::prelude::*;
use service::prelude::*;

pgrx::extension_sql!(
    "\
CREATE TYPE VectorIndexStat AS (
    idx_status TEXT,
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
    use service::index::IndexStat;
    let id = Handle::from_sys(oid);
    let mut res = pgrx::prelude::PgHeapTuple::new_composite_type("VectorIndexStat").unwrap();
    let mut rpc = crate::ipc::client::borrow_mut();
    let stat = rpc.stat(id);
    match stat {
        IndexStat::Normal {
            indexing,
            sealed,
            growing,
            write,
            options,
            sizes,
        } => {
            res.set_by_name("idx_status", "NORMAL").unwrap();
            res.set_by_name("idx_indexing", indexing).unwrap();
            res.set_by_name("idx_tuples", {
                let mut tuples = 0;
                tuples += sealed.iter().map(|x| *x as i64).sum::<i64>();
                tuples += growing.iter().map(|x| *x as i64).sum::<i64>();
                tuples += write as i64;
                tuples
            })
            .unwrap();
            res.set_by_name(
                "idx_sealed",
                sealed.into_iter().map(|x| x as i64).collect::<Vec<_>>(),
            )
            .unwrap();
            res.set_by_name(
                "idx_growing",
                growing.into_iter().map(|x| x as i64).collect::<Vec<_>>(),
            )
            .unwrap();
            res.set_by_name("idx_write", write as i64).unwrap();
            res.set_by_name("idx_size", sizes.iter().map(|x| x.size as i64).sum::<i64>())
                .unwrap();
            res.set_by_name("idx_options", serde_json::to_string(&options))
                .unwrap();
            res
        }
        IndexStat::Upgrade => {
            res.set_by_name("idx_status", "UPGRADE").unwrap();
            res
        }
    }
}
