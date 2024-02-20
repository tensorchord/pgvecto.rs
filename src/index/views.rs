use crate::prelude::*;

#[pgrx::pg_extern(volatile, strict)]
fn _vectors_index_stat(
    oid: pgrx::pg_sys::Oid,
) -> pgrx::composite_type!('static, "vectors.vector_index_stat") {
    use pgrx::heap_tuple::PgHeapTuple;
    let id = Handle::from_sys(oid);
    let mut res = PgHeapTuple::new_composite_type("vectors.vector_index_stat").unwrap();
    let mut rpc = check_client(crate::ipc::client());
    let stat = rpc.stat(id);
    match stat {
        Ok(IndexStat {
            indexing,
            options,
            segments,
        }) => {
            res.set_by_name("idx_status", "NORMAL").unwrap();
            res.set_by_name("idx_indexing", indexing).unwrap();
            res.set_by_name(
                "idx_tuples",
                segments.iter().map(|x| x.length as i64).sum::<i64>(),
            )
            .unwrap();
            res.set_by_name(
                "idx_sealed",
                segments
                    .iter()
                    .filter(|x| x.typ == "sealed")
                    .map(|x| x.length as i64)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            res.set_by_name(
                "idx_growing",
                segments
                    .iter()
                    .filter(|x| x.typ == "growing")
                    .map(|x| x.length as i64)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            res.set_by_name(
                "idx_write",
                segments
                    .iter()
                    .filter(|x| x.typ == "write")
                    .map(|x| x.length as i64)
                    .sum::<i64>(),
            )
            .unwrap();
            res.set_by_name(
                "idx_size",
                segments.iter().map(|x| x.size as i64).sum::<i64>(),
            )
            .unwrap();
            res.set_by_name("idx_options", serde_json::to_string(&options))
                .unwrap();
            res
        }
        Err(StatError::NotExist) => {
            bad_service_not_exist();
        }
        Err(StatError::Upgrade) => {
            res.set_by_name("idx_status", "UPGRADE").unwrap();
            res
        }
    }
}
