use crate::error::*;
use crate::index::utils::from_oid_to_handle;
use crate::ipc::client;
use base::index::*;
use pgrx::error;

#[pgrx::pg_extern(volatile, strict)]
fn _vectors_alter_vector_index(oid: pgrx::pg_sys::Oid, key: String, value: String) {
    let id = from_oid_to_handle(oid);
    let mut rpc = check_client(client());
    match rpc.alter(id, key, value) {
        Ok(_) => {}
        Err(e) => error!("{}", e.to_string()),
    }
}

#[pgrx::pg_extern(volatile, strict, parallel_safe)]
fn _vectors_index_stat(
    oid: pgrx::pg_sys::Oid,
) -> pgrx::composite_type!('static, "vector_index_stat") {
    use pgrx::heap_tuple::PgHeapTuple;
    let handle = from_oid_to_handle(oid);
    let type_name = crate::SCHEMA.to_string() + ".vector_index_stat";
    let mut res = PgHeapTuple::new_composite_type(&type_name).unwrap();
    let mut rpc = check_client(client());
    let stat = rpc.stat(handle);
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
                    .filter(|x| x.r#type == "sealed")
                    .map(|x| x.length as i64)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            res.set_by_name(
                "idx_growing",
                segments
                    .iter()
                    .filter(|x| x.r#type == "growing")
                    .map(|x| x.length as i64)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            res.set_by_name(
                "idx_write",
                segments
                    .iter()
                    .filter(|x| x.r#type == "write")
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
    }
}
