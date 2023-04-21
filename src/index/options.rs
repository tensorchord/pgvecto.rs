use pgrx::prelude::*;

#[derive(Clone)]
pub struct VectorsOptions {
    oid: pg_sys::Oid,
}

impl VectorsOptions {}

#[pg_guard]
pub(crate) extern "C" fn am_options(
    rel_options: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    unimplemented!()
}
