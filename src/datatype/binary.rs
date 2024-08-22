use pgrx::datum::IntoDatum;
use pgrx::pg_sys::{bytea, Datum, Oid};
use pgrx::pgrx_sql_entity_graph::metadata::*;

#[repr(transparent)]
pub struct Bytea(*mut bytea);

impl Bytea {
    pub fn new(x: *mut bytea) -> Self {
        Self(x)
    }
}

impl IntoDatum for Bytea {
    fn into_datum(self) -> Option<Datum> {
        if !self.0.is_null() {
            Some(Datum::from(self.0))
        } else {
            None
        }
    }

    fn type_oid() -> Oid {
        pgrx::pg_sys::BYTEAOID
    }
}

unsafe impl SqlTranslatable for Bytea {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("bytea")))
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("bytea"))))
    }
}

unsafe impl pgrx::callconv::BoxRet for Bytea {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        unsafe { fcinfo.return_raw_datum(Datum::from(self.0 as *mut ())) }
    }
}
