use memoffset::offset_of;
use pgrx::{pg_sys::AsPgCStr, prelude::*, set_varsize, PgRelation};

pub(crate) const DEFAULT_MAX_CLUSTER_SIZE: usize = 2000;
pub(crate) const DEFAULT_CLUSTER_SIZE: usize = 64;

static mut RELOPT_KIND_VECTORS: pg_sys::relopt_kind = 0;

pub(crate) unsafe fn init() {
    RELOPT_KIND_VECTORS = pg_sys::add_reloption_kind();

    pg_sys::add_int_reloption(
        RELOPT_KIND_VECTORS,
        "cluster".as_pg_cstr(),
        "the number of the clusters".as_pg_cstr(),
        DEFAULT_CLUSTER_SIZE as i32,
        1,
        DEFAULT_MAX_CLUSTER_SIZE as i32,
        #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct VectorsOptions {
    /* varlena header (do not touch directly!) */
    #[allow(dead_code)]
    vl_len_: i32,

    pub cluster: usize,
}

impl VectorsOptions {
    fn from_relation(relation: &PgRelation) -> PgBox<VectorsOptions> {
        if relation.rd_index.is_null() {
            panic!("'{}' is not a Vectors index", relation.name());
        } else if relation.rd_options.is_null() {
            let mut ops = unsafe { PgBox::<VectorsOptions>::alloc0() };
            ops.cluster = 64;
            unsafe {
                set_varsize(
                    ops.as_ptr().cast(),
                    std::mem::size_of::<VectorsOptions>() as i32,
                )
            }
            ops.into_pg_boxed()
        } else {
            unsafe { PgBox::from_pg(relation.rd_options as *mut VectorsOptions) }
        }
    }
}

#[pg_guard]
pub(crate) unsafe extern "C" fn am_options(
    rel_options: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    let tab = vec![pg_sys::relopt_parse_elt {
        optname: "cluster".as_pg_cstr(),
        opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
        offset: offset_of!(VectorsOptions, cluster) as i32,
    }];

    build_relopts(rel_options, validate, tab)
}

#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
unsafe fn build_relopts(
    rel_options: pg_sys::Datum,
    validate: bool,
    tab: Vec<pg_sys::relopt_parse_elt>,
) -> *mut pg_sys::bytea {
    let rdopts = pg_sys::build_reloptions(
        rel_options,
        validate,
        RELOPT_KIND_VECTORS,
        std::mem::size_of::<VectorsOptions>(),
        tab.as_ptr(),
        tab.len() as i32,
    );

    rdopts as *mut pg_sys::bytea
}

#[cfg(any(feature = "pg11", feature = "pg12"))]
unsafe fn build_relopts(
    rel_options: pg_sys::Datum,
    validate: bool,
    tab: Vec<pg_sys::relopt_parse_elt>,
) -> *mut pg_sys::bytea {
    use pgrx::void_mut_ptr;

    let mut num_options = 0;
    let options =
        pg_sys::parseRelOptions(rel_options, validate, RELOPT_KIND_VECTORS, &mut num_options);
    if num_options == 0 {
        return std::ptr::null_mut();
    }

    for relopt in std::slice::from_raw_parts_mut(options, num_options as usize) {
        relopt.gen.as_mut().unwrap().lockmode = pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE;
    }

    let rdopts =
        pg_sys::allocateReloptStruct(std::mem::size_of::<VectorsOptions>(), options, num_options);
    pg_sys::fillRelOptions(
        rdopts,
        std::mem::size_of::<VectorsOptions>(),
        options,
        num_options,
        validate,
        tab.as_ptr(),
        tab.len() as i32,
    );
    pg_sys::pfree(options as void_mut_ptr);

    rdopts as *mut pg_sys::bytea
}
