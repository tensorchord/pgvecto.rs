use super::index_build;
use super::index_scan;
use super::index_setup;
use super::index_update;
use crate::postgres::datatype::VectorInput;
use crate::postgres::gucs::FILTER_MODE;
use crate::postgres::gucs::FilterMode;
use crate::prelude::*;
use std::cell::Cell;

#[thread_local]
static RELOPT_KIND: Cell<pgrx::pg_sys::relopt_kind> = Cell::new(0);

pub unsafe fn init() {
    use pgrx::pg_sys::AsPgCStr;
    RELOPT_KIND.set(pgrx::pg_sys::add_reloption_kind());
    pgrx::pg_sys::add_string_reloption(
        RELOPT_KIND.get(),
        "options".as_pg_cstr(),
        "".as_pg_cstr(),
        "".as_pg_cstr(),
        None,
        #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
        {
            pgrx::pg_sys::AccessExclusiveLock as pgrx::pg_sys::LOCKMODE
        },
    );
}

#[pgrx::pg_extern(sql = "
    CREATE OR REPLACE FUNCTION vectors_amhandler(internal) RETURNS index_am_handler
    PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
    CREATE ACCESS METHOD vectors TYPE INDEX HANDLER vectors_amhandler;
    COMMENT ON ACCESS METHOD vectors IS 'pgvecto.rs index access method';
", requires = ["vector"])]
fn vectors_amhandler(
    _fcinfo: pgrx::pg_sys::FunctionCallInfo,
) -> pgrx::PgBox<pgrx::pg_sys::IndexAmRoutine> {
    unsafe {
        let mut am_routine = pgrx::PgBox::<pgrx::pg_sys::IndexAmRoutine>::alloc0();
        *am_routine = AM_HANDLER;
        am_routine.into_pg_boxed()
    }
}

const AM_HANDLER: pgrx::pg_sys::IndexAmRoutine = {
    let mut am_routine =
        unsafe { std::mem::MaybeUninit::<pgrx::pg_sys::IndexAmRoutine>::zeroed().assume_init() };

    am_routine.type_ = pgrx::pg_sys::NodeTag_T_IndexAmRoutine;

    am_routine.amstrategies = 1;
    am_routine.amsupport = 0;
    am_routine.amoptsprocnum = 0;

    am_routine.amcanorder = false;
    am_routine.amcanorderbyop = true;
    am_routine.amcanbackward = false;
    am_routine.amcanunique = false;
    am_routine.amcanmulticol = false;
    am_routine.amoptionalkey = true;
    am_routine.amsearcharray = false;
    am_routine.amsearchnulls = false;
    am_routine.amstorage = false;
    am_routine.amclusterable = false;
    am_routine.ampredlocks = false;
    am_routine.amcaninclude = false;
    am_routine.amusemaintenanceworkmem = false;
    am_routine.amkeytype = pgrx::pg_sys::InvalidOid;

    am_routine.amvalidate = Some(amvalidate);
    am_routine.amoptions = Some(amoptions);
    am_routine.amcostestimate = Some(amcostestimate);

    am_routine.ambuild = Some(ambuild);
    am_routine.ambuildempty = Some(ambuildempty);
    am_routine.aminsert = Some(aminsert);

    am_routine.ambeginscan = Some(ambeginscan);
    am_routine.amrescan = Some(amrescan);
    am_routine.amgettuple = Some(amgettuple);
    am_routine.amendscan = Some(amendscan);

    am_routine.ambulkdelete = Some(ambulkdelete);
    am_routine.amvacuumcleanup = Some(amvacuumcleanup);

    am_routine
};

#[pgrx::pg_guard]
pub unsafe extern "C" fn amvalidate(opclass_oid: pgrx::pg_sys::Oid) -> bool {
    index_setup::convert_opclass_to_distance(opclass_oid);
    true
}

#[cfg(any(feature = "pg11", feature = "pg12"))]
#[pgrx::pg_guard]
pub unsafe extern "C" fn amoptions(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    use pg_sys::AsPgCStr;
    let tab: &[pg_sys::relopt_parse_elt] = &[pg_sys::relopt_parse_elt {
        optname: "options".as_pg_cstr(),
        opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
        offset: index_setup::helper_offset() as i32,
    }];
    let mut noptions = 0;
    let options = pg_sys::parseRelOptions(reloptions, validate, RELOPT_KIND.get(), &mut noptions);
    if noptions == 0 {
        return std::ptr::null_mut();
    }
    for relopt in std::slice::from_raw_parts_mut(options, noptions as usize) {
        relopt.gen.as_mut().unwrap().lockmode = pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE;
    }
    let rdopts = pg_sys::allocateReloptStruct(index_setup::helper_size(), options, noptions);
    pg_sys::fillRelOptions(
        rdopts,
        index_setup::helper_size(),
        options,
        noptions,
        validate,
        tab.as_ptr(),
        tab.len() as i32,
    );
    pg_sys::pfree(options as pgrx::void_mut_ptr);
    rdopts as *mut pg_sys::bytea
}

#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
#[pgrx::pg_guard]
pub unsafe extern "C" fn amoptions(
    reloptions: pgrx::pg_sys::Datum,
    validate: bool,
) -> *mut pgrx::pg_sys::bytea {
    use pgrx::pg_sys::AsPgCStr;

    let tab: &[pgrx::pg_sys::relopt_parse_elt] = &[pgrx::pg_sys::relopt_parse_elt {
        optname: "options".as_pg_cstr(),
        opttype: pgrx::pg_sys::relopt_type_RELOPT_TYPE_STRING,
        offset: index_setup::helper_offset() as i32,
    }];
    let rdopts = pgrx::pg_sys::build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND.get(),
        index_setup::helper_size(),
        tab.as_ptr(),
        tab.len() as _,
    );
    rdopts as *mut pgrx::pg_sys::bytea
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amcostestimate(
    _root: *mut pgrx::pg_sys::PlannerInfo,
    path: *mut pgrx::pg_sys::IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut pgrx::pg_sys::Cost,
    index_total_cost: *mut pgrx::pg_sys::Cost,
    index_selectivity: *mut pgrx::pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    if (*path).indexorderbys.is_null() || FILTER_MODE.get() == FilterMode::Skip {
        *index_startup_cost = f64::MAX;
        *index_total_cost = f64::MAX;
        *index_selectivity = 0.0;
        *index_correlation = 0.0;
        *index_pages = 0.0;
        return;
    }
    *index_startup_cost = 0.0;
    *index_total_cost = 0.0;
    *index_selectivity = 1.0;
    *index_correlation = 1.0;
    *index_pages = 0.0;
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuild(
    heap_relation: pgrx::pg_sys::Relation,
    index_relation: pgrx::pg_sys::Relation,
    index_info: *mut pgrx::pg_sys::IndexInfo,
) -> *mut pgrx::pg_sys::IndexBuildResult {
    index_build::build(index_relation, Some((heap_relation, index_info)));
    let mut result = pgrx::PgBox::<pgrx::pg_sys::IndexBuildResult>::alloc0();
    result.heap_tuples = 0.0;
    result.index_tuples = 0.0;
    result.into_pg()
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuildempty(index_relation: pgrx::pg_sys::Relation) {
    index_build::build(index_relation, None);
}

#[cfg(any(feature = "pg11", feature = "pg12", feature = "pg13"))]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    is_null: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    use pgrx::FromDatum;
    let oid = (*index_relation).rd_id;
    let id = Id::from_sys(oid);
    let vector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
    let vector = vector.data().to_vec().into_boxed_slice();
    index_update::update_insert(id, vector, heap_tid);
    true
}

#[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16"))]
#[pgrx::pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pgrx::pg_sys::Relation,
    values: *mut pgrx::pg_sys::Datum,
    is_null: *mut bool,
    heap_tid: pgrx::pg_sys::ItemPointer,
    _heap_relation: pgrx::pg_sys::Relation,
    _check_unique: pgrx::pg_sys::IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: *mut pgrx::pg_sys::IndexInfo,
) -> bool {
    use pgrx::FromDatum;
    let oid = (*index_relation).rd_id;
    let id = Id::from_sys(oid);
    let vector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
    let vector = vector.data().to_vec().into_boxed_slice();
    index_update::update_insert(id, vector, heap_tid);
    true
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambeginscan(
    index_relation: pgrx::pg_sys::Relation,
    n_keys: std::os::raw::c_int,
    n_order_bys: std::os::raw::c_int,
) -> pgrx::pg_sys::IndexScanDesc {
    index_scan::make_scan(index_relation, n_keys, n_order_bys)
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amrescan(
    scan: pgrx::pg_sys::IndexScanDesc,
    keys: pgrx::pg_sys::ScanKey,
    n_keys: std::os::raw::c_int,
    orderbys: pgrx::pg_sys::ScanKey,
    n_orderbys: std::os::raw::c_int,
) {
    index_scan::start_scan(scan, keys, n_keys, orderbys, n_orderbys);
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amgettuple(
    scan: pgrx::pg_sys::IndexScanDesc,
    direction: pgrx::pg_sys::ScanDirection,
) -> bool {
    assert!(direction == pgrx::pg_sys::ScanDirection_ForwardScanDirection);
    index_scan::next_scan(scan)
}

#[pgrx::pg_guard]
pub extern "C" fn amendscan(_scan: pgrx::pg_sys::IndexScanDesc) {}

#[cfg(any(feature = "pg11", feature = "pg12"))]
#[pg_guard]
pub unsafe extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    #[repr(C)]
    pub struct LVRelStats {
        pub useindex: bool,
        pub old_rel_pages: pg_sys::BlockNumber,
        pub rel_pages: pg_sys::BlockNumber,
        pub scanned_pages: pg_sys::BlockNumber,
        pub pinskipped_pages: pg_sys::BlockNumber,
        pub frozenskipped_pages: pg_sys::BlockNumber,
        pub tupcount_pages: pg_sys::BlockNumber,
        pub old_live_tuples: libc::c_double,
        pub new_rel_tuples: libc::c_double,
        pub new_live_tuples: libc::c_double,
        pub new_dead_tuples: libc::c_double,
        pub pages_removed: pg_sys::BlockNumber,
        pub tuples_deleted: libc::c_double,
        pub nonempty_pages: pg_sys::BlockNumber,
        pub num_dead_tuples: libc::c_int,
        pub max_dead_tuples: libc::c_int,
        pub dead_tuples: pg_sys::ItemPointer,
        pub num_index_scans: libc::c_int,
        pub latestRemovedXid: pg_sys::TransactionId,
        pub lock_waiter_detected: bool,
    }
    let oid = (*(*info).index).rd_id;
    let id = Id::from_sys(oid);
    let items = callback_state as *mut LVRelStats;
    let deletes =
        std::slice::from_raw_parts((*items).dead_tuples, (*items).num_dead_tuples as usize)
            .iter()
            .copied()
            .map(Pointer::from_sys)
            .collect::<Vec<Pointer>>();
    update_delete(id, deletes);
    let result = PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}

#[cfg(any(feature = "pg13", feature = "pg14"))]
#[pg_guard]
pub unsafe extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    use crate::pg_sys::__IncompleteArrayField;
    #[repr(C)]
    struct LVDeadTuples {
        max_tuples: ::std::os::raw::c_int,
        num_tuples: ::std::os::raw::c_int,
        itemptrs: __IncompleteArrayField<pg_sys::ItemPointerData>,
    }
    let oid = (*(*info).index).rd_id;
    let id = Id::from_sys(oid);
    let items = callback_state as *mut LVDeadTuples;
    let deletes = (*items)
        .itemptrs
        .as_slice((*items).num_tuples as usize)
        .iter()
        .copied()
        .map(Pointer::from_sys)
        .collect::<Vec<Pointer>>();
    update_delete(id, deletes);
    let result = PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}

#[cfg(any(feature = "pg15", feature = "pg16"))]
#[pgrx::pg_guard]
pub unsafe extern "C" fn ambulkdelete(
    info: *mut pgrx::pg_sys::IndexVacuumInfo,
    _stats: *mut pgrx::pg_sys::IndexBulkDeleteResult,
    _callback: pgrx::pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut std::os::raw::c_void,
) -> *mut pgrx::pg_sys::IndexBulkDeleteResult {
    let oid = (*(*info).index).rd_id;
    let id = Id::from_sys(oid);
    let items = callback_state as *mut pgrx::pg_sys::VacDeadItems;
    let deletes = (*items)
        .items
        .as_slice((*items).num_items as usize)
        .iter()
        .copied()
        .map(Pointer::from_sys)
        .collect::<Vec<Pointer>>();
    index_update::update_delete(id, deletes);
    let result = pgrx::PgBox::<pgrx::pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amvacuumcleanup(
    _info: *mut pgrx::pg_sys::IndexVacuumInfo,
    _stats: *mut pgrx::pg_sys::IndexBulkDeleteResult,
) -> *mut pgrx::pg_sys::IndexBulkDeleteResult {
    let result = pgrx::PgBox::<pgrx::pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}
