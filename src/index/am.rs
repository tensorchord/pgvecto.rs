#![allow(unsafe_op_in_unsafe_fn)]

use super::am_build;
use super::am_scan;
use super::am_setup;
use super::am_update;
use crate::gucs::planning::ENABLE_INDEX;
use crate::index::utils::{from_datum, get_handle};
use crate::utils::cells::PgCell;
use crate::utils::sys::IntoSys;
use pgrx::datum::Internal;
use pgrx::pg_sys::Datum;

static RELOPT_KIND: PgCell<pgrx::pg_sys::relopt_kind> = unsafe { PgCell::new(0) };

pub unsafe fn init() {
    use pgrx::pg_sys::AsPgCStr;
    RELOPT_KIND.set(pgrx::pg_sys::add_reloption_kind());
    pgrx::pg_sys::add_string_reloption(
        RELOPT_KIND.get(),
        "options".as_pg_cstr(),
        "".as_pg_cstr(),
        "".as_pg_cstr(),
        None,
        pgrx::pg_sys::AccessExclusiveLock as pgrx::pg_sys::LOCKMODE,
    );
}

#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_amhandler(internal) RETURNS index_am_handler
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_amhandler(_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Internal {
    type T = pgrx::pg_sys::IndexAmRoutine;
    unsafe {
        let index_am_routine = pgrx::pg_sys::palloc0(std::mem::size_of::<T>()) as *mut T;
        index_am_routine.write(AM_HANDLER);
        Internal::from(Some(Datum::from(index_am_routine)))
    }
}

const AM_HANDLER: pgrx::pg_sys::IndexAmRoutine = {
    let mut am_routine =
        unsafe { std::mem::MaybeUninit::<pgrx::pg_sys::IndexAmRoutine>::zeroed().assume_init() };

    am_routine.type_ = pgrx::pg_sys::NodeTag::T_IndexAmRoutine;

    am_routine.amcanorderbyop = true;

    // Index access methods that set `amoptionalkey` to `false`
    // must index all tuples, even if the first column is `NULL`.
    // However, PostgreSQL does not generate a path if there is no
    // index clauses, even if there is a `ORDER BY` clause.
    // So we have to set it to `true` and set costs of every path
    // for vector index scans without `ORDER BY` clauses a large number
    // and throw errors if someone really wants such a path.
    am_routine.amoptionalkey = true;

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
    if am_setup::convert_opclass_to_vd(opclass_oid).is_some() {
        pgrx::info!("Vector indexes can only be built on built-in operator classes.");
        true
    } else {
        false
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amoptions(reloptions: Datum, validate: bool) -> *mut pgrx::pg_sys::bytea {
    use pgrx::pg_sys::AsPgCStr;

    let tab: &[pgrx::pg_sys::relopt_parse_elt] = &[pgrx::pg_sys::relopt_parse_elt {
        optname: "options".as_pg_cstr(),
        opttype: pgrx::pg_sys::relopt_type_RELOPT_TYPE_STRING,
        offset: am_setup::helper_offset() as i32,
    }];
    let rdopts = pgrx::pg_sys::build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND.get(),
        am_setup::helper_size(),
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
    if (*path).indexorderbys.is_null() || !ENABLE_INDEX.get() {
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
    heap: pgrx::pg_sys::Relation,
    index: pgrx::pg_sys::Relation,
    index_info: *mut pgrx::pg_sys::IndexInfo,
) -> *mut pgrx::pg_sys::IndexBuildResult {
    initialize_meta(index, pgrx::pg_sys::ForkNumber_MAIN_FORKNUM);
    make_well_formed(index);
    let result = pgrx::PgBox::<pgrx::pg_sys::IndexBuildResult>::alloc0();
    am_build::build_insertions(index, heap, index_info, result.as_ptr());
    result.into_pg()
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuildempty(index: pgrx::pg_sys::Relation) {
    initialize_meta(index, pgrx::pg_sys::ForkNumber_INIT_FORKNUM);
}

unsafe fn initialize_meta(index: pgrx::pg_sys::Relation, forknum: i32) {
    unsafe {
        let meta_buffer = scopeguard::guard(
            {
                let meta_buffer = pgrx::pg_sys::ReadBufferExtended(
                    index,
                    forknum,
                    0xFFFFFFFF,
                    pgrx::pg_sys::ReadBufferMode_RBM_NORMAL,
                    std::ptr::null_mut(),
                );
                pgrx::pg_sys::LockBuffer(meta_buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
                meta_buffer
            },
            |meta_buffer| {
                pgrx::pg_sys::UnlockReleaseBuffer(meta_buffer);
            },
        );
        let xlog = pgrx::pg_sys::GenericXLogStart(index);
        pgrx::pg_sys::GenericXLogRegisterBuffer(
            xlog,
            *meta_buffer,
            pgrx::pg_sys::GENERIC_XLOG_FULL_IMAGE as _,
        );
        pgrx::pg_sys::GenericXLogFinish(xlog);
    }
}

#[repr(C)]
struct VectorsPageOpaqueData {
    version: u32,
    _reserved: [u8; 2044],
}

const _: () = assert!(std::mem::size_of::<VectorsPageOpaqueData>() == 2048);

unsafe fn make_well_formed(index: pgrx::pg_sys::Relation) {
    unsafe {
        let meta_buffer = scopeguard::guard(
            {
                let meta_buffer = pgrx::pg_sys::ReadBuffer(index, 0);
                pgrx::pg_sys::LockBuffer(meta_buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
                meta_buffer
            },
            |meta_buffer| {
                pgrx::pg_sys::UnlockReleaseBuffer(meta_buffer);
            },
        );
        let meta_page = pgrx::pg_sys::BufferGetPage(*meta_buffer);
        let meta_header = meta_page.cast::<pgrx::pg_sys::PageHeaderData>();
        let meta_offset = (*meta_header).pd_special as usize;
        let meta_opaque = meta_page.add(meta_offset).cast::<VectorsPageOpaqueData>();
        if (*meta_opaque).version != 0 {
            return;
        }
        am_build::build(index);
        let xlog = pgrx::pg_sys::GenericXLogStart(index);
        let xlog_meta_page = pgrx::pg_sys::GenericXLogRegisterBuffer(
            xlog,
            *meta_buffer,
            pgrx::pg_sys::GENERIC_XLOG_FULL_IMAGE as _,
        );
        pgrx::pg_sys::PageInit(
            xlog_meta_page,
            pgrx::pg_sys::BLCKSZ as usize,
            std::mem::size_of::<VectorsPageOpaqueData>(),
        );
        let xlog_meta_header = xlog_meta_page.cast::<pgrx::pg_sys::PageHeaderData>();
        let xlog_meta_offset = (*xlog_meta_header).pd_special as usize;
        let xlog_meta_opaque = xlog_meta_page
            .add(xlog_meta_offset)
            .cast::<VectorsPageOpaqueData>();
        (*xlog_meta_opaque).version = 1;
        pgrx::pg_sys::GenericXLogFinish(xlog);
    }
}

unsafe fn check_well_formed(index: pgrx::pg_sys::Relation) {
    if !test_well_formed(index) {
        make_well_formed(index);
    }
}

unsafe fn test_well_formed(index: pgrx::pg_sys::Relation) -> bool {
    unsafe {
        let meta_buffer = scopeguard::guard(
            {
                let meta_buffer = pgrx::pg_sys::ReadBuffer(index, 0);
                pgrx::pg_sys::LockBuffer(meta_buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
                meta_buffer
            },
            |meta_buffer| {
                pgrx::pg_sys::UnlockReleaseBuffer(meta_buffer);
            },
        );
        let meta_page = pgrx::pg_sys::BufferGetPage(*meta_buffer);
        let meta_header = meta_page.cast::<pgrx::pg_sys::PageHeaderData>();
        let meta_offset = (*meta_header).pd_special as usize;
        let meta_opaque = meta_page.add(meta_offset).cast::<VectorsPageOpaqueData>();
        (*meta_opaque).version == 1
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn aminsert(
    index: pgrx::pg_sys::Relation,
    values: *mut Datum,
    is_null: *mut bool,
    heap_tid: pgrx::pg_sys::ItemPointer,
    _heap: pgrx::pg_sys::Relation,
    _check_unique: pgrx::pg_sys::IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: *mut pgrx::pg_sys::IndexInfo,
) -> bool {
    check_well_formed(index);
    let oid = (*index).rd_id;
    let id = get_handle(oid);
    let vector = from_datum(*values.add(0), *is_null.add(0));
    if let Some(v) = vector {
        am_update::update_insert(id, v, *heap_tid);
    }
    false
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambeginscan(
    index: pgrx::pg_sys::Relation,
    n_keys: std::os::raw::c_int,
    n_orderbys: std::os::raw::c_int,
) -> pgrx::pg_sys::IndexScanDesc {
    check_well_formed(index);
    assert!(n_keys == 0);
    assert!(n_orderbys == 1);
    am_scan::make_scan(index)
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amrescan(
    scan: pgrx::pg_sys::IndexScanDesc,
    _keys: pgrx::pg_sys::ScanKey,
    n_keys: std::os::raw::c_int,
    orderbys: pgrx::pg_sys::ScanKey,
    n_orderbys: std::os::raw::c_int,
) {
    assert!((*scan).numberOfKeys == n_keys);
    assert!((*scan).numberOfOrderBys == n_orderbys);
    assert!(n_keys == 0);
    assert!(n_orderbys == 1);
    am_scan::start_scan(scan, orderbys);
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amgettuple(
    scan: pgrx::pg_sys::IndexScanDesc,
    direction: pgrx::pg_sys::ScanDirection,
) -> bool {
    assert!(direction == pgrx::pg_sys::ScanDirection_ForwardScanDirection);
    am_scan::next_scan(scan)
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amendscan(scan: pgrx::pg_sys::IndexScanDesc) {
    am_scan::end_scan(scan);
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambulkdelete(
    info: *mut pgrx::pg_sys::IndexVacuumInfo,
    _stats: *mut pgrx::pg_sys::IndexBulkDeleteResult,
    callback: pgrx::pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut std::os::raw::c_void,
) -> *mut pgrx::pg_sys::IndexBulkDeleteResult {
    if !test_well_formed((*info).index) {
        pgrx::warning!("The vector index is not initialized.");
        let result = pgrx::PgBox::<pgrx::pg_sys::IndexBulkDeleteResult>::alloc0();
        return result.into_pg();
    }
    let oid = (*(*info).index).rd_id;
    let id = get_handle(oid);
    if let Some(callback) = callback {
        am_update::update_delete(id, |pointer| {
            callback(
                &mut pointer.into_sys() as *mut pgrx::pg_sys::ItemPointerData,
                callback_state,
            )
        });
    }
    let result = pgrx::PgBox::<pgrx::pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amvacuumcleanup(
    info: *mut pgrx::pg_sys::IndexVacuumInfo,
    _stats: *mut pgrx::pg_sys::IndexBulkDeleteResult,
) -> *mut pgrx::pg_sys::IndexBulkDeleteResult {
    if !test_well_formed((*info).index) {
        pgrx::warning!("The vector index is not initialized.");
    }
    let result = pgrx::PgBox::<pgrx::pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}
