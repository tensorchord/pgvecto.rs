use super::am_options;
use super::am_scan;
use crate::error::*;
use crate::gucs::planning::ENABLE_INDEX;
use crate::index::am_scan::Scanner;
use crate::index::catalog::{on_index_build, on_index_write};
use crate::index::utils::{ctid_to_pointer, pointer_to_ctid};
use crate::index::utils::{from_datum_to_range, from_datum_to_vector, from_oid_to_handle};
use crate::ipc::{client, ClientRpc};
use crate::utils::cells::PgCell;
use am_options::Reloption;
use base::index::*;
use base::scalar::ScalarLike;
use base::vector::OwnedVector;
use pgrx::datum::Internal;
use pgrx::pg_sys::Datum;

static RELOPT_KIND_VECTORS: PgCell<pgrx::pg_sys::relopt_kind> = unsafe { PgCell::new(0) };

pub unsafe fn init() {
    unsafe {
        RELOPT_KIND_VECTORS.set(pgrx::pg_sys::add_reloption_kind());
        pgrx::pg_sys::add_string_reloption(
            RELOPT_KIND_VECTORS.get(),
            c"options".as_ptr(),
            c"Vector index options, represented as a TOML string.".as_ptr(),
            c"".as_ptr(),
            None,
            pgrx::pg_sys::AccessExclusiveLock as pgrx::pg_sys::LOCKMODE,
        );
    }
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
    am_routine.ambulkdelete = Some(ambulkdelete);
    am_routine.amvacuumcleanup = Some(amvacuumcleanup);

    am_routine.ambeginscan = Some(ambeginscan);
    am_routine.amrescan = Some(amrescan);
    am_routine.amgettuple = Some(amgettuple);
    am_routine.amendscan = Some(amendscan);

    am_routine
};

#[pgrx::pg_guard]
pub unsafe extern "C" fn amvalidate(opclass_oid: pgrx::pg_sys::Oid) -> bool {
    if am_options::convert_opclass_to_vd(opclass_oid).is_some() {
        pgrx::info!("Vector indexes can only be built on built-in operator classes.");
        true
    } else {
        false
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amoptions(reloptions: Datum, validate: bool) -> *mut pgrx::pg_sys::bytea {
    let rdopts = unsafe {
        pgrx::pg_sys::build_reloptions(
            reloptions,
            validate,
            RELOPT_KIND_VECTORS.get(),
            std::mem::size_of::<Reloption>(),
            Reloption::TAB.as_ptr(),
            Reloption::TAB.len() as _,
        )
    };
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
    unsafe {
        if ((*path).indexorderbys.is_null() && (*path).indexclauses.is_null())
            || !ENABLE_INDEX.get()
        {
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
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuild(
    heap: pgrx::pg_sys::Relation,
    index: pgrx::pg_sys::Relation,
    index_info: *mut pgrx::pg_sys::IndexInfo,
) -> *mut pgrx::pg_sys::IndexBuildResult {
    pub struct Builder {
        pub rpc: ClientRpc,
        pub result: *mut pgrx::pg_sys::IndexBuildResult,
    }
    let oid = unsafe { (*index).rd_id };
    let handle = from_oid_to_handle(oid);
    let (options, alterable_options) = unsafe { am_options::options(index) };
    let mut rpc = check_client(client());
    match rpc.create(handle, options, alterable_options) {
        Ok(()) => (),
        Err(CreateError::InvalidIndexOptions { reason }) => {
            bad_service_invalid_index_options(&reason);
        }
    }
    on_index_build(handle);
    match rpc.stop(handle) {
        Ok(()) => (),
        Err(StopError::NotExist) => pgrx::error!("internal error"),
    }
    let result = unsafe { pgrx::PgBox::<pgrx::pg_sys::IndexBuildResult>::alloc0() };
    let mut builder = Builder {
        rpc,
        result: result.as_ptr(),
    };
    let table_am = unsafe { &*(*heap).rd_tableam };
    unsafe {
        table_am.index_build_range_scan.unwrap()(
            heap,
            index,
            index_info,
            true,
            false,
            true,
            0,
            pgrx::pg_sys::InvalidBlockNumber,
            Some(callback),
            (&mut builder) as *mut Builder as *mut std::os::raw::c_void,
            std::ptr::null_mut(),
        );
    }
    #[pgrx::pg_guard]
    unsafe extern "C" fn callback(
        index: pgrx::pg_sys::Relation,
        ctid: pgrx::pg_sys::ItemPointer,
        values: *mut Datum,
        is_null: *mut bool,
        _tuple_is_alive: bool,
        state: *mut std::os::raw::c_void,
    ) {
        let state = unsafe { &mut *state.cast::<Builder>() };
        let vector = unsafe { from_datum_to_vector(*values.add(0), *is_null.add(0)) };
        if let Some(vector) = vector {
            let oid = unsafe { (*index).rd_id };
            let handle = from_oid_to_handle(oid);
            let pointer = ctid_to_pointer(unsafe { ctid.read() });
            match state.rpc.insert(handle, vector, pointer) {
                Ok(()) => (),
                Err(InsertError::NotExist) => bad_service_not_exist(),
                Err(InsertError::InvalidVector) => bad_service_invalid_vector(),
            }
            unsafe {
                (*state.result).index_tuples += 1.0;
            }
        }
        unsafe {
            (*state.result).heap_tuples += 1.0;
        }
    }
    let mut rpc = builder.rpc;
    match rpc.start(handle) {
        Ok(()) => (),
        Err(StartError::NotExist) => pgrx::error!("internal error"),
    }
    loop {
        pgrx::check_for_interrupts!();
        match rpc.stat(handle) {
            Ok(s) => {
                if !s.indexing {
                    break;
                }
            }
            Err(StatError::NotExist) => pgrx::error!("internal error"),
        }
        unsafe {
            pgrx::pg_sys::WaitLatch(
                pgrx::pg_sys::MyLatch,
                (pgrx::pg_sys::WL_LATCH_SET
                    | pgrx::pg_sys::WL_TIMEOUT
                    | pgrx::pg_sys::WL_EXIT_ON_PM_DEATH) as _,
                1000,
                pgrx::pg_sys::WaitEventTimeout_WAIT_EVENT_PG_SLEEP,
            );
            pgrx::pg_sys::ResetLatch(pgrx::pg_sys::MyLatch);
        }
    }
    result.into_pg()
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambuildempty(_index: pgrx::pg_sys::Relation) {
    pgrx::error!("Unlogged indexes are not supported.");
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
    let oid = unsafe { (*index).rd_id };
    let handle = from_oid_to_handle(oid);
    let vector = unsafe { from_datum_to_vector(*values.add(0), *is_null.add(0)) };
    if let Some(vector) = vector {
        let pointer = ctid_to_pointer(unsafe { heap_tid.read() });

        on_index_write(handle);

        let mut rpc = check_client(client());

        match rpc.insert(handle, vector, pointer) {
            Ok(()) => (),
            Err(InsertError::NotExist) => bad_service_not_exist(),
            Err(InsertError::InvalidVector) => bad_service_invalid_vector(),
        }
    }
    false
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambeginscan(
    index: pgrx::pg_sys::Relation,
    n_keys: std::os::raw::c_int,
    n_orderbys: std::os::raw::c_int,
) -> pgrx::pg_sys::IndexScanDesc {
    use pgrx::PgMemoryContexts::CurrentMemoryContext;

    let scan = unsafe { pgrx::pg_sys::RelationGetIndexScan(index, n_keys, n_orderbys) };
    unsafe {
        let scanner = am_scan::scan_make(None, None);
        (*scan).opaque = CurrentMemoryContext.leak_and_drop_on_delete(scanner).cast();
    }
    scan
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amrescan(
    scan: pgrx::pg_sys::IndexScanDesc,
    keys: pgrx::pg_sys::ScanKey,
    _n_keys: std::os::raw::c_int,
    orderbys: pgrx::pg_sys::ScanKey,
    _n_orderbys: std::os::raw::c_int,
) {
    unsafe {
        if (*scan).numberOfOrderBys < 0 || (*scan).numberOfOrderBys > 1 {
            pgrx::error!(
                "vector search with {} ORDER BY clauses is not supported",
                (*scan).numberOfOrderBys
            );
        }
        if (*scan).numberOfKeys < 0 {
            pgrx::error!(
                "vector search with {} WHERE clauses is not supported",
                (*scan).numberOfKeys
            );
        }
        if !keys.is_null() && (*scan).numberOfKeys > 0 {
            std::ptr::copy(keys, (*scan).keyData, (*scan).numberOfKeys as _);
        }
        if !orderbys.is_null() && (*scan).numberOfOrderBys > 0 {
            std::ptr::copy(orderbys, (*scan).orderByData, (*scan).numberOfOrderBys as _);
        }
        (*scan).xs_recheck = false;
        let orderby_vector = match (*scan).numberOfOrderBys {
            0 => None,
            1 => {
                let data = (*scan).orderByData.add(0);
                let value = (*data).sk_argument;
                let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
                from_datum_to_vector(value, is_null)
            }
            _ => unreachable!(),
        };
        let (range_vector, threshold) = match (*scan).numberOfKeys {
            0 => (None, None),
            1 => {
                let data = (*scan).keyData.add(0);
                let value = (*data).sk_argument;
                let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
                let (options, _) = am_options::options((*scan).indexRelation);
                let (v, threshold) = from_datum_to_range(value, &options.vector, is_null);
                if orderby_vector.is_none() || (orderby_vector.is_some() && v == orderby_vector) {
                    (v, threshold)
                } else {
                    (*scan).xs_recheck = true;
                    (None, None)
                }
            }
            n if orderby_vector.is_some() => {
                // Pick range vector by orderby vector
                let mut vector: Option<OwnedVector> = None;
                let mut threshold: Option<f32> = None;

                let (options, _) = am_options::options((*scan).indexRelation);
                for i in 0..n as usize {
                    let data = (*scan).keyData.add(i);
                    if (*data).sk_strategy != 2 {
                        continue;
                    }
                    let value = (*data).sk_argument;
                    let is_null = ((*data).sk_flags & pgrx::pg_sys::SK_ISNULL as i32) != 0;
                    let (v, t) = from_datum_to_range(value, &options.vector, is_null);
                    if v == orderby_vector && vector.is_none() {
                        (vector, threshold) = (v, t);
                    } else if v == orderby_vector && vector.is_some() {
                        pgrx::error!(
                            "vector search with two WHERE clause of same key is not supported"
                        );
                    }
                }
                (*scan).xs_recheck = true;
                (vector, threshold)
            }
            _ if orderby_vector.is_none() => {
                (*scan).xs_recheck = true;
                (None, None)
            }
            _ => unreachable!(),
        };

        let vector = match (orderby_vector, range_vector) {
            (Some(v), _) => Some(v),
            (None, Some(v)) => Some(v),
            (None, None) => None,
        };

        let scanner = (*scan).opaque.cast::<Scanner>().as_mut().unwrap_unchecked();
        let scanner = std::mem::replace(scanner, am_scan::scan_make(vector, threshold));
        am_scan::scan_release(scanner);
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amgettuple(
    scan: pgrx::pg_sys::IndexScanDesc,
    direction: pgrx::pg_sys::ScanDirection,
) -> bool {
    if direction != pgrx::pg_sys::ScanDirection_ForwardScanDirection {
        pgrx::error!("vector search without a forward scan direction is not supported");
    }
    // https://www.postgresql.org/docs/current/index-locking.html
    // If heap entries referenced physical pointers are deleted before
    // they are consumed by PostgreSQL, PostgreSQL will received wrong
    // physical pointers: no rows or irreverent rows are referenced.
    if unsafe { (*(*scan).xs_snapshot).snapshot_type } != pgrx::pg_sys::SnapshotType_SNAPSHOT_MVCC {
        pgrx::error!("scanning with a non-MVCC-compliant snapshot is not supported");
    }
    let scanner = unsafe { (*scan).opaque.cast::<Scanner>().as_mut().unwrap_unchecked() };
    let oid = unsafe { (*(*scan).indexRelation).rd_id };
    let handle = from_oid_to_handle(oid);
    if let Some((distance, pointer)) = am_scan::scan_next(scanner, handle) {
        let ctid = pointer_to_ctid(pointer);
        unsafe {
            (*scan).xs_heaptid = ctid;
            (*scan).xs_recheckorderby = false;
        }
        if let Some(threshold) = scanner.threshold() {
            distance.to_f32() < threshold
        } else {
            true
        }
    } else {
        false
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amendscan(scan: pgrx::pg_sys::IndexScanDesc) {
    unsafe {
        let scanner = (*scan).opaque.cast::<Scanner>().as_mut().unwrap_unchecked();
        let scanner = std::mem::replace(scanner, am_scan::scan_make(None, None));
        am_scan::scan_release(scanner);
    }
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn ambulkdelete(
    info: *mut pgrx::pg_sys::IndexVacuumInfo,
    stats: *mut pgrx::pg_sys::IndexBulkDeleteResult,
    callback: pgrx::pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut std::os::raw::c_void,
) -> *mut pgrx::pg_sys::IndexBulkDeleteResult {
    let mut stats = stats;
    if stats.is_null() {
        stats = unsafe {
            pgrx::pg_sys::palloc0(std::mem::size_of::<pgrx::pg_sys::IndexBulkDeleteResult>()).cast()
        };
    }
    let oid = unsafe { (*(*info).index).rd_id };
    let handle = from_oid_to_handle(oid);
    if let Some(callback) = callback {
        on_index_write(handle);

        let mut x = match check_client(client()).list(handle) {
            Ok(x) => x,
            Err((_, ListError::NotExist)) => bad_service_not_exist(),
        };
        let mut y = check_client(client());
        while let Some(pointer) = x.next() {
            let mut ctid = pointer_to_ctid(pointer);
            if unsafe { callback(&mut ctid, callback_state) } {
                match y.delete(handle, pointer) {
                    Ok(()) => (),
                    Err(DeleteError::NotExist) => (),
                }
            }
        }
        x.leave();
    }
    stats
}

#[pgrx::pg_guard]
pub unsafe extern "C" fn amvacuumcleanup(
    _info: *mut pgrx::pg_sys::IndexVacuumInfo,
    _stats: *mut pgrx::pg_sys::IndexBulkDeleteResult,
) -> *mut pgrx::pg_sys::IndexBulkDeleteResult {
    std::ptr::null_mut()
}
