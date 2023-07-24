use crate::bgworker::ClientBuild;
use crate::postgres::datatype::VectorInput;
use crate::postgres::datatype::VectorTypmod;
use crate::postgres::gucs::K;
use crate::postgres::hooks::client;
use crate::postgres::hooks::flush_if_commit;
use crate::prelude::*;
use pg_sys::Datum;
use pgrx::prelude::*;
use serde::{Deserialize, Serialize};
use std::cell::Cell;
use std::ffi::CStr;
use validator::Validate;

#[thread_local]
static RELOPT_KIND: Cell<pg_sys::relopt_kind> = Cell::new(0);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartialOptions {
    capacity: usize,
    #[serde(default = "PartialOptions::default_size_ram")]
    size_ram: usize,
    #[serde(default = "PartialOptions::default_size_disk")]
    size_disk: usize,
    storage_vectors: Storage,
    algorithm: AlgorithmOptions,
}

impl PartialOptions {
    fn default_size_ram() -> usize {
        16384
    }
    fn default_size_disk() -> usize {
        16384
    }
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
struct PartialOptionsHelper {
    vl_len_: i32,
    offset: i32,
}

impl PartialOptionsHelper {
    unsafe fn get(this: *const Self) -> PartialOptions {
        if (*this).offset == 0 {
            panic!("`options` cannot be null.")
        } else {
            let ptr = (this as *const std::os::raw::c_char).offset((*this).offset as isize);
            toml::from_str::<PartialOptions>(CStr::from_ptr(ptr).to_str().unwrap()).unwrap()
        }
    }
}

struct BuildState<'a> {
    build: ClientBuild<'a>,
    ntuples: f64,
}

struct ScanState {
    data: Option<Vec<Pointer>>,
}

pub unsafe fn init() {
    use pg_sys::AsPgCStr;
    RELOPT_KIND.set(pg_sys::add_reloption_kind());
    pg_sys::add_string_reloption(
        RELOPT_KIND.get(),
        "options".as_pg_cstr(),
        "".as_pg_cstr(),
        "".as_pg_cstr(),
        None,
        #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
}

#[pg_extern(sql = "
    CREATE OR REPLACE FUNCTION vectors_amhandler(internal) RETURNS index_am_handler
    PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
    CREATE ACCESS METHOD vectors TYPE INDEX HANDLER vectors_amhandler;
    COMMENT ON ACCESS METHOD vectors IS 'pgvecto.rs index access method';
", requires = ["vector"])]
fn vectors_amhandler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut am_routine =
        unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc_node(pg_sys::NodeTag_T_IndexAmRoutine) };

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

    am_routine.into_pg_boxed()
}

#[pg_guard]
unsafe extern "C" fn amvalidate(opclass_oid: pg_sys::Oid) -> bool {
    validate_opclass(opclass_oid);
    true
}

#[cfg(any(feature = "pg11", feature = "pg12"))]
#[pg_guard]
unsafe extern "C" fn amoptions(reloptions: pg_sys::Datum, validate: bool) -> *mut pg_sys::bytea {
    use pg_sys::AsPgCStr;
    let tab: &[pg_sys::relopt_parse_elt] = &[pg_sys::relopt_parse_elt {
        optname: "options".as_pg_cstr(),
        opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
        offset: memoffset::offset_of!(PartialOptionsHelper, offset) as i32,
    }];
    let mut noptions = 0;
    let options = pg_sys::parseRelOptions(reloptions, validate, RELOPT_KIND.get(), &mut noptions);
    if noptions == 0 {
        return std::ptr::null_mut();
    }
    for relopt in std::slice::from_raw_parts_mut(options, noptions as usize) {
        relopt.gen.as_mut().unwrap().lockmode = pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE;
    }
    let rdopts = pg_sys::allocateReloptStruct(
        std::mem::size_of::<PartialOptionsHelper>(),
        options,
        noptions,
    );
    pg_sys::fillRelOptions(
        rdopts,
        std::mem::size_of::<PartialOptionsHelper>(),
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
#[pg_guard]
unsafe extern "C" fn amoptions(reloptions: pg_sys::Datum, validate: bool) -> *mut pg_sys::bytea {
    use pg_sys::AsPgCStr;
    let tab: &[pg_sys::relopt_parse_elt] = &[pg_sys::relopt_parse_elt {
        optname: "options".as_pg_cstr(),
        opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
        offset: memoffset::offset_of!(PartialOptionsHelper, offset) as i32,
    }];
    let rdopts = pg_sys::build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND.get(),
        std::mem::size_of::<PartialOptionsHelper>(),
        tab.as_ptr(),
        tab.len() as _,
    );
    rdopts as *mut pg_sys::bytea
}

#[pg_guard]
unsafe extern "C" fn amcostestimate(
    _root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    index_selectivity: *mut pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    if (*path).indexorderbys.is_null() {
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

#[pg_guard]
unsafe extern "C" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    _index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let oid = (*index_relation).rd_id;
    let id = Id::from_sys(oid);
    flush_if_commit(id);
    let options = options(index_relation);
    let mut client = client();
    let mut state = BuildState {
        build: client.build(id, options).unwrap(),
        ntuples: 0.0,
    };
    #[cfg(any(feature = "pg11", feature = "pg12"))]
    #[pg_guard]
    unsafe extern "C" fn callback(
        _index_relation: pg_sys::Relation,
        htup: pg_sys::HeapTuple,
        values: *mut pg_sys::Datum,
        is_null: *mut bool,
        _tuple_is_alive: bool,
        state: *mut std::os::raw::c_void,
    ) {
        let ctid = &(*htup).t_self;
        let state = &mut *(state as *mut BuildState);
        let pgvector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
        let data = (
            pgvector.to_vec().into_boxed_slice(),
            Pointer::from_sys(*ctid),
        );
        state.build.next(data).unwrap();
        state.ntuples += 1.0;
    }
    #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15", feature = "pg16"))]
    #[pg_guard]
    unsafe extern "C" fn callback(
        _index_relation: pg_sys::Relation,
        ctid: pg_sys::ItemPointer,
        values: *mut pg_sys::Datum,
        is_null: *mut bool,
        _tuple_is_alive: bool,
        state: *mut std::os::raw::c_void,
    ) {
        let state = &mut *(state as *mut BuildState);
        let pgvector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
        let data = (
            pgvector.to_vec().into_boxed_slice(),
            Pointer::from_sys(*ctid),
        );
        state.build.next(data).unwrap();
    }
    let index_info = pg_sys::BuildIndexInfo(index_relation);
    pg_sys::IndexBuildHeapScan(
        heap_relation,
        index_relation,
        index_info,
        Some(callback),
        &mut state,
    );
    state.build.finish().unwrap();
    let mut result = PgBox::<pg_sys::IndexBuildResult>::alloc0();
    result.heap_tuples = state.ntuples;
    result.index_tuples = 0.0;
    result.into_pg()
}

#[pg_guard]
unsafe extern "C" fn ambuildempty(index_relation: pg_sys::Relation) {
    let oid = (*index_relation).rd_id;
    let id = Id::from_sys(oid);
    flush_if_commit(id);
    let options = options(index_relation);
    let mut client = client();
    let build = client.build(id, options).unwrap();
    build.finish().unwrap();
}

#[cfg(any(feature = "pg11", feature = "pg12", feature = "pg13"))]
#[pg_guard]
unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    is_null: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    _aminsert(index_relation, values, is_null, heap_tid)
}

#[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16"))]
#[pg_guard]
unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    is_null: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    _aminsert(index_relation, values, is_null, heap_tid)
}

#[pg_guard]
unsafe extern "C" fn _aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    is_null: *mut bool,
    heap_tid: pg_sys::ItemPointer,
) -> bool {
    let oid = (*index_relation).rd_id;
    let id = Id::from_sys(oid);
    flush_if_commit(id);
    let pgvector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
    let vector = pgvector.data().to_vec().into_boxed_slice();
    let p = Pointer::from_sys(*heap_tid);
    client().insert(id, (vector, p)).unwrap();
    true
}

#[pg_guard]
unsafe extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    n_keys: std::os::raw::c_int,
    n_order_bys: std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    let mut scan = PgBox::from_pg(pg_sys::RelationGetIndexScan(
        index_relation,
        n_keys,
        n_order_bys,
    ));

    let state = ScanState { data: None };

    scan.opaque = pgrx::PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(state)
        as pgrx::void_mut_ptr;

    scan.into_pg()
}

#[pg_guard]
unsafe extern "C" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    n_keys: std::os::raw::c_int,
    orderbys: pg_sys::ScanKey,
    n_orderbys: std::os::raw::c_int,
) {
    let oid = (*(*scan).indexRelation).rd_id;
    let id = Id::from_sys(oid);
    if n_orderbys > 0 {
        let orderbys = std::slice::from_raw_parts_mut(orderbys, n_orderbys as usize);
        std::ptr::copy(orderbys.as_ptr(), (*scan).orderByData, orderbys.len());
    }
    if n_keys > 0 {
        let keys = std::slice::from_raw_parts_mut(keys, n_keys as usize);
        std::ptr::copy(keys.as_ptr(), (*scan).keyData, keys.len());
    }
    if (*scan).numberOfOrderBys > 0 {
        use pg_sys::{palloc, palloc0};
        let size_datum = std::mem::size_of::<Datum>();
        let size_bool = std::mem::size_of::<bool>();
        let orderbyvals = palloc0(size_datum * (*scan).numberOfOrderBys as usize) as *mut Datum;
        let orderbynulls = palloc(size_bool * (*scan).numberOfOrderBys as usize) as *mut bool;
        orderbynulls.write_bytes(1, (*scan).numberOfOrderBys as usize);
        (*scan).xs_orderbyvals = orderbyvals;
        (*scan).xs_orderbynulls = orderbynulls;
    }
    assert!(n_orderbys == 1, "Not supported.");
    let state = &mut *((*scan).opaque as *mut ScanState);
    let scan_vector = (*orderbys.add(0)).sk_argument;
    let dt_vector = VectorInput::from_datum(scan_vector, false).unwrap();
    let vector = dt_vector.data();
    state.data = {
        let k = K.get() as _;
        let mut data = client()
            .search(id, (vector.to_vec().into_boxed_slice(), k))
            .unwrap();
        data.reverse();
        Some(data)
    };
}

#[pg_guard]
unsafe extern "C" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection,
) -> bool {
    (*scan).xs_recheck = false;
    (*scan).xs_recheckorderby = false;
    let state = &mut *((*scan).opaque as *mut ScanState);
    if let Some(data) = state.data.as_mut() {
        if let Some(p) = data.pop() {
            #[cfg(any(feature = "pg11"))]
            {
                (*scan).xs_ctup.t_self = p.into_sys();
            }
            #[cfg(not(feature = "pg11"))]
            {
                (*scan).xs_heaptid = p.into_sys();
            }
            true
        } else {
            false
        }
    } else {
        unreachable!()
    }
}

#[pg_guard]
extern "C" fn amendscan(_scan: pg_sys::IndexScanDesc) {}

#[cfg(any(feature = "pg11", feature = "pg12"))]
#[pg_guard]
unsafe extern "C" fn ambulkdelete(
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
    flush_if_commit(id);
    let items = callback_state as *mut LVRelStats;
    let deletes =
        std::slice::from_raw_parts((*items).dead_tuples, (*items).num_dead_tuples as usize)
            .iter()
            .copied()
            .map(Pointer::from_sys)
            .collect::<Vec<Pointer>>();
    for message in deletes {
        client().delete(id, message).unwrap();
    }
    let result = PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}

#[cfg(any(feature = "pg13", feature = "pg14"))]
#[pg_guard]
unsafe extern "C" fn ambulkdelete(
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
    flush_if_commit(id);
    let items = callback_state as *mut LVDeadTuples;
    let deletes = (*items)
        .itemptrs
        .as_slice((*items).num_tuples as usize)
        .iter()
        .copied()
        .map(Pointer::from_sys)
        .collect::<Vec<Pointer>>();
    for message in deletes {
        client().delete(id, message).unwrap();
    }
    let result = PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}

#[cfg(any(feature = "pg15", feature = "pg16"))]
#[pg_guard]
unsafe extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let oid = (*(*info).index).rd_id;
    let id = Id::from_sys(oid);
    flush_if_commit(id);
    let items = callback_state as *mut pg_sys::VacDeadItems;
    let deletes = (*items)
        .items
        .as_slice((*items).num_items as usize)
        .iter()
        .copied()
        .map(Pointer::from_sys)
        .collect::<Vec<Pointer>>();
    for message in deletes {
        client().delete(id, message).unwrap();
    }
    let result = PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}

#[pg_guard]
unsafe extern "C" fn amvacuumcleanup(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let result = PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}

unsafe fn options(index_relation: pg_sys::Relation) -> Options {
    let nkeys = (*(*index_relation).rd_index).indnkeyatts;
    let opfamily = (*index_relation).rd_opfamily.read();
    let typmod = (*(*(*index_relation).rd_att).attrs.as_ptr().add(0)).type_mod();
    let options = (*index_relation).rd_options as *mut PartialOptionsHelper;
    if nkeys != 1 {
        panic!("Only supports exactly one key column.");
    }
    if options.is_null() {
        panic!("The options is null.");
    }
    let typmod = VectorTypmod::parse_from_i32(typmod).unwrap();
    let options = PartialOptionsHelper::get(options);
    let options = Options {
        dims: typmod.dims().expect("Column does not have dimensions."),
        distance: validate_opfamily(opfamily),
        capacity: options.capacity,
        size_disk: options.size_disk,
        size_ram: options.size_ram,
        storage_vectors: options.storage_vectors,
        algorithm: options.algorithm,
    };
    options.validate().expect("The options is invalid.");
    options
}

fn regoperatorin(name: &str) -> pg_sys::Oid {
    let cstr = std::ffi::CString::new(name).expect("specified name has embedded NULL byte");
    unsafe {
        pgrx::direct_function_call::<pg_sys::Oid>(
            pg_sys::regoperatorin,
            &[cstr.as_c_str().into_datum()],
        )
        .expect("operator lookup returned NULL")
    }
}

unsafe fn validate_opclass(opclass: pg_sys::Oid) -> Distance {
    let tup = pg_sys::SearchSysCache1(pg_sys::SysCacheIdentifier_CLAOID as _, opclass.into());
    if tup.is_null() {
        panic!("cache lookup failed for operator class {opclass}");
    }
    let classform = pg_sys::GETSTRUCT(tup).cast::<pg_sys::FormData_pg_opclass>();
    let opfamily = (*classform).opcfamily;
    let distance = validate_opfamily(opfamily);
    pg_sys::ReleaseSysCache(tup);
    distance
}

unsafe fn validate_opfamily(opfamily: pg_sys::Oid) -> Distance {
    let tup = pg_sys::SearchSysCache1(pg_sys::SysCacheIdentifier_OPFAMILYOID as _, opfamily.into());
    if tup.is_null() {
        panic!("cache lookup failed for operator family {opfamily}");
    }
    let oprlist = pg_sys::SearchSysCacheList(
        pg_sys::SysCacheIdentifier_AMOPSTRATEGY as _,
        1,
        opfamily.into(),
        0.into(),
        0.into(),
    );
    assert!((*oprlist).n_members == 1);
    let member = (*oprlist).members.as_slice(1)[0];
    let oprtup = &mut (*member).tuple;
    let oprform = pg_sys::GETSTRUCT(oprtup).cast::<pg_sys::FormData_pg_amop>();
    assert!((*oprform).amopstrategy == 1);
    assert!((*oprform).amoppurpose == pg_sys::AMOP_ORDER as i8);
    let opropr = (*oprform).amopopr;
    let distance = if opropr == regoperatorin("<->(vector,vector)") {
        Distance::L2
    } else if opropr == regoperatorin("<#>(vector,vector)") {
        Distance::Dot
    } else if opropr == regoperatorin("<=>(vector,vector)") {
        Distance::Cosine
    } else {
        panic!("Unsupported operator.")
    };
    pg_sys::ReleaseCatCacheList(oprlist);
    pg_sys::ReleaseSysCache(tup);
    distance
}

#[pg_extern(strict)]
unsafe fn vectors_load(oid: pg_sys::Oid) {
    let id = Id::from_sys(oid);
    client().load(id).unwrap();
}

#[pg_extern(strict)]
unsafe fn vectors_unload(oid: pg_sys::Oid) {
    let id = Id::from_sys(oid);
    client().unload(id).unwrap();
}
