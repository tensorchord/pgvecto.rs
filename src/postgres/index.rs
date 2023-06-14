use crate::postgres::datatype::VectorInput;
use crate::postgres::datatype::VectorTypmod;
use crate::postgres::gucs::SEARCH_K;
use crate::postgres::hooks::client;
use crate::prelude::Distance;
use crate::prelude::Id;
use crate::prelude::Options;
use crate::prelude::Pointer;
use crate::prelude::Scalar;
use pg_sys::Datum;
use pgrx::pg_sys::AsPgCStr;
use pgrx::prelude::*;
use std::ffi::CStr;
use std::ptr::null_mut;

struct ScanState {
    data: Vec<Pointer>,
}

#[derive(Copy, Clone, Debug)]
#[repr(C)]
struct IndexOptions {
    #[allow(dead_code)]
    vl_len_: i32,

    algorithm_offset: i32,
    options_algorithm_offset: i32,
}

impl IndexOptions {
    unsafe fn get_str(this: *const Self, offset: i32, default: &str) -> &str {
        if offset == 0 {
            default
        } else {
            let ptr = (this as *const std::os::raw::c_char).offset(offset as isize);
            CStr::from_ptr(ptr).to_str().unwrap()
        }
    }
}

#[pgrx::pg_extern(sql = "
    CREATE OR REPLACE FUNCTION pgvectors_amhandler(internal) RETURNS index_am_handler
    PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
    CREATE ACCESS METHOD pgvectors TYPE INDEX HANDLER pgvectors_amhandler;
    COMMENT ON ACCESS METHOD pgvectors IS 'HNSW index access method';
", requires = ["vector"])]
fn pgvectors_amhandler(
    _fcinfo: pg_sys::FunctionCallInfo,
) -> pgrx::PgBox<pgrx::pg_sys::IndexAmRoutine> {
    let mut am_routine = unsafe {
        pgrx::PgBox::<pgrx::pg_sys::IndexAmRoutine>::alloc_node(
            pgrx::pg_sys::NodeTag_T_IndexAmRoutine,
        )
    };

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

static mut RELOPT_KIND: pg_sys::relopt_kind = 0;

pub unsafe fn init() {
    RELOPT_KIND = pg_sys::add_reloption_kind();
    pg_sys::add_string_reloption(
        RELOPT_KIND,
        "algorithm".as_pg_cstr(),
        "The algorithm.".as_pg_cstr(),
        "UNDEFINED".as_pg_cstr(),
        None,
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND,
        "options_algorithm".as_pg_cstr(),
        "Options for the algorithm.".as_pg_cstr(),
        "{}".as_pg_cstr(),
        None,
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );
}

#[pg_guard]
unsafe extern "C" fn amvalidate(opclass_oid: pg_sys::Oid) -> bool {
    validate_opclass(opclass_oid);
    true
}

#[pg_guard]
unsafe extern "C" fn amoptions(reloptions: pg_sys::Datum, validate: bool) -> *mut pg_sys::bytea {
    let tab: &[pg_sys::relopt_parse_elt] = &[
        pg_sys::relopt_parse_elt {
            optname: "algorithm".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: memoffset::offset_of!(IndexOptions, algorithm_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "options_algorithm".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: memoffset::offset_of!(IndexOptions, options_algorithm_offset) as i32,
        },
    ];
    let rdopts = pg_sys::build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND,
        std::mem::size_of::<IndexOptions>(),
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
    crate::postgres::hooks::hook_on_writing(id);
    let options = options(index_relation);
    let (tx, rx) = async_channel::bounded::<(Vec<Scalar>, Pointer)>(65536);
    let thread = std::thread::spawn({
        move || {
            client().build(id, options, rx).unwrap();
        }
    });
    struct BuildState {
        tx: async_channel::Sender<(Vec<Scalar>, Pointer)>,
    }
    let mut state = BuildState { tx };
    #[pgrx::pg_guard]
    unsafe extern "C" fn callback(
        _index_relation: pgrx::pg_sys::Relation,
        ctid: pgrx::pg_sys::ItemPointer,
        values: *mut pgrx::pg_sys::Datum,
        is_null: *mut bool,
        _tuple_is_alive: bool,
        state: *mut std::os::raw::c_void,
    ) {
        let pgvector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
        (&mut *(state as *mut BuildState))
            .tx
            .send_blocking((pgvector.to_vec(), Pointer::from_sys(*ctid)))
            .unwrap();
    }
    let index_info = pgrx::pg_sys::BuildIndexInfo(index_relation);
    let ntuples = (*(*heap_relation).rd_tableam)
        .index_build_range_scan
        .unwrap()(
        heap_relation,
        index_relation,
        index_info,
        true,
        false,
        true,
        0,
        pgrx::pg_sys::InvalidBlockNumber,
        Some(callback),
        (&mut state) as *mut _ as _,
        null_mut(),
    );
    drop(state);
    thread.join().unwrap();
    let mut result = PgBox::<pg_sys::IndexBuildResult>::alloc0();
    result.heap_tuples = ntuples;
    result.index_tuples = 0.0;
    result.into_pg()
}

#[pg_guard]
unsafe extern "C" fn ambuildempty(index_relation: pg_sys::Relation) {
    let oid = (*index_relation).rd_id;
    let id = Id::from_sys(oid);
    crate::postgres::hooks::hook_on_writing(id);
    let options = options(index_relation);
    let (_, rx) = async_channel::bounded::<(Vec<Scalar>, Pointer)>(1);
    client().build(id, options, rx).unwrap();
}

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
    let oid = (*index_relation).rd_id;
    let id = Id::from_sys(oid);
    crate::postgres::hooks::hook_on_writing(id);
    let pgvector = VectorInput::from_datum(*values.add(0), *is_null.add(0)).unwrap();
    let vector = pgvector.data();
    let p = Pointer::from_sys(*heap_tid);
    client().insert(id, (vector.to_vec(), p)).unwrap();
    true
}

#[pg_guard]
unsafe extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    n_keys: std::os::raw::c_int,
    n_order_bys: std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    let mut scan: PgBox<pg_sys::IndexScanDescData> = PgBox::from_pg(pg_sys::RelationGetIndexScan(
        index_relation,
        n_keys,
        n_order_bys,
    ));

    let state = Option::<ScanState>::None;

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
        use pgrx::pg_sys::{palloc, palloc0};
        let size_datum = std::mem::size_of::<Datum>();
        let size_bool = std::mem::size_of::<bool>();
        let orderbyvals = palloc0(size_datum * (*scan).numberOfOrderBys as usize) as *mut Datum;
        let orderbynulls = palloc(size_bool * (*scan).numberOfOrderBys as usize) as *mut bool;
        orderbynulls.write_bytes(1, (*scan).numberOfOrderBys as usize);
        (*scan).xs_orderbyvals = orderbyvals;
        (*scan).xs_orderbynulls = orderbynulls;
    }
    assert!(n_orderbys == 1, "Not supported.");
    let state = &mut *((*scan).opaque as *mut Option<ScanState>);
    let scan_vector = (*orderbys.add(0)).sk_argument;
    let dt_vector = VectorInput::from_datum(scan_vector, false).unwrap();
    let vector = dt_vector.data();
    *state = Some(ScanState {
        data: {
            let k = SEARCH_K.get() as _;
            let mut data = client().search(id, (vector.to_vec(), k)).unwrap();
            data.reverse();
            data
        },
    });
}

#[pg_guard]
unsafe extern "C" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection,
) -> bool {
    (*scan).xs_recheck = false;
    (*scan).xs_recheckorderby = false;
    let state = &mut *((*scan).opaque as *mut Option<ScanState>);
    if let Some(x) = state.as_mut().unwrap().data.pop() {
        (*scan).xs_heaptid = x.into_sys();
        true
    } else {
        false
    }
}

#[pg_guard]
extern "C" fn amendscan(_scan: pg_sys::IndexScanDesc) {}

#[pg_guard]
unsafe extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let oid = (*(*info).index).rd_id;
    let id = Id::from_sys(oid);
    crate::postgres::hooks::hook_on_writing(id);
    let items = callback_state as *mut pgrx::pg_sys::VacDeadItems;
    let deletes = (*items)
        .items
        .as_slice((*items).max_items as usize)
        .iter()
        .copied()
        .map(Pointer::from_sys)
        .collect::<Vec<Pointer>>();
    for message in deletes {
        client().delete(id, message).unwrap();
    }
    let result = pgrx::PgBox::<pgrx::pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}

#[pg_guard]
unsafe extern "C" fn amvacuumcleanup(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let result = pgrx::PgBox::<pgrx::pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
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

unsafe fn options(index_relation: pg_sys::Relation) -> Options {
    assert!(
        (*(*index_relation).rd_index).indnkeyatts == 1,
        "Only supports exactly one key column."
    );
    let opfamily_oid = (*index_relation).rd_opfamily.read();
    let distance = validate_opfamily(opfamily_oid);
    let typmod = VectorTypmod::parse_from_i32(
        (*(*(*index_relation).rd_att).attrs.as_ptr().add(0)).type_mod(),
    )
    .unwrap();
    let dims = typmod
        .dimensions()
        .expect("Column does not have dimensions.");
    let options = (*index_relation).rd_options as *mut IndexOptions;
    assert!(!options.is_null(), "Options must be set.");
    let algorithm = IndexOptions::get_str(options, (*options).algorithm_offset, "UNDEFINED");
    let options_algorithm =
        IndexOptions::get_str(options, (*options).options_algorithm_offset, "{}");
    Options {
        dims,
        distance,
        algorithm: algorithm.to_string(),
        options_algorithm: options_algorithm.to_string(),
    }
}

unsafe fn validate_opclass(opclass_oid: pg_sys::Oid) -> Distance {
    let classtup = pgrx::pg_sys::SearchSysCache1(
        pgrx::pg_sys::SysCacheIdentifier_CLAOID as _,
        opclass_oid.into(),
    );
    if classtup.is_null() {
        panic!("cache lookup failed for operator class {opclass_oid}");
    }
    let classform = pgrx::pg_sys::GETSTRUCT(classtup).cast::<pgrx::pg_sys::FormData_pg_opclass>();
    let opfamily_oid = (*classform).opcfamily;
    let distance = validate_opfamily(opfamily_oid);
    pgrx::pg_sys::ReleaseSysCache(classtup);
    distance
}

unsafe fn validate_opfamily(opfamily_oid: pg_sys::Oid) -> Distance {
    let familytup = pgrx::pg_sys::SearchSysCache1(
        pgrx::pg_sys::SysCacheIdentifier_OPFAMILYOID as _,
        opfamily_oid.into(),
    );
    if familytup.is_null() {
        panic!("cache lookup failed for operator family {opfamily_oid}");
    }
    let oprlist = pgrx::pg_sys::SearchSysCacheList(
        pgrx::pg_sys::SysCacheIdentifier_AMOPSTRATEGY as _,
        1,
        opfamily_oid.into(),
        0.into(),
        0.into(),
    );
    let oprlist_members = (*oprlist).members.as_slice((*oprlist).n_members as _);
    let mut found_1 = false;
    let mut distance = None;
    for member in oprlist_members.iter().copied() {
        let oprtup = &mut (*member).tuple;
        let oprform = pgrx::pg_sys::GETSTRUCT(oprtup).cast::<pgrx::pg_sys::FormData_pg_amop>();
        match (*oprform).amopstrategy {
            1 => {
                assert!(
                    (*oprform).amoppurpose == pgrx::pg_sys::AMOP_ORDER as i8,
                    "Only supports indexing for order-by."
                );
                if (*oprform).amopopr == regoperatorin("<->(vector,vector)") {
                    distance = Some(Distance::L2);
                } else if (*oprform).amopopr == regoperatorin("<#>(vector,vector)") {
                    distance = Some(Distance::Dot);
                } else if (*oprform).amopopr == regoperatorin("<=>(vector,vector)") {
                    distance = Some(Distance::Cosine);
                } else {
                    panic!("Unsupported operator.");
                }
                found_1 = true;
            }
            _ => panic!("Unsupported stragegy number."),
        }
    }
    assert!(found_1, "Stragegy 1 is not found.");
    pgrx::pg_sys::ReleaseCatCacheList(oprlist);
    pgrx::pg_sys::ReleaseSysCache(familytup);
    distance.unwrap()
}

#[pg_extern]
unsafe fn pgvectors_load(oid: pgrx::pg_sys::Oid) {
    let id = Id::from_sys(oid);
    client().load(id).unwrap();
}

#[pg_extern]
unsafe fn pgvectors_unload(oid: pgrx::pg_sys::Oid) {
    let id = Id::from_sys(oid);
    client().unload(id).unwrap();
}
