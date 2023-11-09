use crate::postgres::{gucs::ENABLE_BITMAP_PUSHDOWN, index_scan::Scanner};
use cstr::cstr;
use pgrx::list::List;
use std::ffi::CStr;

static mut PREV_SET_REL_PATHLIST_HOOK: pgrx::pg_sys::set_rel_pathlist_hook_type = None;

const CUSTOM_SCAN_NAME: &CStr = cstr!("Fusion Vector Index Scan");

const CUSTOM_PATH_METHODS: pgrx::pg_sys::CustomPathMethods = pgrx::pg_sys::CustomPathMethods {
    CustomName: CUSTOM_SCAN_NAME.as_ptr().cast(),
    PlanCustomPath: Some(plan_fusion_path_hook),
    ReparameterizeCustomPathByChild: None,
};

const CUSTOM_SCAN_METHODS: pgrx::pg_sys::CustomScanMethods = pgrx::pg_sys::CustomScanMethods {
    CustomName: CUSTOM_SCAN_NAME.as_ptr().cast(),
    CreateCustomScanState: Some(create_fusion_scan_state_hook),
};

const CUSTOM_SCAN_EXEC_METHODS: pgrx::pg_sys::CustomExecMethods = pgrx::pg_sys::CustomExecMethods {
    CustomName: CUSTOM_SCAN_NAME.as_ptr().cast(),
    BeginCustomScan: Some(begin_fusion_scan_hook),
    ExecCustomScan: Some(exec_fusion_scan_hook),
    EndCustomScan: Some(end_fusion_scan_hook),
    ReScanCustomScan: Some(rescan_fusion_scan_hook),
    MarkPosCustomScan: None,
    RestrPosCustomScan: None,
    EstimateDSMCustomScan: None,
    InitializeDSMCustomScan: None,
    InitializeWorkerCustomScan: None,
    ReInitializeDSMCustomScan: None,
    ShutdownCustomScan: None,
    ExplainCustomScan: Some(explain_fusion_scan_hook),
};

struct FusionScanState {
    css: pgrx::pg_sys::CustomScanState,
    vector_index_scan: *mut pgrx::pg_sys::IndexScanState,
    bitmap_index_scan: *mut pgrx::pg_sys::BitmapIndexScanState,
    tbm: *mut pgrx::pg_sys::TIDBitmap,
}

pub unsafe fn init() {
    PREV_SET_REL_PATHLIST_HOOK = pgrx::pg_sys::set_rel_pathlist_hook;
    pgrx::pg_sys::set_rel_pathlist_hook = Some(set_fusion_path_hook);
}

unsafe extern "C" fn set_fusion_path_hook(
    root: *mut pgrx::pg_sys::PlannerInfo,
    rel: *mut pgrx::pg_sys::RelOptInfo,
    rti: pgrx::pg_sys::Index, // index num of the relation in base_rel_array
    rte: *mut pgrx::pg_sys::RangeTblEntry,
) {
    if let Some(prev_hook) = PREV_SET_REL_PATHLIST_HOOK {
        prev_hook(root, rel, rti, rte);
    }

    if !ENABLE_BITMAP_PUSHDOWN.get() {
        return;
    }

    if (*rte).rtekind != pgrx::pg_sys::RTEKind_RTE_RELATION
        || (*rte).relkind as u8 != pgrx::pg_sys::RELKIND_RELATION
    {
        return;
    }

    // find vector index scan and bitmap index scan
    let mut vector_index_scan = None;
    let mut bitmap_index_scan = None;
    let path_lists = List::<*mut libc::c_void>::downcast_ptr((*rel).pathlist).unwrap();
    for ptr in path_lists.iter() {
        let path = &*(ptr.cast::<pgrx::pg_sys::Path>());
        if let Some(ptr) = find_vector_index_scan(path) {
            match vector_index_scan {
                Some(_) => {
                    pgrx::warning!(
                        "set_fusion_path_hook: Found more than one vector index scan, skip"
                    );
                    return;
                }
                None => {
                    let new_ptr =
                        pgrx::pg_sys::palloc(std::mem::size_of::<pgrx::pg_sys::IndexPath>()).cast();
                    std::ptr::copy_nonoverlapping(
                        ptr as *mut u8,
                        new_ptr as *mut u8,
                        std::mem::size_of::<pgrx::pg_sys::IndexPath>(),
                    );
                    vector_index_scan = Some(new_ptr);
                }
            }
            continue;
        }
        if let Some(ptr) = find_bitmap_index_scan(path) {
            match bitmap_index_scan {
                Some(_) => {
                    pgrx::warning!(
                        "set_fusion_path_hook: Found more than one bitmap index scan, skip"
                    );
                    return;
                }
                None => {
                    let new_ptr =
                        pgrx::pg_sys::palloc(std::mem::size_of::<pgrx::pg_sys::IndexPath>()).cast();
                    std::ptr::copy_nonoverlapping(
                        ptr as *mut u8,
                        new_ptr as *mut u8,
                        std::mem::size_of::<pgrx::pg_sys::IndexPath>(),
                    );
                    bitmap_index_scan = Some(new_ptr);
                }
            }
        }
    }

    if vector_index_scan.is_none() || bitmap_index_scan.is_none() {
        if let Some(ptr) = vector_index_scan {
            pgrx::pg_sys::pfree(ptr);
        }
        if let Some(ptr) = bitmap_index_scan {
            pgrx::pg_sys::pfree(ptr);
        }
        return;
    }
    let vector_index_scan = vector_index_scan.unwrap().cast();
    let bitmap_index_scan = bitmap_index_scan.unwrap().cast();

    if !check_same_clause(vector_index_scan, bitmap_index_scan) {
        pgrx::pg_sys::pfree(vector_index_scan.cast());
        pgrx::pg_sys::pfree(bitmap_index_scan.cast());
        return;
    }

    pgrx::debug1!("set_fusion_path_hook: found vector index scan and bitmap index scan");

    let cpath_ptr = pgrx::pg_sys::palloc0(std::mem::size_of::<pgrx::pg_sys::CustomPath>());
    let cpath = &mut *cpath_ptr.cast::<pgrx::pg_sys::CustomPath>();
    cpath.path.type_ = pgrx::pg_sys::NodeTag::T_CustomPath;
    cpath.path.pathtype = pgrx::pg_sys::NodeTag::T_CustomScan;
    cpath.path.pathkeys = (*vector_index_scan).pathkeys;
    cpath.path.parent = rel;
    cpath.path.pathtarget = (*rel).reltarget;
    cpath.path.param_info =
        pgrx::pg_sys::get_baserel_parampathinfo(root, rel, (*rel).lateral_relids);
    cpath.methods = &CUSTOM_PATH_METHODS;

    let mut custom_paths_list = pgrx::list::old_list::PgList::new();
    custom_paths_list.push(bitmap_index_scan);
    custom_paths_list.push(vector_index_scan);
    cpath.custom_paths = custom_paths_list.into_pg();

    // TODO: set cost
    pgrx::pg_sys::add_path(rel, cpath_ptr.cast());
    pgrx::debug1!("set_fusion_path_hook: added custom path");
}

unsafe fn find_vector_index_scan(path: &pgrx::pg_sys::Path) -> Option<*mut pgrx::pg_sys::Path> {
    if path.type_ == pgrx::pg_sys::NodeTag::T_IndexPath {
        let index_scan_path: &pgrx::pg_sys::IndexPath = &*(path as *const _ as *const _);
        if (*index_scan_path.indexinfo)
            .amcostestimate
            .is_some_and(|func| func as usize == super::index::amcostestimate as usize)
        {
            return Some(path as *const _ as *mut _);
        }
    }

    // todo: find in children recursively
    None
}

unsafe fn find_bitmap_index_scan(path: &pgrx::pg_sys::Path) -> Option<*mut pgrx::pg_sys::Path> {
    // todo: deal with BitmapAndPath
    if path.type_ == pgrx::pg_sys::NodeTag::T_BitmapHeapPath {
        let bitmap_scan_path: &pgrx::pg_sys::BitmapHeapPath = &*(path as *const _ as *const _);
        let index_path = &*bitmap_scan_path.bitmapqual;
        if index_path.type_ == pgrx::pg_sys::NodeTag::T_IndexPath {
            let index_scan_path: &pgrx::pg_sys::IndexPath = &*(index_path as *const _ as *const _);
            if (*index_scan_path.indexinfo).amhasgetbitmap {
                return Some(index_path as *const _ as *mut _);
            }
        }
    }

    // todo: find in children recursively
    None
}

unsafe fn check_same_clause(
    vector_index_scan: *mut pgrx::pg_sys::Path,
    bitmap_index_scan: *mut pgrx::pg_sys::Path,
) -> bool {
    let vector_index_path: &pgrx::pg_sys::IndexPath = &*vector_index_scan.cast();
    let bitmap_index_path: &pgrx::pg_sys::IndexPath = &*bitmap_index_scan.cast();
    let vector_index_clause = (*vector_index_path.indexinfo).indrestrictinfo;
    let bitmap_index_clause = bitmap_index_path.indexclauses;
    let vlist = List::<*mut libc::c_void>::downcast_ptr(vector_index_clause).unwrap();
    let blist = List::<*mut libc::c_void>::downcast_ptr(bitmap_index_clause).unwrap();
    if vlist.len() != blist.len() {
        return false;
    }
    for (v, b) in vlist.iter().zip(blist.iter()) {
        let v_ptr = *v as usize;
        let b_ptr = (*b.cast::<pgrx::pg_sys::IndexClause>()).rinfo as usize;
        if v_ptr != b_ptr {
            return false;
        }
    }
    true
}

#[allow(unused_variables)]
unsafe extern "C" fn plan_fusion_path_hook(
    root: *mut pgrx::pg_sys::PlannerInfo,
    rel: *mut pgrx::pg_sys::RelOptInfo,
    path: *mut pgrx::pg_sys::CustomPath,
    tlist: *mut pgrx::pg_sys::List,
    clauses: *mut pgrx::pg_sys::List,
    custom_plans: *mut pgrx::pg_sys::List,
) -> *mut pgrx::pg_sys::Plan {
    let cscan_ptr = pgrx::pg_sys::palloc0(std::mem::size_of::<pgrx::pg_sys::CustomScan>());
    let cscan = &mut *cscan_ptr.cast::<pgrx::pg_sys::CustomScan>();
    cscan.scan.plan.type_ = pgrx::pg_sys::NodeTag::T_CustomScan;
    cscan.custom_plans = custom_plans;
    cscan.methods = &CUSTOM_SCAN_METHODS;

    let custom_plan_lists = List::<*mut libc::c_void>::downcast_ptr(custom_plans).unwrap();
    cscan.scan.plan.lefttree = custom_plan_lists.get(0).unwrap().cast();
    pgrx::debug1!("tlist: {}", &*tlist);
    (*cscan.scan.plan.lefttree).targetlist = tlist;

    return cscan_ptr.cast();
}

unsafe extern "C" fn create_fusion_scan_state_hook(
    plan: *mut pgrx::pg_sys::CustomScan,
) -> *mut pgrx::pg_sys::Node {
    let cscan = &mut *plan.cast::<pgrx::pg_sys::CustomScan>();
    let cstate_ptr = pgrx::pg_sys::palloc0(std::mem::size_of::<FusionScanState>());
    let cstate = &mut *cstate_ptr.cast::<pgrx::pg_sys::CustomScanState>();
    cstate.custom_ps = cscan.custom_plans;
    cstate.methods = &CUSTOM_SCAN_EXEC_METHODS;

    return cstate_ptr.cast();
}

unsafe extern "C" fn begin_fusion_scan_hook(
    node: *mut pgrx::pg_sys::CustomScanState,
    executor: *mut pgrx::pg_sys::EState,
    eflags: i32,
) {
    pgrx::debug1!("begin_fusion_scan_hook");
    let cstate = &mut *node.cast::<FusionScanState>();
    let custom_ps = cstate.css.custom_ps;
    let custom_plan = List::<*mut libc::c_void>::downcast_ptr(custom_ps).unwrap();
    let vector_index_scan = custom_plan.get(0).unwrap().cast::<pgrx::pg_sys::Plan>();
    let bitmap_index_scan = custom_plan.get(1).unwrap().cast::<pgrx::pg_sys::Plan>();
    cstate.vector_index_scan =
        pgrx::pg_sys::ExecInitNode(vector_index_scan, executor, eflags).cast();
    cstate.bitmap_index_scan =
        pgrx::pg_sys::ExecInitNode(bitmap_index_scan, executor, eflags).cast();
    pgrx::debug1!("successfully init fusion scan");
}

unsafe extern "C" fn rescan_fusion_scan_hook(node: *mut pgrx::pg_sys::CustomScanState) {
    pgrx::debug1!("begin to rescan fusion scan");
    let cstate = &mut *node.cast::<FusionScanState>();
    pgrx::pg_sys::ExecReScan(cstate.vector_index_scan.cast());
    pgrx::pg_sys::ExecReScan(cstate.bitmap_index_scan.cast());

    cstate.tbm = pgrx::pg_sys::MultiExecProcNode(cstate.bitmap_index_scan.cast()).cast();
    let vector_index_scan = &mut *cstate
        .vector_index_scan
        .cast::<pgrx::pg_sys::IndexScanState>();
    let scanner: &mut Scanner = &mut *(*vector_index_scan.iss_ScanDesc).opaque.cast();
    let Scanner::Initial { bitmap, .. } = scanner else {
        unreachable!()
    };
    *bitmap = Some(cstate.tbm);
    pgrx::debug1!("success to rescan fusion scan");
}

unsafe extern "C" fn exec_fusion_scan_hook(
    node: *mut pgrx::pg_sys::CustomScanState,
) -> *mut pgrx::pg_sys::TupleTableSlot {
    pgrx::debug1!("begin to exec fusion scan");
    let cstate = &mut *node.cast::<FusionScanState>();
    let vector_index_scan = cstate.vector_index_scan;
    let res = (*vector_index_scan).ss.ps.ExecProcNode.unwrap()(vector_index_scan.cast());
    pgrx::debug1!("success to exec fusion scan");
    res
}

unsafe extern "C" fn end_fusion_scan_hook(node: *mut pgrx::pg_sys::CustomScanState) {
    pgrx::debug1!("begin to end fusion scan");
    let cstate = &mut *node.cast::<FusionScanState>();
    pgrx::pg_sys::ExecEndNode(cstate.vector_index_scan.cast());
    pgrx::pg_sys::ExecEndNode(cstate.bitmap_index_scan.cast());
    pgrx::pg_sys::tbm_free(cstate.tbm);
    pgrx::debug1!("success to end fusion scan");
}

#[allow(unused_variables)]
unsafe extern "C" fn explain_fusion_scan_hook(
    node: *mut pgrx::pg_sys::CustomScanState,
    ancestors: *mut pgrx::pg_sys::List,
    es: *mut pgrx::pg_sys::ExplainState,
) {
    // todo
}
