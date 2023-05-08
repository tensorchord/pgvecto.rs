use pgrx::{prelude::*, PgMemoryContexts, PgRelation, PgTupleDesc};

use crate::index::manager::Vector;
use crate::index::options::{VectorsOptions, DEFAULT_CLUSTER_SIZE};

struct BuildState<'a> {
    // tuple_desc: &'a PgTupleDesc<'a>,
    mem_context: PgMemoryContexts,

    heap: &'a PgRelation,
    index: &'a PgRelation,

    cluster: usize,
    dim: usize,

    heap_tuples: f64,
    index_tuples: f64,

    centers: Vec<Vector>,
    item_num: Vec<usize>,
    collation: pg_sys::Oid,
}

impl<'a> BuildState<'a> {
    fn new(index: &'a PgRelation, heap: &'a PgRelation) -> Self {
        let cluster = match index.rd_options.is_null() {
            true => DEFAULT_CLUSTER_SIZE,
            false => {
                let opts = unsafe { PgBox::from_pg(index.rd_options as *mut VectorsOptions) };
                opts.cluster
            }
        };
        let type_mod = index
            .tuple_desc()
            .get(0)
            .expect("no attribute #0 on the tuple desc")
            .type_mod();
        if type_mod < 0 {
            error!("column doesn't have dimensions: {}", type_mod)
        }

        BuildState {
            mem_context: PgMemoryContexts::new("vectors build context"),
            heap,
            index,
            cluster,
            dim: type_mod as usize,
            heap_tuples: 0f64,
            index_tuples: 0f64,
            centers: Vec::with_capacity(cluster),
            item_num: vec![0; cluster],
            collation: unsafe { *index.rd_indcollation },
        }
    }
}

#[pg_guard]
pub(crate) extern "C" fn am_build(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let heap = unsafe { PgRelation::from_pg(heap_relation) };
    let index = unsafe { PgRelation::from_pg(index_relation) };
    // let tuple_desc = get_index_tuple_desc(&index);
    let mut state = BuildState::new(&index, &heap);

    build_index(index_info, &heap, &index, &mut state);

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = state.heap_tuples;
    result.index_tuples = state.index_tuples;

    result.into_pg()
}

fn get_index_tuple_desc(index: &PgRelation) -> PgTupleDesc<'static> {
    let desc = index.tuple_desc();
    let type_oid = desc
        .get(0)
        .expect("no attribute #0 on tuple desc")
        .type_oid()
        .value();
    let type_mod = desc
        .get(0)
        .expect("no attribute #0 on tuple desc")
        .type_mod();

    unsafe {
        PgMemoryContexts::TopTransactionContext.switch_to(|_| {
            PgTupleDesc::from_pg_is_copy(pg_sys::lookup_rowtype_tupdesc_copy(type_oid, type_mod))
        })
    }
}

#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
#[pg_guard]
unsafe extern "C" fn build_callback(
    _index: pg_sys::Relation,
    _ctid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    is_null: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    if *is_null {
        return;
    }
    build_callback_internal(values, state)
}

#[inline(always)]
fn square_euclidean_distance_ref(left: &Vector, right: &Vector) -> f64 {
    left.iter()
        .zip(right.iter())
        .map(|(x, y)| (x - y).powi(2) as f64)
        .sum()
}

#[inline(always)]
unsafe extern "C" fn build_callback_internal(
    values: *mut pg_sys::Datum,
    state: *mut std::os::raw::c_void,
) {
    check_for_interrupts!();

    let state = (state as *mut BuildState).as_mut().unwrap();

    let mut old_context = state.mem_context.set_as_current();

    // TODO
    let val = std::ptr::read(values.read().cast_mut_ptr::<Vector>());
    if state.centers.len() < state.cluster {
        state.centers.push(val);
        state.item_num[state.centers.len() - 1] += 1;
    } else {
        let distances: Vec<f64> = state
            .centers
            .iter()
            .map(|vec| square_euclidean_distance_ref(vec, &val))
            .collect();
        let min_index = distances
            .iter()
            .enumerate()
            .max_by(|(_, x), (_, y)| x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(index, _)| index);
        match min_index {
            Some(index) => {
                let num = state.item_num[index];
                state.centers[index] = state.centers[index]
                    .iter()
                    .zip(&val)
                    .map(|(x, y)| x * (num as f32 / (num + 1) as f32) + y / (num + 1) as f32)
                    .collect();
                state.item_num[index] += 1;
            }
            None => {
                error!("cannot find the min distance due to NaN");
            }
        }
    }

    old_context.set_as_current();
    state.mem_context.reset();
}

fn build_index(
    index_info: *mut pg_sys::IndexInfo,
    heap: &PgRelation,
    index: &PgRelation,
    state: &mut BuildState,
) {
    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap.as_ptr(),
            index.as_ptr(),
            index_info,
            Some(build_callback),
            state,
        );
    }

    // TODO
}

#[pg_guard]
pub(crate) extern "C" fn am_build_empty(_index_relation: pg_sys::Relation) {}

#[cfg(any(feature = "pg10", feature = "pg11", feature = "pg12", feature = "pg13"))]
#[pg_guard]
pub(crate) extern "C" fn am_insert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    _is_null: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    am_insert_internal(index_relation, values, heap_tid)
}

#[cfg(any(feature = "pg14", feature = "pg15"))]
#[pg_guard]
pub(crate) extern "C" fn am_insert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    is_null: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    heap_relation: pg_sys::Relation,
    check_unique: pg_sys::IndexUniqueCheck,
    index_unchanged: bool,
    index_info: *mut pg_sys::IndexInfo,
) -> bool {
    am_insert_internal(index_relation, values, heap_tid)
}

#[inline(always)]
fn am_insert_internal(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    heap_tid: pg_sys::ItemPointer,
) -> bool {
    unimplemented!()
}
