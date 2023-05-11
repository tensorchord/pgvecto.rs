use rand::prelude::*;
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
    centers_count: usize,
    samples: Vec<Vector>,
    // expected number for samples
    sample_num: usize,
    // actual number for samples
    sample_count: usize,
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
        let mut sample_num = std::cmp::max(10000, cluster * 50);
        if heap.is_null() {
            sample_num = 1;
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
            centers_count: 0,
            samples: Vec::with_capacity(sample_num),
            sample_num,
            sample_count: 0,
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

    // reservoir sampling
    let val = std::ptr::read(values.read().cast_mut_ptr::<Vector>());
    state.sample_count += 1;
    if state.samples.len() < state.sample_num {
        state.samples.push(val);
    } else {
        let mut rng = rand::thread_rng();
        let index = rng.gen_range(1..=state.sample_count);
        if index < state.sample_num {
            state.samples[index] = val;
        }
    }

    old_context.set_as_current();
    state.mem_context.reset();
}

// kmeans++ initialization
fn init_cluster_centers(state: &mut BuildState) {
    let mut rng = rand::thread_rng();
    let mut weights = vec![f64::MAX; state.samples.len()];

    state.centers[0] = state.samples[rng.gen_range(0..state.samples.len())].clone();
    state.centers_count += 1;
    for i in 0..state.centers.len() {
        check_for_interrupts!();

        let mut sum = 0f64;
        for j in 0..state.samples.len() {
            let dist = square_euclidean_distance_ref(&state.centers[i], &state.samples[j]);
            if dist < weights[j] {
                weights[j] = dist.powi(2);
            }
            sum += weights[j];
        }

        if i + 1 == state.centers.len() {
            break;
        }

        let mut choice = sum * rng.gen::<f64>();
        let mut index = 0;
        for j in 0..(state.samples.len()-1) {
            choice -= weights[j];
            index = j;
            if choice <= 0f64 {
                break;
            }
        }
        state.centers[i + 1] = state.samples[index].clone();
        state.centers_count += 1;
    }
}

fn kmeans_clustering(state: &mut BuildState) {
    init_cluster_centers(state);

    let mut cluster_elements = vec![Vec::<usize>::new(); state.cluster];
    let mut sample_cluster = vec![0usize; state.samples.len()];
    
    // assign each sample to the nearest cluster
    for i in 0..state.samples.len() {
        let mut min_dist = f64::MAX;
        let mut min_index = 0;
        for j in 0..state.centers.len() {
            let dist = square_euclidean_distance_ref(&state.centers[j], &state.samples[i]);
            if dist < min_dist {
                min_dist = dist;
                min_index = j;
            }
        }
        cluster_elements[min_index].push(i);
        sample_cluster[i] = min_index;
    }

    for _ in 0..500 {
        check_for_interrupts!();

        let mut changed = 0;

        // compute the centers
        for i in 0..state.centers.len() {
            let mut new_center = vec![0f32; state.dim];
            for j in 0..cluster_elements[i].len() {
                let index = cluster_elements[i][j];
                for k in 0..state.dim {
                    new_center[k] += state.samples[index][k];
                }
            }
            for k in 0..state.dim {
                new_center[k] /= cluster_elements[i].len() as f32;
            }
            if new_center != state.centers[i] {
                state.centers[i] = new_center;
            }
        }

        for i in 0..cluster_elements.len() {
            cluster_elements[i].clear();
        }

        // assign samples to the nearest cluster
        for i in 0..state.samples.len() {
            let mut min_dist = f64::MAX;
            let mut min_index = 0;
            for j in 0..state.centers.len() {
                let dist = square_euclidean_distance_ref(&state.centers[j], &state.samples[i]);
                if dist < min_dist {
                    min_dist = dist;
                    min_index = j;
                }
            }
            if min_index != sample_cluster[i] {
                changed += 1;
            }
            cluster_elements[min_index].push(i);
            sample_cluster[i] = min_index;
        }

        if changed == 0 {
            break;
        }
    }
}

fn build_index(
    index_info: *mut pg_sys::IndexInfo,
    heap: &PgRelation,
    index: &PgRelation,
    state: &mut BuildState,
) {
    // scan the heap to sample the vectors
    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap.as_ptr(),
            index.as_ptr(),
            index_info,
            Some(build_callback),
            state,
        );
    }
    kmeans_clustering(state);
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
