use crate::datatype::memory_vecf32::{Vecf32Input, Vecf32Output};
use crate::error::*;
use base::scalar::*;
use base::vector::*;

// sql
// CREATE TYPE vector_accum_state AS (
// 	count BIGINT,
// 	sum double precision[]
// );

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_accum(
    mut state: pgrx::composite_type!('static, "vectors.vector_accum_state"),
    value: Vecf32Input<'_>,
) -> pgrx::composite_type!('static, "vectors.vector_accum_state") {
    let count = state
        .get_by_name::<i64>("count")
        .unwrap()
        .unwrap_or_default();
    if count == 0 {
        let mut result =
            pgrx::heap_tuple::PgHeapTuple::new_composite_type("vectors.vector_accum_state")
                .unwrap();
        let sum = value.iter().map(|x| x.0 as f64).collect::<Vec<_>>();
        result.set_by_name("count", count + 1).unwrap();
        result.set_by_name("sum", sum).unwrap();
        result
    } else {
        let sum = state
            .get_by_name::<pgrx::Array<f64>>("sum")
            .unwrap()
            .unwrap();
        check_matched_dims(sum.len(), value.dims());
        // TODO: pgrx::Array<T> don't support mutable operations currently, we can reuse the state once it's supported.
        let sum = sum
            .iter_deny_null()
            .zip(value.iter())
            .map(|(x, y)| x + y.0 as f64)
            .collect::<Vec<_>>();
        state.set_by_name("count", count + 1).unwrap();
        state.set_by_name("sum", sum).unwrap();
        state
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_combine(
    state1: pgrx::composite_type!('static, "vectors.vector_accum_state"),
    state2: pgrx::composite_type!('static, "vectors.vector_accum_state"),
) -> pgrx::composite_type!('static, "vectors.vector_accum_state") {
    let count1 = state1
        .get_by_name::<i64>("count")
        .unwrap()
        .unwrap_or_default();
    let count2 = state2
        .get_by_name::<i64>("count")
        .unwrap()
        .unwrap_or_default();
    if count1 == 0 {
        state2
    } else if count2 == 0 {
        state1
    } else {
        let sum1 = state1
            .get_by_name::<pgrx::Array<f64>>("sum")
            .unwrap()
            .unwrap();
        let sum2 = state2
            .get_by_name::<pgrx::Array<f64>>("sum")
            .unwrap()
            .unwrap();
        check_matched_dims(sum1.len(), sum2.len());
        let sum = sum1
            .iter_deny_null()
            .zip(sum2.iter_deny_null())
            .map(|(x, y)| x + y)
            .collect::<Vec<_>>();
        let mut result =
            pgrx::heap_tuple::PgHeapTuple::new_composite_type("vectors.vector_accum_state")
                .unwrap();
        result.set_by_name("count", count1 + count2).unwrap();
        result.set_by_name("sum", sum).unwrap();
        result
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_final(
    state: pgrx::composite_type!('static, "vectors.vector_accum_state"),
) -> Vecf32Output {
    let count = state
        .get_by_name::<i64>("count")
        .unwrap()
        .unwrap_or_default();
    if count == 0 {
        //TODO: it is possible to return NULL datum here?
        bad_literal("No input data.");
    }
    let sum = state
        .get_by_name::<pgrx::Array<f64>>("sum")
        .unwrap()
        .unwrap();
    let sum = sum
        .iter_deny_null()
        .map(|x| F32((x / count as f64) as f32))
        .collect::<Vec<_>>();
    Vecf32Output::new(Vecf32Borrowed::new_checked(&sum).unwrap())
}
