use crate::datatype::memory_vecf32::{Vecf32Input, Vecf32Output};
use crate::error::*;
use base::scalar::*;
use base::vector::*;
use pgrx::datum::Internal;

#[repr(C, align(8))]
pub struct Vecf32AggregateAvgSumStype {
    dims: u32,
    count: u64,
    values: Vec<F32>,
}

impl Vecf32AggregateAvgSumStype {
    pub fn dims(&self) -> u32 {
        self.dims
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    pub fn slice(&self) -> &[F32] {
        self.values.as_slice()
    }
    pub fn slice_mut(&mut self) -> &mut [F32] {
        self.values.as_mut_slice()
    }
}

impl Vecf32AggregateAvgSumStype {
    pub fn new_with_slice(count: u64, slice: &[F32]) -> Self {
        let dims = slice.len() as u32;
        let mut values = Vec::with_capacity(dims as _);
        values.extend_from_slice(slice);
        Self {
            dims,
            count,
            values,
        }
    }
}

/// accumulate intermediate state for vector average
#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_vecf32_aggregate_avg_sum_sfunc(internal, vector) RETURNS internal IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_vecf32_aggregate_avg_sum_sfunc(
    current: Internal,
    value: Option<Vecf32Input<'_>>,
    fcinfo: pgrx::pg_sys::FunctionCallInfo,
) -> Internal {
    let Some(value) = value else { return current };
    let old_context = unsafe {
        let mut agg_context: *mut ::pgrx::pg_sys::MemoryContextData = std::ptr::null_mut();
        if ::pgrx::pg_sys::AggCheckCallContext(fcinfo, &mut agg_context) == 0 {
            ::pgrx::error!("aggregate function called in non-aggregate context");
        }
        ::pgrx::pg_sys::MemoryContextSwitchTo(agg_context)
    };
    let result = match unsafe { current.get_mut::<Vecf32AggregateAvgSumStype>() } {
        // if the state is empty, copy the input vector
        None => Internal::new(Vecf32AggregateAvgSumStype::new_with_slice(
            1,
            value.iter().as_slice(),
        )),
        Some(state) => {
            let dims = state.dims();
            let value_dims = value.dims();
            check_matched_dims(dims, value_dims);
            let sum = state.slice_mut();
            // accumulate the input vector
            for (x, y) in sum.iter_mut().zip(value.iter()) {
                *x += *y;
            }
            // increase the count
            state.count += 1;
            current
        }
    };
    unsafe {
        ::pgrx::pg_sys::MemoryContextSwitchTo(old_context);
    }
    result
}

/// combine two intermediate states for vector average
#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_vecf32_aggregate_avg_sum_combinefunc(internal, internal) RETURNS internal IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_vecf32_aggregate_avg_sum_combinefunc(
    state1: Internal,
    state2: Internal,
    fcinfo: pgrx::pg_sys::FunctionCallInfo,
) -> Internal {
    let old_context = unsafe {
        let mut agg_context: *mut ::pgrx::pg_sys::MemoryContextData = std::ptr::null_mut();
        if ::pgrx::pg_sys::AggCheckCallContext(fcinfo, &mut agg_context) == 0 {
            ::pgrx::error!("aggregate function called in non-aggregate context");
        }
        ::pgrx::pg_sys::MemoryContextSwitchTo(agg_context)
    };
    let result = match (
        unsafe { state1.get_mut::<Vecf32AggregateAvgSumStype>() },
        unsafe { state2.get_mut::<Vecf32AggregateAvgSumStype>() },
    ) {
        (_, None) => state1,
        (None, Some(_)) => state2,
        (Some(s1), Some(s2)) => {
            let dims1 = s1.dims();
            let dims2 = s2.dims();
            check_matched_dims(dims1, dims2);
            s1.count += s2.count();
            let sum1 = s1.slice_mut();
            let sum2 = s2.slice();
            for (x, y) in sum1.iter_mut().zip(sum2.iter()) {
                *x += *y;
            }
            state1
        }
    };
    unsafe {
        ::pgrx::pg_sys::MemoryContextSwitchTo(old_context);
    }
    result
}

/// finalize the intermediate state for vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_finalfunc(state: Internal) -> Option<Vecf32Output> {
    match unsafe { state.get_mut::<Vecf32AggregateAvgSumStype>() } {
        Some(state) => {
            let count = state.count;
            state
                .slice_mut()
                .iter_mut()
                .for_each(|x| *x /= F32(count as f32));
            Some(Vecf32Output::new(
                Vecf32Borrowed::new_checked(state.slice()).unwrap(),
            ))
        }
        None => None,
    }
}

/// finalize the intermediate state for vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_sum_finalfunc(state: Internal) -> Option<Vecf32Output> {
    unsafe { state.get_mut::<Vecf32AggregateAvgSumStype>() }
        .map(|state| Vecf32Output::new(Vecf32Borrowed::new_checked(state.slice()).unwrap()))
}
