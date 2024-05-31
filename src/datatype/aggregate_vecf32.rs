#![allow(unused_lifetimes)]
#![allow(clippy::extra_unused_lifetimes)]

use crate::datatype::memory_vecf32::{Vecf32Input, Vecf32Output};
use crate::error::*;
use base::scalar::*;
use base::vector::*;
use pgrx::Internal;

use super::get_mut_internal;

#[repr(C, align(8))]
pub struct Vecf32AggregateAvgSumStype {
    dims: u16,
    count: u64,
    values: Vec<F32>,
}

impl Vecf32AggregateAvgSumStype {
    pub fn dims(&self) -> usize {
        self.dims as usize
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
        let dims = slice.len();
        let mut values = Vec::with_capacity(dims);
        values.extend_from_slice(slice);
        Self {
            dims: dims as u16,
            count,
            values,
        }
    }
}

/// accumulate intermediate state for vector average
#[base_macros::aggregate_func]
#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_sum_sfunc(
    mut current: Option<Internal>,
    value: Option<Vecf32Input<'_>>,
) -> Option<Internal> {
    if value.is_none() {
        match get_mut_internal::<Vecf32AggregateAvgSumStype>(&mut current) {
            Some(_) => {
                return current;
            }
            None => {
                return None;
            }
        }
    }
    let value = value.unwrap();
    match get_mut_internal::<Vecf32AggregateAvgSumStype>(&mut current) {
        // if the state is empty, copy the input vector
        None => Some(Internal::new(Vecf32AggregateAvgSumStype::new_with_slice(
            1,
            value.iter().as_slice(),
        ))),
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
    }
}

/// combine two intermediate states for vector average
#[base_macros::aggregate_func]
#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_sum_combinefunc(
    mut state1: Option<Internal>,
    mut state2: Option<Internal>,
) -> Option<Internal> {
    match (
        get_mut_internal::<Vecf32AggregateAvgSumStype>(&mut state1),
        get_mut_internal::<Vecf32AggregateAvgSumStype>(&mut state2),
    ) {
        (None, None) => None,
        (Some(_), None) => state1,
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
    }
}

/// finalize the intermediate state for vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_finalfunc(mut state: Option<Internal>) -> Option<Vecf32Output> {
    match get_mut_internal::<Vecf32AggregateAvgSumStype>(&mut state) {
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
fn _vectors_vecf32_aggregate_sum_finalfunc(mut state: Option<Internal>) -> Option<Vecf32Output> {
    get_mut_internal::<Vecf32AggregateAvgSumStype>(&mut state)
        .map(|state| Vecf32Output::new(Vecf32Borrowed::new_checked(state.slice()).unwrap()))
}
