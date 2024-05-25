#![allow(unused_lifetimes)]
#![allow(clippy::extra_unused_lifetimes)]

use crate::datatype::memory_vecf32::{Vecf32Input, Vecf32Output};
use crate::error::*;
use base::scalar::*;
use base::vector::*;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::ArgumentError;
use pgrx::pgrx_sql_entity_graph::metadata::Returns;
use pgrx::pgrx_sql_entity_graph::metadata::ReturnsError;
use pgrx::pgrx_sql_entity_graph::metadata::SqlMapping;
use pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
use pgrx::{FromDatum, IntoDatum};
use std::alloc::Layout;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

#[repr(C, align(8))]
pub struct Vecf32AggregateAvgSumStypeHeader {
    varlena: u32,
    dims: u16,
    count: u64,
    phantom: [F32; 0],
}

impl Vecf32AggregateAvgSumStypeHeader {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        let _ = u16::try_from(len).unwrap();
        let layout_alpha = Layout::new::<Vecf32AggregateAvgSumStypeHeader>();
        let layout_beta = Layout::array::<F32>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn dims(&self) -> usize {
        self.dims as usize
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    pub fn slice(&self) -> &[F32] {
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.dims as usize) }
    }
    pub fn slice_mut(&mut self) -> &mut [F32] {
        unsafe { std::slice::from_raw_parts_mut(self.phantom.as_mut_ptr(), self.dims as usize) }
    }
}

pub struct Vecf32AggregateAvgSumStype<'a> {
    pub(crate) header: &'a mut Vecf32AggregateAvgSumStypeHeader,
}

impl<'a> Vecf32AggregateAvgSumStype<'a> {
    unsafe fn new(p: NonNull<Vecf32AggregateAvgSumStypeHeader>) -> Self {
        unsafe {
            Vecf32AggregateAvgSumStype {
                header: &mut *p.as_ptr(),
            }
        }
    }

    pub fn new_with_slice(count: u64, slice: &[F32]) -> Self {
        let dims = slice.len();
        let layout = Vecf32AggregateAvgSumStypeHeader::layout(dims);
        unsafe {
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Vecf32AggregateAvgSumStypeHeader;
            std::ptr::addr_of_mut!((*ptr).varlena)
                .write(Vecf32AggregateAvgSumStypeHeader::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).dims).write(dims as u16);
            std::ptr::addr_of_mut!((*ptr).count).write(count);
            if dims > 0 {
                std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), dims);
            }
            Vecf32AggregateAvgSumStype { header: &mut *ptr }
        }
    }

    pub fn into_raw(self) -> *mut Vecf32AggregateAvgSumStypeHeader {
        (self.header) as *const Vecf32AggregateAvgSumStypeHeader
            as *mut Vecf32AggregateAvgSumStypeHeader
    }
}

impl Deref for Vecf32AggregateAvgSumStype<'_> {
    type Target = Vecf32AggregateAvgSumStypeHeader;

    fn deref(&self) -> &Self::Target {
        self.header
    }
}

impl DerefMut for Vecf32AggregateAvgSumStype<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.header
    }
}

impl Drop for Vecf32AggregateAvgSumStype<'_> {
    fn drop(&mut self) {}
}

impl FromDatum for Vecf32AggregateAvgSumStype<'_> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typmod: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr =
                NonNull::new(datum.cast_mut_ptr::<Vecf32AggregateAvgSumStypeHeader>()).unwrap();
            unsafe { Some(Vecf32AggregateAvgSumStype::new(ptr)) }
        }
    }
}

impl IntoDatum for Vecf32AggregateAvgSumStype<'_> {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::pg_sys::INTERNALOID
    }
}

unsafe impl SqlTranslatable for Vecf32AggregateAvgSumStype<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("internal")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("internal"))))
    }
}

/// accumulate intermediate state for vector average
#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_sum_sfunc<'a>(
    state: Option<Vecf32AggregateAvgSumStype<'a>>,
    value: Option<Vecf32Input<'_>>,
) -> Option<Vecf32AggregateAvgSumStype<'a>> {
    if value.is_none() {
        return state;
    }
    let value = value.unwrap();
    match state {
        // if the state is empty, copy the input vector
        None => Some(Vecf32AggregateAvgSumStype::new_with_slice(
            1,
            value.iter().as_slice(),
        )),
        Some(mut state) => {
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
            Some(state)
        }
    }
}

/// combine two intermediate states for vector average
#[pgrx::pg_extern(immutable, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_sum_combinefunc<'a>(
    state1: Option<Vecf32AggregateAvgSumStype<'a>>,
    state2: Option<Vecf32AggregateAvgSumStype<'a>>,
) -> Option<Vecf32AggregateAvgSumStype<'a>> {
    match (state1, state2) {
        (None, None) => None,
        (Some(state), None) => Some(state),
        (None, Some(state)) => Some(state),
        (Some(mut state1), Some(state2)) => {
            let dims1 = state1.dims();
            let dims2 = state2.dims();
            check_matched_dims(dims1, dims2);
            state1.count += state2.count();
            let sum1 = state1.slice_mut();
            let sum2 = state2.slice();
            for (x, y) in sum1.iter_mut().zip(sum2.iter()) {
                *x += *y;
            }
            Some(state1)
        }
    }
}

/// finalize the intermediate state for vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_finalfunc(
    state: Vecf32AggregateAvgSumStype<'_>,
) -> Option<Vecf32Output> {
    let sum = state
        .slice()
        .iter()
        .map(|x| *x / F32(state.count as f32))
        .collect::<Vec<_>>();
    Some(Vecf32Output::new(
        Vecf32Borrowed::new_checked(&sum).unwrap(),
    ))
}

/// finalize the intermediate state for vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_sum_finalfunc(
    state: Vecf32AggregateAvgSumStype<'_>,
) -> Option<Vecf32Output> {
    let sum = state.slice();
    Some(Vecf32Output::new(Vecf32Borrowed::new_checked(sum).unwrap()))
}
