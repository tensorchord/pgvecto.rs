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
use std::ffi::{CStr, CString};
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

pub enum Vecf32AggregateAvgSumStype<'a> {
    Owned(NonNull<Vecf32AggregateAvgSumStypeHeader>),
    Borrowed(&'a mut Vecf32AggregateAvgSumStypeHeader),
}

impl<'a> Vecf32AggregateAvgSumStype<'a> {
    unsafe fn new(p: NonNull<Vecf32AggregateAvgSumStypeHeader>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.as_ptr().cast()).cast()).unwrap()
        };
        if p != q {
            Vecf32AggregateAvgSumStype::Owned(q)
        } else {
            unsafe { Vecf32AggregateAvgSumStype::Borrowed(&mut *p.as_ptr()) }
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
            Vecf32AggregateAvgSumStype::Owned(NonNull::new(ptr).unwrap())
        }
    }

    pub fn into_raw(self) -> *mut Vecf32AggregateAvgSumStypeHeader {
        let result = match self {
            Vecf32AggregateAvgSumStype::Owned(p) => p.as_ptr(),
            Vecf32AggregateAvgSumStype::Borrowed(ref p) => {
                *p as *const Vecf32AggregateAvgSumStypeHeader
                    as *mut Vecf32AggregateAvgSumStypeHeader
            }
        };
        std::mem::forget(self);
        result
    }
}

impl Deref for Vecf32AggregateAvgSumStype<'_> {
    type Target = Vecf32AggregateAvgSumStypeHeader;

    fn deref(&self) -> &Self::Target {
        match self {
            Vecf32AggregateAvgSumStype::Owned(p) => unsafe { p.as_ref() },
            Vecf32AggregateAvgSumStype::Borrowed(p) => p,
        }
    }
}

impl DerefMut for Vecf32AggregateAvgSumStype<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Vecf32AggregateAvgSumStype::Owned(p) => unsafe { p.as_mut() },
            Vecf32AggregateAvgSumStype::Borrowed(p) => p,
        }
    }
}

impl Drop for Vecf32AggregateAvgSumStype<'_> {
    fn drop(&mut self) {
        match self {
            Vecf32AggregateAvgSumStype::Owned(p) => unsafe {
                pgrx::pg_sys::pfree(p.as_ptr().cast());
            },
            Vecf32AggregateAvgSumStype::Borrowed(_) => {}
        }
    }
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
        let namespace = pgrx::pg_catalog::PgNamespace::search_namespacename(c"vectors").unwrap();
        let namespace = namespace.get().expect("pgvecto.rs is not installed.");
        let t = pgrx::pg_catalog::PgType::search_typenamensp(
            c"_vectors_vecf32_aggregate_avg_sum_stype",
            namespace.oid(),
        )
        .unwrap();
        let t = t.get().expect("pg_catalog is broken.");
        t.oid()
    }
}

unsafe impl SqlTranslatable for Vecf32AggregateAvgSumStype<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from(
            "_vectors_vecf32_aggregate_avg_sum_stype",
        )))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from(
            "_vectors_vecf32_aggregate_avg_sum_stype",
        ))))
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_sum_stype_in(
    input: &CStr,
    _oid: Oid,
    _typmod: i32,
) -> Vecf32AggregateAvgSumStype<'_> {
    fn parse(input: &[u8]) -> Result<(u64, Vec<F32>), String> {
        use crate::utils::parse::parse_vector;
        let hint = "Invalid input format for _vecf32_aggregate_avg_stype, using \'bigint, array \' like \'1, [1]\'";
        let (count, slice) = input.split_once(|&c| c == b',').ok_or(hint)?;
        let count = std::str::from_utf8(count)
            .map_err(|e| e.to_string() + "\n" + hint)?
            .parse::<u64>()
            .map_err(|e| e.to_string() + "\n" + hint)?;
        let v = parse_vector(slice, 0, |s| s.parse().ok());
        match v {
            Err(e) => Err(e.to_string() + "\n" + hint),
            Ok(vector) => Ok((count, vector)),
        }
    }
    // parse one bigint and a vector of f32, split with a comma
    let res = parse(input.to_bytes());
    match res {
        Err(e) => {
            bad_literal(&e.to_string());
        }
        Ok((count, vector)) => Vecf32AggregateAvgSumStype::new_with_slice(count, &vector),
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_sum_stype_out(state: Vecf32AggregateAvgSumStype<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push_str(format!("{}, ", state.count()).as_str());
    buffer.push('[');
    if let Some(&x) = state.slice().first() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for &x in state.slice().iter().skip(1) {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}

/// accumulate intermediate state for vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_sum_sfunc<'a>(
    mut state: Vecf32AggregateAvgSumStype<'a>,
    value: Vecf32Input<'_>,
) -> Vecf32AggregateAvgSumStype<'a> {
    let count = state.count();
    match count {
        // if the state is empty, copy the input vector
        0 => Vecf32AggregateAvgSumStype::new_with_slice(1, value.iter().as_slice()),
        _ => {
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
            state
        }
    }
}

/// combine two intermediate states for vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_sum_combinefunc<'a>(
    mut state1: Vecf32AggregateAvgSumStype<'a>,
    state2: Vecf32AggregateAvgSumStype<'a>,
) -> Vecf32AggregateAvgSumStype<'a> {
    let count1 = state1.count();
    let count2 = state2.count();
    if count1 == 0 {
        state2
    } else if count2 == 0 {
        state1
    } else {
        let dims1 = state1.dims();
        let dims2 = state2.dims();
        check_matched_dims(dims1, dims2);
        state1.count += count2;
        let sum1 = state1.slice_mut();
        let sum2 = state2.slice();
        for (x, y) in sum1.iter_mut().zip(sum2.iter()) {
            *x += *y;
        }
        state1
    }
}

/// finalize the intermediate state for vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_finalfunc(
    state: Vecf32AggregateAvgSumStype<'_>,
) -> Option<Vecf32Output> {
    let count = state.count();
    if count == 0 {
        // return NULL if all inputs are NULL
        return None;
    }
    let sum = state
        .slice()
        .iter()
        .map(|x| *x / F32(count as f32))
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
    let count = state.count();
    if count == 0 {
        // return NULL if all inputs are NULL
        return None;
    }
    let sum = state.slice();
    Some(Vecf32Output::new(Vecf32Borrowed::new_checked(sum).unwrap()))
}
