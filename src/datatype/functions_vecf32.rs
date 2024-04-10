#![allow(unused_lifetimes)]
#![allow(clippy::extra_unused_lifetimes)]
use crate::datatype::memory_vecf32::{Vecf32Input, Vecf32Output};
use crate::error::*;
use base::operator::{Operator, Vecf32Dot};
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
pub struct AccumulateStateHeader {
    varlena: u32,
    dims: u16,
    count: u64,
    phantom: [F32; 0],
}

impl AccumulateStateHeader {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<AccumulateStateHeader>();
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

pub enum AccumulateState<'a> {
    Owned(NonNull<AccumulateStateHeader>),
    Borrowed(&'a mut AccumulateStateHeader),
}

impl<'a> AccumulateState<'a> {
    unsafe fn new(p: NonNull<AccumulateStateHeader>) -> Self {
        // datum maybe toasted, try to detoast it
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.as_ptr().cast()).cast()).unwrap()
        };
        if p != q {
            AccumulateState::Owned(q)
        } else {
            unsafe { AccumulateState::Borrowed(&mut *p.as_ptr()) }
        }
    }

    pub fn new_with_slice(count: u64, slice: &[F32]) -> Self {
        let dims = slice.len();
        let layout = AccumulateStateHeader::layout(dims);
        unsafe {
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut AccumulateStateHeader;
            std::ptr::addr_of_mut!((*ptr).varlena)
                .write(AccumulateStateHeader::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).dims).write(dims as u16);
            std::ptr::addr_of_mut!((*ptr).count).write(count);
            if dims > 0 {
                std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), dims);
            }
            AccumulateState::Owned(NonNull::new(ptr).unwrap())
        }
    }

    pub fn into_raw(self) -> *mut AccumulateStateHeader {
        let result = match self {
            AccumulateState::Owned(p) => p.as_ptr(),
            AccumulateState::Borrowed(ref p) => {
                *p as *const AccumulateStateHeader as *mut AccumulateStateHeader
            }
        };
        std::mem::forget(self);
        result
    }
}

impl Deref for AccumulateState<'_> {
    type Target = AccumulateStateHeader;

    fn deref(&self) -> &Self::Target {
        match self {
            AccumulateState::Owned(p) => unsafe { p.as_ref() },
            AccumulateState::Borrowed(p) => p,
        }
    }
}

impl DerefMut for AccumulateState<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            AccumulateState::Owned(p) => unsafe { p.as_mut() },
            AccumulateState::Borrowed(p) => p,
        }
    }
}

impl Drop for AccumulateState<'_> {
    fn drop(&mut self) {
        match self {
            AccumulateState::Owned(p) => unsafe {
                pgrx::pg_sys::pfree(p.as_ptr().cast());
            },
            AccumulateState::Borrowed(_) => {}
        }
    }
}

impl FromDatum for AccumulateState<'_> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typmod: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<AccumulateStateHeader>()).unwrap();
            unsafe { Some(AccumulateState::new(ptr)) }
        }
    }
}

impl IntoDatum for AccumulateState<'_> {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        let namespace = pgrx::pg_catalog::PgNamespace::search_namespacename(c"vectors").unwrap();
        let namespace = namespace.get().expect("pgvecto.rs is not installed.");
        let t = pgrx::pg_catalog::PgType::search_typenamensp(c"accumulate_state", namespace.oid())
            .unwrap();
        let t = t.get().expect("pg_catalog is broken.");
        t.oid()
    }
}

unsafe impl SqlTranslatable for AccumulateState<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("accumulate_state")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from(
            "accumulate_state",
        ))))
    }
}

fn parse_accumulate_state(input: &[u8]) -> Result<(u64, Vec<F32>), String> {
    use crate::utils::parse::parse_vector;
    let hint = "Invalid input format for accumulatestate, using \'bigint, array \' like \'1, [1]\'";
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

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_accumulate_state_in(input: &CStr, _oid: Oid, _typmod: i32) -> AccumulateState<'_> {
    // parse one bigint and a vector of f32, split with a comma
    let res = parse_accumulate_state(input.to_bytes());
    match res {
        Err(e) => {
            bad_literal(&e.to_string());
        }
        Ok((count, vector)) => AccumulateState::new_with_slice(count, &vector),
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_accumulate_state_out(state: AccumulateState<'_>) -> CString {
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
fn _vectors_vector_accum<'a>(
    mut state: AccumulateState<'a>,
    value: Vecf32Input<'_>,
) -> AccumulateState<'a> {
    let count = state.count();
    match count {
        // if the state is empty, copy the input vector
        0 => AccumulateState::new_with_slice(1, value.iter().as_slice()),
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
fn _vectors_vector_combine<'a>(
    mut state1: AccumulateState<'a>,
    state2: AccumulateState<'a>,
) -> AccumulateState<'a> {
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
fn _vectors_vector_final(state: AccumulateState<'_>) -> Option<Vecf32Output> {
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

/// Get the dimensions of a vector.
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vector_dims(vector: Vecf32Input<'_>) -> i32 {
    vector.dims() as i32
}

/// Calculate the l2 norm of a vector.
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vector_norm(vector: Vecf32Input<'_>) -> f32 {
    Vecf32Dot::distance(vector.for_borrow(), vector.for_borrow())
        .to_f32()
        .sqrt()
}
