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

impl DerefMut for Vecf32AggregateAvgStype<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Vecf32AggregateAvgStype::Owned(p) => unsafe { p.as_mut() },
            Vecf32AggregateAvgStype::Borrowed(p) => p,
        }
    }
}

impl Drop for Vecf32AggregateAvgStype<'_> {
    fn drop(&mut self) {
        match self {
            Vecf32AggregateAvgStype::Owned(p) => unsafe {
                pgrx::pg_sys::pfree(p.as_ptr().cast());
            },
            Vecf32AggregateAvgStype::Borrowed(_) => {}
        }
    }
}

impl FromDatum for Vecf32AggregateAvgStype<'_> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typmod: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<Vecf32AggregateAvgStypeHeader>()).unwrap();
            unsafe { Some(Vecf32AggregateAvgStype::new(ptr)) }
        }
    }
}

impl IntoDatum for Vecf32AggregateAvgStype<'_> {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        let namespace =
            pgrx::pg_catalog::PgNamespace::search_namespacename(crate::SCHEMA_C_STR).unwrap();
        let namespace = namespace.get().expect("pgvecto.rs is not installed.");
        let t = pgrx::pg_catalog::PgType::search_typenamensp(
            c"_vectors_vecf32_aggregate_avg_stype",
            namespace.oid(),
        )
        .unwrap();
        let t = t.get().expect("pg_catalog is broken.");
        t.oid()
    }
}

unsafe impl SqlTranslatable for Vecf32AggregateAvgStype<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from(
            "_vectors_vecf32_aggregate_avg_stype",
        )))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from(
            "_vectors_vecf32_aggregate_avg_stype",
        ))))
    }
}

unsafe impl pgrx::callconv::BoxRet for Vecf32AggregateAvgStype<'_> {
    unsafe fn box_in_fcinfo(self, fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Datum {
        self.into_datum()
            .unwrap_or_else(|| unsafe { pgrx::fcinfo::pg_return_null(fcinfo) })
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_stype_in(
    input: &CStr,
    _oid: Oid,
    _typmod: i32,
) -> Vecf32AggregateAvgStype<'_> {
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
        Ok((count, vector)) => Vecf32AggregateAvgStype::new_with_slice(count, &vector),
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_vecf32_aggregate_avg_stype_out(state: Vecf32AggregateAvgStype<'_>) -> CString {
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
fn _vectors_vecf32_aggregate_avg_sfunc<'a>(
    mut state: Vecf32AggregateAvgStype<'a>,
    value: Vecf32Input<'_>,
) -> Vecf32AggregateAvgStype<'a> {
    let count = state.count();
    match count {
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
