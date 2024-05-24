#![allow(unused_lifetimes)]
#![allow(clippy::extra_unused_lifetimes)]

use super::memory_svecf32::*;
use crate::error::*;
use base::scalar::*;
use base::vector::*;
use num_traits::Zero;
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
pub struct SVecf32AggregateAvgSumStypeHeader {
    varlena: u32,
    dims: u32,
    len: u32,
    capacity: u32,
    count: u64,
    phantom: [F32; 0],
}

impl SVecf32AggregateAvgSumStypeHeader {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(capacity: usize) -> Layout {
        u32::try_from(capacity).expect("Vector is too large.");
        let layout_alpha = Layout::new::<SVecf32AggregateAvgSumStypeHeader>();
        let layout1 = Layout::array::<u32>(capacity).unwrap();
        let layout2 = Layout::array::<F32>(capacity).unwrap();
        let layout = layout_alpha.extend(layout1).unwrap().0.pad_to_align();
        layout.extend(layout2).unwrap().0.pad_to_align()
    }
    pub fn dims(&self) -> usize {
        self.dims as usize
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn capacity(&self) -> usize {
        self.capacity as usize
    }
    pub fn count(&self) -> u64 {
        self.count
    }
    /// Get the indexes of the sparse state.
    fn indexes(&self) -> &[u32] {
        let ptr = self.phantom.as_ptr().cast();
        unsafe { std::slice::from_raw_parts(ptr, self.len()) }
    }
    /// Get the values of the sparse state.
    fn values(&self) -> &[F32] {
        unsafe {
            let ptr = self.phantom.as_ptr().cast::<u32>().add(self.capacity());
            let offset = ptr.align_offset(8);
            let ptr = ptr.add(offset).cast();
            std::slice::from_raw_parts(ptr, self.len())
        }
    }
    /// Get the mutable references of the indexes and values of the sparse state. The indexes and values may contain reserved elements.
    fn indexes_values_mut(&mut self) -> (&mut [u32], &mut [F32]) {
        let ptr = self.phantom.as_mut_ptr().cast();
        let indexes = unsafe { std::slice::from_raw_parts_mut(ptr, self.capacity as usize) };
        let len = self.capacity as usize;
        let ptr = unsafe { self.phantom.as_mut_ptr().cast::<u32>().add(len) };
        let offset = ptr.align_offset(8);
        let ptr = unsafe { ptr.add(offset).cast() };
        let values = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
        (indexes, values)
    }
    /// Filter zero values from the sparse state.
    fn filter_zero(&mut self) {
        let len = self.len();
        let mut i = 0;
        let mut j = 0;
        let (indexes, values) = self.indexes_values_mut();
        while i < len {
            if !values[i].is_zero() {
                indexes[j] = indexes[i];
                values[j] = values[i];
                j += 1;
            }
            i += 1;
        }
        self.len = j as u32;
    }
}

pub struct SVecf32AggregateAvgSumStypeBorrowed<'a> {
    #[allow(dead_code)]
    dims: u32,
    len: u32,
    capacity: u32,
    #[allow(dead_code)]
    count: u64,
    indexes: &'a mut [u32],
    values: &'a mut [F32],
}

impl<'a> SVecf32AggregateAvgSumStypeBorrowed<'a> {
    pub fn new(
        dims: u32,
        len: u32,
        capacity: u32,
        count: u64,
        indexes: &'a mut [u32],
        values: &'a mut [F32],
    ) -> Self {
        Self {
            dims,
            len,
            capacity,
            count,
            indexes,
            values,
        }
    }

    /// Merge a sparse vector into the state in place.
    /// To promise the state is enough to merge the sparse vector, the caller should check the capacity of the state before calling this function.
    pub fn merge_in_place(&mut self, svec: SVecf32Borrowed<'_>) -> u32 {
        let sindexes = svec.indexes();
        let svalues = svec.values();
        let slen = sindexes.len();
        let len = self.len;
        let capacity = self.capacity as usize;
        // To reduce the memory usage, we try to merge the sparse vector in place.
        // If the state capacity is enough, we can merge the sparse vector from the end. Then copy the result to the beginning. The merge process compares the indexes of the sparse vector and the state from end to start, and the result is stored in the state from the end to start to avoid the conflict.
        let mut i = (len as i32) - 1;
        let mut j = (slen as i32) - 1;
        let mut p = (capacity as i32) - 1;
        while i >= 0 && j >= 0 {
            let index = self.indexes[i as usize];
            let sindex = sindexes[j as usize];
            let value = self.values[i as usize];
            let svalue = svalues[j as usize];
            let pi = std::cmp::max(index, sindex);
            let pv = F32((pi == index) as usize as f32) * value
                + F32((pi == sindex) as usize as f32) * svalue;
            i -= (index >= sindex) as i32;
            j -= (index <= sindex) as i32;
            assert!(p > i, "Conflict occurs when merge in place.");
            self.indexes[p as usize] = pi;
            self.values[p as usize] = pv;
            // Skip zero value.
            p -= (!pv.is_zero()) as i32;
        }
        while j >= 0 {
            assert!(p > i, "Conflict occurs when merge in place.");
            self.indexes[p as usize] = sindexes[j as usize];
            self.values[p as usize] = svalues[j as usize];
            p -= 1;
            j -= 1;
        }
        self.len = if i < 0 {
            // move the whole state [p+1..capacity] to the beginning
            self.indexes.copy_within(((p + 1) as usize)..capacity, 0);
            self.values.copy_within(((p + 1) as usize)..capacity, 0);
            ((capacity as i32) - p - 1) as u32
        } else {
            // concatenate the state [p+1..capacity] and remaining state [0..i]
            self.indexes
                .copy_within(((p + 1) as usize)..capacity, (i as usize) + 1);
            self.values
                .copy_within(((p + 1) as usize)..capacity, (i as usize) + 1);
            ((capacity as i32) - p + i) as u32
        };
        self.len
    }
}

pub enum SVecf32AggregateAvgSumStype<'a> {
    Owned(NonNull<SVecf32AggregateAvgSumStypeHeader>),
    Borrowed(&'a mut SVecf32AggregateAvgSumStypeHeader),
}

impl SVecf32AggregateAvgSumStype<'_> {
    unsafe fn new(p: NonNull<SVecf32AggregateAvgSumStypeHeader>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.as_ptr().cast()).cast()).unwrap()
        };
        if p != q {
            SVecf32AggregateAvgSumStype::Owned(q)
        } else {
            unsafe { SVecf32AggregateAvgSumStype::Borrowed(&mut *p.as_ptr()) }
        }
    }

    /// Create a new sparse accumulate state with a given capacity.
    pub fn new_with_capacity(dims: u32, capacity: usize) -> Self {
        // set capacity at least 16
        let capacity = std::cmp::max(usize::next_power_of_two(capacity), 16);
        // set capacity at most dims
        let capacity = std::cmp::min(capacity, dims as usize);
        let layout = SVecf32AggregateAvgSumStypeHeader::layout(capacity);
        unsafe {
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut SVecf32AggregateAvgSumStypeHeader;
            std::ptr::addr_of_mut!((*ptr).varlena)
                .write(SVecf32AggregateAvgSumStypeHeader::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).dims).write(dims);
            std::ptr::addr_of_mut!((*ptr).len).write(0);
            std::ptr::addr_of_mut!((*ptr).capacity).write(capacity as u32);
            std::ptr::addr_of_mut!((*ptr).count).write(0);
            SVecf32AggregateAvgSumStype::Owned(NonNull::new(ptr).unwrap())
        }
    }

    pub fn for_mut_borrow(&mut self) -> SVecf32AggregateAvgSumStypeBorrowed<'_> {
        let dims = self.dims() as u32;
        let len = self.len() as u32;
        let capacity = self.capacity() as u32;
        let count = self.count();
        let (indexes, values) = self.indexes_values_mut();
        SVecf32AggregateAvgSumStypeBorrowed::new(dims, len, capacity, count, indexes, values)
    }

    /// check whether the rest of the state is enough to append the sparse vector of the given length. Approximately predict the rest of the state is enough.
    pub fn check_capacity(&self, length: usize) -> bool {
        let capacity = self.capacity();
        let rest = capacity - self.len();
        rest >= length
    }

    pub fn merge_in_place(&mut self, svec: SVecf32Borrowed<'_>) {
        let len = self.for_mut_borrow().merge_in_place(svec);
        self.len = len;
    }

    pub fn into_raw(self) -> *mut SVecf32AggregateAvgSumStypeHeader {
        let result = match self {
            SVecf32AggregateAvgSumStype::Owned(p) => p.as_ptr(),
            SVecf32AggregateAvgSumStype::Borrowed(ref p) => {
                *p as *const SVecf32AggregateAvgSumStypeHeader
                    as *mut SVecf32AggregateAvgSumStypeHeader
            }
        };
        std::mem::forget(self);
        result
    }
}

impl Deref for SVecf32AggregateAvgSumStype<'_> {
    type Target = SVecf32AggregateAvgSumStypeHeader;

    fn deref(&self) -> &Self::Target {
        match self {
            SVecf32AggregateAvgSumStype::Owned(p) => unsafe { p.as_ref() },
            SVecf32AggregateAvgSumStype::Borrowed(p) => p,
        }
    }
}

impl DerefMut for SVecf32AggregateAvgSumStype<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            SVecf32AggregateAvgSumStype::Owned(p) => unsafe { p.as_mut() },
            SVecf32AggregateAvgSumStype::Borrowed(p) => p,
        }
    }
}

impl Drop for SVecf32AggregateAvgSumStype<'_> {
    fn drop(&mut self) {
        match self {
            SVecf32AggregateAvgSumStype::Owned(p) => unsafe {
                pgrx::pg_sys::pfree(p.as_ptr().cast());
            },
            SVecf32AggregateAvgSumStype::Borrowed(_) => {}
        }
    }
}

impl FromDatum for SVecf32AggregateAvgSumStype<'_> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typmod: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr =
                NonNull::new(datum.cast_mut_ptr::<SVecf32AggregateAvgSumStypeHeader>()).unwrap();
            unsafe { Some(SVecf32AggregateAvgSumStype::new(ptr)) }
        }
    }
}

impl IntoDatum for SVecf32AggregateAvgSumStype<'_> {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        let namespace = pgrx::pg_catalog::PgNamespace::search_namespacename(c"vectors").unwrap();
        let namespace = namespace.get().expect("pgvecto.rs is not installed.");
        let t = pgrx::pg_catalog::PgType::search_typenamensp(
            c"_vectors_svecf32_aggregate_avg_sum_stype ",
            namespace.oid(),
        )
        .unwrap();
        let t = t.get().expect("pg_catalog is broken.");
        t.oid()
    }
}

unsafe impl SqlTranslatable for SVecf32AggregateAvgSumStype<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from(
            "_vectors_svecf32_aggregate_avg_sum_stype",
        )))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from(
            "_vectors_svecf32_aggregate_avg_sum_stype",
        ))))
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_aggregate_avg_sum_stype_in(
    input: &CStr,
    _oid: Oid,
    _typmod: i32,
) -> SVecf32AggregateAvgSumStype<'_> {
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
    let result = parse(input.to_bytes());
    match result {
        Err(e) => {
            bad_literal(&e);
        }
        Ok((count, vector)) => {
            // This function only used for create a new empty state.
            let mut state = SVecf32AggregateAvgSumStype::new_with_capacity(vector.len() as u32, 0);
            state.count = count;
            state
        }
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_aggregate_avg_sum_stype_out(state: SVecf32AggregateAvgSumStype<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push_str(format!("{}, ", state.count()).as_str());
    // This function is never used.
    buffer.push_str("[]");
    CString::new(buffer).unwrap()
}

/// accumulate intermediate state for sparse vector
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_aggregate_avg_sum_sfunc<'a>(
    state: SVecf32AggregateAvgSumStype<'a>,
    value: SVecf32Input<'_>,
) -> SVecf32AggregateAvgSumStype<'a> {
    let count = state.count();
    match count {
        // if the state is empty, copy the input vector
        0 => {
            let mut state =
                SVecf32AggregateAvgSumStype::new_with_capacity(value.dims() as u32, value.len());
            state.merge_in_place(value.for_borrow());
            state.count = 1;
            state
        }
        _ => {
            let dims = state.dims();
            let value_dims = value.dims();
            check_matched_dims(dims, value_dims);
            let mut state = match state.check_capacity(value.len()) {
                true => state,
                false => {
                    // allocate a new state and merge the old state
                    let mut new_state = SVecf32AggregateAvgSumStype::new_with_capacity(
                        dims as u32,
                        state.len() + value.len(),
                    );
                    new_state.merge_in_place(SVecf32Borrowed::new(
                        dims as u32,
                        state.indexes(),
                        state.values(),
                    ));
                    new_state
                }
            };
            // merge the input vector into state
            state.merge_in_place(value.for_borrow());
            state.count = count + 1;
            state
        }
    }
}

/// combine two intermediate states for sparse vector
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_aggregate_avg_sum_combinefunc<'a>(
    state1: SVecf32AggregateAvgSumStype<'a>,
    state2: SVecf32AggregateAvgSumStype<'a>,
) -> SVecf32AggregateAvgSumStype<'a> {
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
        // ensure state1 has larger capacity
        let (state1, state2) = if state1.capacity() > state2.capacity() {
            (state1, state2)
        } else {
            (state2, state1)
        };
        let mut state = match state1.check_capacity(state2.len()) {
            true => state1,
            false => {
                // allocate a new state and merge the old state
                let mut new_state = SVecf32AggregateAvgSumStype::new_with_capacity(
                    dims1 as u32,
                    state1.len() + state2.len(),
                );
                new_state.merge_in_place(SVecf32Borrowed::new(
                    dims1 as u32,
                    state1.indexes(),
                    state1.values(),
                ));
                new_state
            }
        };
        // merge state2 into state
        state.merge_in_place(SVecf32Borrowed::new(
            state2.dims() as u32,
            state2.indexes(),
            state2.values(),
        ));
        state.count = count1 + count2;
        state
    }
}

/// finalize the intermediate state for sparse vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_aggregate_avg_finalfunc(
    mut state: SVecf32AggregateAvgSumStype<'_>,
) -> Option<SVecf32Output> {
    let count = state.count();
    if count == 0 {
        // return NULL if all inputs are NULL
        None
    } else {
        let len = state.len();
        state.indexes_values_mut().1[..len]
            .iter_mut()
            .for_each(|x| *x /= count as f32);
        state.filter_zero();
        let indexes = state.indexes();
        let values = state.values();
        Some(SVecf32Output::new(SVecf32Borrowed::new(
            state.dims() as u32,
            indexes,
            values,
        )))
    }
}

/// finalize the intermediate state for sparse vector sum
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_aggregate_sum_finalfunc(
    mut state: SVecf32AggregateAvgSumStype<'_>,
) -> Option<SVecf32Output> {
    let count = state.count();
    if count == 0 {
        // return NULL if all inputs are NULL
        None
    } else {
        state.filter_zero();
        let indexes = state.indexes();
        let values = state.values();
        Some(SVecf32Output::new(SVecf32Borrowed::new(
            state.dims() as u32,
            indexes,
            values,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_in_place() {
        // test merge_in_place success
        let indexes_20: Vec<u32> = vec![
            1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let values_20: Vec<F32> = vec![
            1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
        .iter()
        .map(|&x| F32(x as f32))
        .collect();
        let mut indexes = indexes_20.clone();
        let mut values = values_20.clone();
        let dims = 20;
        let mut len = 10;
        let mut capacity = 20;
        let count = 1;
        let mut state = SVecf32AggregateAvgSumStypeBorrowed::new(
            dims,
            len,
            capacity,
            count,
            indexes.as_mut_slice(),
            values.as_mut_slice(),
        );
        let sindexes = vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18];
        let svalues: Vec<F32> = vec![1, 2, 4, 6, 8, 10, 12, 14, 16, 18]
            .iter()
            .map(|&x| F32(x as f32))
            .collect();
        let svec = SVecf32Borrowed::new(dims, sindexes.as_slice(), svalues.as_slice());
        state.merge_in_place(svec);
        assert_eq!(state.len, 20);
        assert_eq!(
            indexes,
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19].as_slice()
        );
        assert_eq!(
            values,
            vec![1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
                .iter()
                .map(|&x| F32(x as f32))
                .collect::<Vec<F32>>()
                .as_slice()
        );
        // test merge_in_place result is full
        len = 6;
        capacity = 16;
        let mut indexes = indexes_20.clone();
        let mut values = values_20.clone();
        let mut state = SVecf32AggregateAvgSumStypeBorrowed::new(
            dims,
            len,
            capacity,
            count,
            indexes.as_mut_slice(),
            values.as_mut_slice(),
        );
        state.merge_in_place(svec);
        let result_len = state.len;
        assert_eq!(result_len, 16);
        assert_eq!(
            indexes[0..(result_len as usize)],
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 18]
                .iter()
                .map(|&x| x as u32)
                .collect::<Vec<u32>>()
        );
        assert_eq!(
            values[0..(result_len as usize)],
            vec![1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 16, 18]
                .iter()
                .map(|&x| F32(x as f32))
                .collect::<Vec<F32>>()
        );
        // test indexes overlap
        let mut indexes = indexes_20.clone();
        let mut values = values_20.clone();
        len = 10;
        capacity = 20;
        let mut state = SVecf32AggregateAvgSumStypeBorrowed::new(
            dims,
            len,
            capacity,
            count,
            indexes.as_mut_slice(),
            values.as_mut_slice(),
        );
        let sindexes = vec![0, 3, 6, 9, 12, 15, 18];
        let svalues: Vec<F32> = vec![1, 1, 1, 1, 1, 1, 1]
            .iter()
            .map(|&x| F32(x as f32))
            .collect();
        let svec = SVecf32Borrowed::new(dims, sindexes.as_slice(), svalues.as_slice());
        state.merge_in_place(svec);
        let result_len = state.len;
        assert_eq!(result_len, 14);
        assert_eq!(
            indexes[0..(result_len as usize)],
            vec![0, 1, 3, 5, 6, 7, 9, 11, 12, 13, 15, 17, 18, 19]
                .iter()
                .map(|&x| x as u32)
                .collect::<Vec<u32>>()
        );
        assert_eq!(
            values[0..(result_len as usize)],
            vec![1, 1, 4, 5, 1, 7, 10, 11, 1, 13, 16, 17, 1, 19]
                .iter()
                .map(|&x| F32(x as f32))
                .collect::<Vec<F32>>()
        );
    }
}
