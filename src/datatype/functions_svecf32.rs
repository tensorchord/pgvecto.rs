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
pub struct SparseAccumulateStateHeader {
    varlena: u32,
    dims: u32,
    len: u32,
    capacity: u32,
    count: u64,
    phantom: [F32; 0],
}

impl SparseAccumulateStateHeader {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(capacity: usize) -> Layout {
        u32::try_from(capacity).expect("Vector is too large.");
        let layout_alpha = Layout::new::<SparseAccumulateStateHeader>();
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
}

fn next_power_of_two(a: usize) -> usize {
    let mut n = a;
    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n + 1
}

#[derive(Debug, Clone)]
pub enum AccumulateError {
    InPlaceMergeFailed,
}

pub enum SparseAccumulateState<'a> {
    Owned(NonNull<SparseAccumulateStateHeader>),
    Borrowed(&'a mut SparseAccumulateStateHeader),
}

impl SparseAccumulateState<'_> {
    unsafe fn new(p: NonNull<SparseAccumulateStateHeader>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.as_ptr().cast()).cast()).unwrap()
        };
        if p != q {
            SparseAccumulateState::Owned(q)
        } else {
            unsafe { SparseAccumulateState::Borrowed(&mut *p.as_ptr()) }
        }
    }

    pub fn new_with_capacity(dims: u32, capacity: usize) -> Self {
        // set capacity at least 16
        let capacity = std::cmp::max(next_power_of_two(capacity), 16);
        // set capacity at most dims
        let capacity = std::cmp::min(capacity, dims as usize);
        let layout = SparseAccumulateStateHeader::layout(capacity);
        unsafe {
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut SparseAccumulateStateHeader;
            std::ptr::addr_of_mut!((*ptr).varlena)
                .write(SparseAccumulateStateHeader::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).dims).write(dims);
            std::ptr::addr_of_mut!((*ptr).len).write(0);
            std::ptr::addr_of_mut!((*ptr).capacity).write(capacity as u32);
            std::ptr::addr_of_mut!((*ptr).count).write(0);
            SparseAccumulateState::Owned(NonNull::new(ptr).unwrap())
        }
    }

    pub fn merge_in_place(&mut self, svec: SVecf32Borrowed<'_>) -> Result<(), AccumulateError> {
        let sindexes = svec.indexes();
        let svalues = svec.values();
        let slen = sindexes.len();
        let len = self.len();
        let capacity = self.capacity();
        let (indexes, values) = self.indexes_values_mut();
        // To reduce the memory usage, we try to merge the sparse vector in place firstly.
        // If the state capacity is enough, we can merge the sparse vector from the end. Then copy the result to the beginning. The merge process compares the indexes of the sparse vector and the state from end to start, and the result is stored in the state from the end to start to avoid the conflict.
        // If result index meets with the state index, the capacity can't match requirement, return an error to tell the caller to allocate a new state.
        let mut i = (len as i32) - 1;
        let mut j = (slen as i32) - 1;
        let mut p = (capacity as i32) - 1;
        while i >= 0 && j >= 0 {
            let index = indexes[i as usize];
            let sindex = sindexes[j as usize];
            let value = values[i as usize];
            let svalue = svalues[j as usize];
            let pi = std::cmp::max(index, sindex);
            let pv = F32((pi == index) as usize as f32) * value
                + F32((pi == sindex) as usize as f32) * svalue;
            i -= (index >= sindex) as i32;
            j -= (index <= sindex) as i32;
            if p <= i {
                // The state capacity is not enough. Conflict occurs.
                return Err(AccumulateError::InPlaceMergeFailed);
            }
            indexes[p as usize] = pi;
            values[p as usize] = pv;
            p -= 1;
        }
        while j >= 0 {
            if p <= i {
                return Err(AccumulateError::InPlaceMergeFailed);
            }
            indexes[p as usize] = sindexes[j as usize];
            values[p as usize] = svalues[j as usize];
            p -= 1;
            j -= 1;
        }
        self.len = if i < 0 {
            // move the whole state [p+1..capacity] to the beginning
            indexes.copy_within(((p + 1) as usize)..capacity, 0);
            values.copy_within(((p + 1) as usize)..capacity, 0);
            ((capacity as i32) - p - 1) as u32
        } else {
            // concatenate the state [p+1..capacity] and remaining state [0..i]
            indexes.copy_within(((p + 1) as usize)..capacity, (i as usize) + 1);
            values.copy_within(((p + 1) as usize)..capacity, (i as usize) + 1);
            ((capacity as i32) - p + i) as u32
        };
        Ok(())
    }

    pub fn into_raw(self) -> *mut SparseAccumulateStateHeader {
        let result = match self {
            SparseAccumulateState::Owned(p) => p.as_ptr(),
            SparseAccumulateState::Borrowed(ref p) => {
                *p as *const SparseAccumulateStateHeader as *mut SparseAccumulateStateHeader
            }
        };
        std::mem::forget(self);
        result
    }
}

impl Deref for SparseAccumulateState<'_> {
    type Target = SparseAccumulateStateHeader;

    fn deref(&self) -> &Self::Target {
        match self {
            SparseAccumulateState::Owned(p) => unsafe { p.as_ref() },
            SparseAccumulateState::Borrowed(p) => p,
        }
    }
}

impl DerefMut for SparseAccumulateState<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            SparseAccumulateState::Owned(p) => unsafe { p.as_mut() },
            SparseAccumulateState::Borrowed(p) => p,
        }
    }
}

impl Drop for SparseAccumulateState<'_> {
    fn drop(&mut self) {
        match self {
            SparseAccumulateState::Owned(p) => unsafe {
                pgrx::pg_sys::pfree(p.as_ptr().cast());
            },
            SparseAccumulateState::Borrowed(_) => {}
        }
    }
}

impl FromDatum for SparseAccumulateState<'_> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typmod: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<SparseAccumulateStateHeader>()).unwrap();
            unsafe { Some(SparseAccumulateState::new(ptr)) }
        }
    }
}

impl IntoDatum for SparseAccumulateState<'_> {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        let namespace = pgrx::pg_catalog::PgNamespace::search_namespacename(c"vectors").unwrap();
        let namespace = namespace.get().expect("pgvecto.rs is not installed.");
        let t = pgrx::pg_catalog::PgType::search_typenamensp(
            c"svector_accumulate_state ",
            namespace.oid(),
        )
        .unwrap();
        let t = t.get().expect("pg_catalog is broken.");
        t.oid()
    }
}

unsafe impl SqlTranslatable for SparseAccumulateState<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("svector_accumulate_state")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from(
            "svector_accumulate_state",
        ))))
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_sparse_accumulate_state_in(
    input: &CStr,
    _oid: Oid,
    _typmod: i32,
) -> SparseAccumulateState<'_> {
    // std::env::set_var("RUST_BACKTRACE", "1");
    use super::functions_vecf32::parse_accumulate_state;
    let result = parse_accumulate_state(input.to_bytes());
    match result {
        Err(e) => {
            bad_literal(&e);
        }
        Ok((count, vector)) => {
            let mut indexes = Vec::<u32>::new();
            let mut values = Vec::<F32>::new();
            for (i, &x) in vector.iter().enumerate() {
                if !x.is_zero() {
                    indexes.push(i as u32);
                    values.push(x);
                }
            }
            let mut state =
                SparseAccumulateState::new_with_capacity(vector.len() as u32, indexes.len());
            if !indexes.is_empty() {
                let _ = state.merge_in_place(SVecf32Borrowed::new(
                    vector.len() as u32,
                    &indexes,
                    &values,
                ));
            }
            state.count = count;
            state
        }
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_sparse_accumulate_state_out(state: SparseAccumulateState<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push_str(format!("{}, ", state.count()).as_str());
    buffer.push('[');
    let indexes = state.indexes();
    let values = state.values();
    if !indexes.is_empty() {
        let svector = SVecf32Borrowed::new(state.dims() as u32, indexes, values);
        let vec = svector.to_vec();
        let mut iter = vec.iter();
        if let Some(x) = iter.next() {
            buffer.push_str(format!("{}", x).as_str());
        }
        for x in iter {
            buffer.push_str(format!(", {}", x).as_str());
        }
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}

/// accumulate intermediate state for sparse vector
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svector_accum<'a>(
    state: SparseAccumulateState<'a>,
    value: SVecf32Input<'_>,
) -> SparseAccumulateState<'a> {
    let count = state.count();
    match count {
        // if the state is empty, copy the input vector
        0 => {
            let mut state =
                SparseAccumulateState::new_with_capacity(value.dims() as u32, value.len());
            let _ = state.merge_in_place(value.for_borrow());
            state.count = 1;
            state
        }
        _ => {
            let dims = state.dims();
            let value_dims = value.dims();
            check_matched_dims(dims, value_dims);
            let mut state = state;
            match state.merge_in_place(value.for_borrow()) {
                Ok(_) => {}
                Err(_) => {
                    // fallback to allocate a new state and merge the input vector
                    let mut state = SparseAccumulateState::new_with_capacity(
                        dims as u32,
                        state.len() + value.len(),
                    );
                    let _ = state.merge_in_place(value.for_borrow());
                }
            }
            state.count += 1;
            state
        }
    }
}

/// combine two intermediate states for sparse vector
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svector_combine<'a>(
    state1: SparseAccumulateState<'a>,
    state2: SparseAccumulateState<'a>,
) -> SparseAccumulateState<'a> {
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
        let mut state1 = state1;
        match state1.merge_in_place(SVecf32Borrowed::new(
            dims1 as u32,
            state2.indexes(),
            state2.values(),
        )) {
            Ok(_) => {}
            Err(_) => {
                // fallback to allocate a new state and merge both states
                let mut state = SparseAccumulateState::new_with_capacity(
                    dims1 as u32,
                    state1.len() + state2.len(),
                );
                let _ = state.merge_in_place(SVecf32Borrowed::new(
                    dims1 as u32,
                    state1.indexes(),
                    state1.values(),
                ));
                let _ = state.merge_in_place(SVecf32Borrowed::new(
                    dims2 as u32,
                    state2.indexes(),
                    state2.values(),
                ));
            }
        }
        state1.count += count2;
        state1
    }
}

/// finalize the intermediate state for sparse vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svector_final_avg(mut state: SparseAccumulateState<'_>) -> Option<SVecf32Output> {
    let count = state.count();
    if count == 0 {
        // return NULL if all inputs are NULL
        None
    } else {
        let len = state.len();
        state.indexes_values_mut().1[..len]
            .iter_mut()
            .for_each(|x| *x /= count as f32);
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
fn _vectors_svector_final_sum(state: SparseAccumulateState<'_>) -> Option<SVecf32Output> {
    let count = state.count();
    if count == 0 {
        // return NULL if all inputs are NULL
        None
    } else {
        let indexes = state.indexes();
        let values = state.values();
        Some(SVecf32Output::new(SVecf32Borrowed::new(
            state.dims() as u32,
            indexes,
            values,
        )))
    }
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_dims(vector: SVecf32Input<'_>) -> i32 {
    vector.for_borrow().dims() as i32
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_norm(vector: SVecf32Input<'_>) -> f32 {
    vector.for_borrow().length().to_f32()
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_normalize(vector: SVecf32Input<'_>) -> SVecf32Output {
    SVecf32Output::new(vector.for_borrow().normalize().for_borrow())
}

#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_to_svector(
    dims: i32,
    index: pgrx::Array<i32>,
    value: pgrx::Array<f32>,
) -> SVecf32Output {
    let dims = check_value_dims_1048575(dims as usize);
    if index.len() != value.len() {
        bad_literal("Lengths of index and value are not matched.");
    }
    if index.contains_nulls() || value.contains_nulls() {
        bad_literal("Index or value contains nulls.");
    }
    let mut vector: Vec<(u32, F32)> = index
        .iter_deny_null()
        .zip(value.iter_deny_null())
        .map(|(index, value)| {
            if index < 0 || index >= dims.get() as i32 {
                bad_literal("Index out of bound.");
            }
            (index as u32, F32(value))
        })
        .collect();
    vector.sort_unstable_by_key(|x| x.0);
    if vector.len() > 1 {
        for i in 0..vector.len() - 1 {
            if vector[i].0 == vector[i + 1].0 {
                bad_literal("Duplicated index.");
            }
        }
    }

    let mut indexes = Vec::<u32>::with_capacity(vector.len());
    let mut values = Vec::<F32>::with_capacity(vector.len());
    for x in vector {
        indexes.push(x.0);
        values.push(x.1);
    }
    SVecf32Output::new(SVecf32Borrowed::new(dims.get(), &indexes, &values))
}

/// divide a sparse vector by a scalar.
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_div(vector: SVecf32Input<'_>, scalar: f32) -> SVecf32Output {
    let scalar = F32(scalar);
    let vector = vector.for_borrow();
    let indexes = vector.indexes();
    let values = vector.values();
    let new_values = values.iter().map(|&x| x / scalar).collect::<Vec<F32>>();
    SVecf32Output::new(SVecf32Borrowed::new(vector.dims(), indexes, &new_values))
}

/// Get the dimensions of a sparse vector.
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svectorf32_dims(vector: SVecf32Input<'_>) -> i32 {
    vector.dims() as i32
}

/// Calculate the l2 norm of a sparse vector.
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svectorf32_norm(vector: SVecf32Input<'_>) -> f32 {
    vector.for_borrow().l2_norm().to_f32()
}
