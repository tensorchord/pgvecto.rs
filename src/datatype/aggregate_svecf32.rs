#![allow(unused_lifetimes)]
#![allow(clippy::extra_unused_lifetimes)]

use super::memory_svecf32::*;
use crate::error::*;
use base::scalar::*;
use base::vector::*;
use num_traits::Zero;
use pgrx::Internal;

pub struct SVecf32AggregateAvgSumStype {
    dims: u32,
    len: u32,
    capacity: u32,
    count: u64,
    indexes: Vec<u32>,
    values: Vec<F32>,
}

impl SVecf32AggregateAvgSumStype {
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
        &self.indexes.as_slice()[0..self.len()]
    }
    /// Get the values of the sparse state.
    fn values(&self) -> &[F32] {
        &self.values.as_slice()[0..self.len()]
    }
    /// Get the mutable references of the indexes and values of the sparse state. The indexes and values may contain reserved elements.
    fn indexes_values_mut(&mut self) -> (&mut [u32], &mut [F32]) {
        (self.indexes.as_mut_slice(), self.values.as_mut_slice())
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

    /// Create a new sparse accumulate state with a given capacity.
    pub fn new_with_capacity(dims: u32, capacity: usize) -> Self {
        // set capacity at least 16
        let capacity = std::cmp::max(usize::next_power_of_two(capacity), 16);
        // set capacity at most dims
        let capacity = std::cmp::min(capacity, dims as usize);
        let indexes = vec![0; capacity];
        let values = vec![F32::zero(); capacity];
        Self {
            dims,
            len: 0,
            capacity: capacity as u32,
            count: 0,
            indexes,
            values,
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
        // If the state is full, return true. So we can enlarger the state less than log2(n) times.
        if self.capacity() == self.dims() {
            return true;
        }
        let capacity = self.capacity();
        let rest = capacity - self.len();
        rest >= length
    }

    pub fn merge_in_place(&mut self, svec: SVecf32Borrowed<'_>) {
        let len = self.for_mut_borrow().merge_in_place(svec);
        self.len = len;
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

#[no_mangle]
#[doc(hidden)]
#[allow(unknown_lints, clippy::no_mangle_with_rust_abi, non_snake_case)]
pub extern "Rust" fn __pgrx_internals_fn__vectors_svecf32_aggregate_avg_sum_sfunc(
) -> ::pgrx::pgrx_sql_entity_graph::SqlGraphEntity {
    extern crate alloc;
    #[allow(unused_imports)]
    use alloc::{vec, vec::Vec};
    type FunctionPointer = fn(Internal, Option<SVecf32Input<'_>>) -> Internal;
    let submission = ::pgrx::pgrx_sql_entity_graph::PgExternEntity {
        name: "_vectors_svecf32_aggregate_avg_sum_sfunc",
        unaliased_name: stringify!(_vectors_svecf32_aggregate_avg_sum_sfunc),
        module_path: core::module_path!(),
        full_path: concat!(
            core::module_path!(),
            "::",
            stringify!(_vectors_svecf32_aggregate_avg_sum_sfunc)
        ),
        metadata: <FunctionPointer as ::pgrx::pgrx_sql_entity_graph::metadata::FunctionMetadata<
            _,
        >>::entity(),
        fn_args: vec![
            ::pgrx::pgrx_sql_entity_graph::PgExternArgumentEntity {
                pattern: stringify!(current),
                used_ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                    ty_source: "Internal",
                    ty_id: core::any::TypeId::of::<Internal>(),
                    full_path: core::any::type_name::<Internal>(),
                    module_path: {
                        let ty_name = core::any::type_name::<Internal>();
                        let mut path_items: Vec<_> = ty_name.split("::").collect();
                        let _ = path_items.pop();
                        path_items.join("::")
                    },
                    composite_type: None,
                    variadic: false,
                    default: None,
                    optional: false,
                    metadata: {
                        use ::pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
                        <Internal>::entity()
                    },
                },
            },
            ::pgrx::pgrx_sql_entity_graph::PgExternArgumentEntity {
                pattern: stringify!(value),
                used_ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                    ty_source: "Option < SVecf32Input < '_ > >",
                    ty_id: core::any::TypeId::of::<Option<SVecf32Input<'_>>>(),
                    full_path: core::any::type_name::<Option<SVecf32Input<'_>>>(),
                    module_path: {
                        let ty_name = core::any::type_name::<Option<SVecf32Input<'_>>>();
                        let mut path_items: Vec<_> = ty_name.split("::").collect();
                        let _ = path_items.pop();
                        path_items.join("::")
                    },
                    composite_type: None,
                    variadic: false,
                    default: None,
                    optional: true,
                    metadata: {
                        use ::pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
                        <Option<SVecf32Input<'_>>>::entity()
                    },
                },
            },
        ],
        fn_return: ::pgrx::pgrx_sql_entity_graph::PgExternReturnEntity::Type {
            ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                ty_source: "Internal",
                ty_id: core::any::TypeId::of::<Internal>(),
                full_path: core::any::type_name::<Internal>(),
                module_path: {
                    let ty_name = core::any::type_name::<Internal>();
                    let mut path_items: Vec<_> = ty_name.split("::").collect();
                    let _ = path_items.pop();
                    path_items.join("::")
                },
                composite_type: None,
                variadic: false,
                default: None,
                optional: false,
                metadata: {
                    use ::pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
                    <Internal>::entity()
                },
            },
        },
        #[allow(clippy::or_fun_call)]
        schema: None,
        file: file!(),
        line: line!(),
        extern_attrs: vec![
            ::pgrx::pgrx_sql_entity_graph::ExternArgs::Immutable,
            ::pgrx::pgrx_sql_entity_graph::ExternArgs::ParallelSafe,
        ],
        #[allow(clippy::or_fun_call)]
        search_path: None,
        #[allow(clippy::or_fun_call)]
        operator: None,
        cast: None,
        to_sql_config: ::pgrx::pgrx_sql_entity_graph::ToSqlConfigEntity {
            enabled: true,
            callback: None,
            content: None,
        },
    };
    ::pgrx::pgrx_sql_entity_graph::SqlGraphEntity::Function(submission)
}
#[doc = r" accumulate intermediate state for sparse vector"]
fn _vectors_svecf32_aggregate_avg_sum_sfunc(
    current: Internal,
    value: Option<SVecf32Input<'_>>,
) -> Internal {
    if value.is_none() {
        return current;
    }
    let value = value.unwrap();
    match unsafe { current.get_mut::<SVecf32AggregateAvgSumStype>() } {
        None => {
            let internal = Internal::new(SVecf32AggregateAvgSumStype::new_with_capacity(
                value.dims() as u32,
                value.len(),
            ));
            let state = unsafe { internal.get_mut::<SVecf32AggregateAvgSumStype>().unwrap() };
            state.merge_in_place(value.for_borrow());
            state.count = 1;
            internal
        }
        Some(state) => {
            let dims = state.dims();
            let value_dims = value.dims();
            check_matched_dims(dims, value_dims);
            let count = state.count() + 1;
            match state.check_capacity(value.len()) {
                true => {
                    state.merge_in_place(value.for_borrow());
                    state.count = count;
                    current
                }
                false => {
                    eprintln!(
                        "old state: indexes{:?} values{:?} indexes ptr{:p} values ptr{:p}",
                        state.indexes(),
                        state.values(),
                        state.indexes().as_ptr(),
                        state.values().as_ptr()
                    );
                    let new_internal =
                        Internal::new(SVecf32AggregateAvgSumStype::new_with_capacity(
                            dims as u32,
                            state.len() + value.len(),
                        ));
                    let new_state = unsafe {
                        new_internal
                            .get_mut::<SVecf32AggregateAvgSumStype>()
                            .unwrap()
                    };
                    eprintln!(
                        "new state: indexes{:?} values{:?} indexes ptr{:p} values ptr{:p}",
                        new_state.indexes(),
                        new_state.values(),
                        new_state.indexes.as_ptr(),
                        new_state.values.as_ptr()
                    );
                    new_state.merge_in_place(SVecf32Borrowed::new(
                        dims as u32,
                        state.indexes(),
                        state.values(),
                    ));
                    new_state.merge_in_place(value.for_borrow());
                    new_state.count = count;
                    new_internal
                }
            }
        }
    }
}
#[no_mangle]
#[doc(hidden)]
pub unsafe extern "C" fn _vectors_svecf32_aggregate_avg_sum_sfunc_wrapper(
    _fcinfo: ::pgrx::pg_sys::FunctionCallInfo,
) -> ::pgrx::pg_sys::Datum {
    #[allow(non_snake_case)]
    unsafe fn _vectors_svecf32_aggregate_avg_sum_sfunc_wrapper_inner(
        _fcinfo: ::pgrx::pg_sys::FunctionCallInfo,
    ) -> ::pgrx::pg_sys::Datum {
        let current_ = unsafe {
            ::pgrx::fcinfo::pg_getarg::<Internal>(_fcinfo, 0usize).unwrap_or_else(|| {
                panic!(
                    "{} is null",
                    stringify! {
                      current_
                    }
                )
            })
        };
        let value_ = unsafe { ::pgrx::fcinfo::pg_getarg::<SVecf32Input<'_>>(_fcinfo, 1usize) };
        #[allow(unused_unsafe)]
        let result = unsafe { _vectors_svecf32_aggregate_avg_sum_sfunc(current_, value_) };
        ::pgrx::datum::IntoDatum::into_datum(result)
            .unwrap_or_else(|| panic!("returned Datum was NULL"))
    }
    #[allow(unused_unsafe)]
    unsafe {
        pgrx::pg_sys::submodules::panic::pgrx_extern_c_guard(move || {
            let mut agg_context: *mut ::pgrx::pg_sys::MemoryContextData = std::ptr::null_mut();
            if ::pgrx::pg_sys::AggCheckCallContext(_fcinfo, &mut agg_context) == 0 {
                ::pgrx::error!("aggregate function called in non-aggregate context",);
            }
            let old_context = ::pgrx::pg_sys::MemoryContextSwitchTo(agg_context);
            let result = _vectors_svecf32_aggregate_avg_sum_sfunc_wrapper_inner(_fcinfo);
            ::pgrx::pg_sys::MemoryContextSwitchTo(old_context);
            result
        })
    }
}
#[no_mangle]
#[doc(hidden)]
pub extern "C" fn pg_finfo__vectors_svecf32_aggregate_avg_sum_sfunc_wrapper(
) -> &'static ::pgrx::pg_sys::Pg_finfo_record {
    const V1_API: ::pgrx::pg_sys::Pg_finfo_record =
        ::pgrx::pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}

#[no_mangle]
#[doc(hidden)]
#[allow(unknown_lints, clippy::no_mangle_with_rust_abi, non_snake_case)]
pub extern "Rust" fn __pgrx_internals_fn__vectors_svecf32_aggregate_avg_sum_combinefunc(
) -> ::pgrx::pgrx_sql_entity_graph::SqlGraphEntity {
    extern crate alloc;
    #[allow(unused_imports)]
    use alloc::{vec, vec::Vec};
    type FunctionPointer = fn(Internal, Internal) -> Internal;
    let submission = ::pgrx::pgrx_sql_entity_graph::PgExternEntity {
        name: "_vectors_svecf32_aggregate_avg_sum_combinefunc",
        unaliased_name: stringify!(_vectors_svecf32_aggregate_avg_sum_combinefunc),
        module_path: core::module_path!(),
        full_path: concat!(
            core::module_path!(),
            "::",
            stringify!(_vectors_svecf32_aggregate_avg_sum_combinefunc)
        ),
        metadata: <FunctionPointer as ::pgrx::pgrx_sql_entity_graph::metadata::FunctionMetadata<
            _,
        >>::entity(),
        fn_args: vec![
            ::pgrx::pgrx_sql_entity_graph::PgExternArgumentEntity {
                pattern: stringify!(state1),
                used_ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                    ty_source: "Internal",
                    ty_id: core::any::TypeId::of::<Internal>(),
                    full_path: core::any::type_name::<Internal>(),
                    module_path: {
                        let ty_name = core::any::type_name::<Internal>();
                        let mut path_items: Vec<_> = ty_name.split("::").collect();
                        let _ = path_items.pop();
                        path_items.join("::")
                    },
                    composite_type: None,
                    variadic: false,
                    default: None,
                    optional: false,
                    metadata: {
                        use ::pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
                        <Internal>::entity()
                    },
                },
            },
            ::pgrx::pgrx_sql_entity_graph::PgExternArgumentEntity {
                pattern: stringify!(state2),
                used_ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                    ty_source: "Internal",
                    ty_id: core::any::TypeId::of::<Internal>(),
                    full_path: core::any::type_name::<Internal>(),
                    module_path: {
                        let ty_name = core::any::type_name::<Internal>();
                        let mut path_items: Vec<_> = ty_name.split("::").collect();
                        let _ = path_items.pop();
                        path_items.join("::")
                    },
                    composite_type: None,
                    variadic: false,
                    default: None,
                    optional: false,
                    metadata: {
                        use ::pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
                        <Internal>::entity()
                    },
                },
            },
        ],
        fn_return: ::pgrx::pgrx_sql_entity_graph::PgExternReturnEntity::Type {
            ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                ty_source: "Internal",
                ty_id: core::any::TypeId::of::<Internal>(),
                full_path: core::any::type_name::<Internal>(),
                module_path: {
                    let ty_name = core::any::type_name::<Internal>();
                    let mut path_items: Vec<_> = ty_name.split("::").collect();
                    let _ = path_items.pop();
                    path_items.join("::")
                },
                composite_type: None,
                variadic: false,
                default: None,
                optional: false,
                metadata: {
                    use ::pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
                    <Internal>::entity()
                },
            },
        },
        #[allow(clippy::or_fun_call)]
        schema: None,
        file: file!(),
        line: line!(),
        extern_attrs: vec![
            ::pgrx::pgrx_sql_entity_graph::ExternArgs::Immutable,
            ::pgrx::pgrx_sql_entity_graph::ExternArgs::ParallelSafe,
        ],
        #[allow(clippy::or_fun_call)]
        search_path: None,
        #[allow(clippy::or_fun_call)]
        operator: None,
        cast: None,
        to_sql_config: ::pgrx::pgrx_sql_entity_graph::ToSqlConfigEntity {
            enabled: true,
            callback: None,
            content: None,
        },
    };
    ::pgrx::pgrx_sql_entity_graph::SqlGraphEntity::Function(submission)
}
#[doc = r" combine two intermediate states for sparse vector"]
fn _vectors_svecf32_aggregate_avg_sum_combinefunc(state1: Internal, state2: Internal) -> Internal {
    match unsafe {
        (
            state1.get_mut::<SVecf32AggregateAvgSumStype>(),
            state2.get_mut::<SVecf32AggregateAvgSumStype>(),
        )
    } {
        (None, None) => state1,
        (Some(_), None) => state1,
        (None, Some(_)) => state2,
        (Some(s1), Some(s2)) => {
            let dims1 = s1.dims();
            let dims2 = s2.dims();
            check_matched_dims(dims1, dims2);
            let (s1, s2, larger_internal) = if s1.capacity() > s2.capacity() {
                (s1, s2, 0)
            } else {
                (s2, s1, 1)
            };
            let total_count = s1.count() + s2.count();
            match s1.check_capacity(s2.len()) {
                true => {
                    s1.merge_in_place(SVecf32Borrowed::new(
                        s2.dims() as u32,
                        s2.indexes(),
                        s2.values(),
                    ));
                    s1.count = total_count;
                    if larger_internal == 0 {
                        state1
                    } else {
                        state2
                    }
                }
                false => {
                    let mut new_state = SVecf32AggregateAvgSumStype::new_with_capacity(
                        dims1 as u32,
                        s1.len() + s2.len(),
                    );
                    new_state.merge_in_place(SVecf32Borrowed::new(
                        dims1 as u32,
                        s1.indexes(),
                        s1.values(),
                    ));
                    new_state.merge_in_place(SVecf32Borrowed::new(
                        s2.dims() as u32,
                        s2.indexes(),
                        s2.values(),
                    ));
                    new_state.count = total_count;
                    Internal::new(new_state)
                }
            }
        }
    }
}
#[no_mangle]
#[doc(hidden)]
pub unsafe extern "C" fn _vectors_svecf32_aggregate_avg_sum_combinefunc_wrapper(
    _fcinfo: ::pgrx::pg_sys::FunctionCallInfo,
) -> ::pgrx::pg_sys::Datum {
    #[allow(non_snake_case)]
    unsafe fn _vectors_svecf32_aggregate_avg_sum_combinefunc_wrapper_inner(
        _fcinfo: ::pgrx::pg_sys::FunctionCallInfo,
    ) -> ::pgrx::pg_sys::Datum {
        let state1_ = unsafe {
            ::pgrx::fcinfo::pg_getarg::<Internal>(_fcinfo, 0usize).unwrap_or_else(|| {
                panic!(
                    "{} is null",
                    stringify! {
                      state1_
                    }
                )
            })
        };
        let state2_ = unsafe {
            ::pgrx::fcinfo::pg_getarg::<Internal>(_fcinfo, 1usize).unwrap_or_else(|| {
                panic!(
                    "{} is null",
                    stringify! {
                      state2_
                    }
                )
            })
        };
        #[allow(unused_unsafe)]
        let result = unsafe { _vectors_svecf32_aggregate_avg_sum_combinefunc(state1_, state2_) };
        ::pgrx::datum::IntoDatum::into_datum(result)
            .unwrap_or_else(|| panic!("returned Datum was NULL"))
    }
    #[allow(unused_unsafe)]
    unsafe {
        pgrx::pg_sys::submodules::panic::pgrx_extern_c_guard(move || {
            let mut agg_context: *mut ::pgrx::pg_sys::MemoryContextData = std::ptr::null_mut();
            if ::pgrx::pg_sys::AggCheckCallContext(_fcinfo, &mut agg_context) == 0 {
                ::pgrx::error!("aggregate function called in non-aggregate context",);
            }
            let old_context = ::pgrx::pg_sys::MemoryContextSwitchTo(agg_context);
            let result = _vectors_svecf32_aggregate_avg_sum_combinefunc_wrapper_inner(_fcinfo);
            ::pgrx::pg_sys::MemoryContextSwitchTo(old_context);
            result
        })
    }
}
#[no_mangle]
#[doc(hidden)]
pub extern "C" fn pg_finfo__vectors_svecf32_aggregate_avg_sum_combinefunc_wrapper(
) -> &'static ::pgrx::pg_sys::Pg_finfo_record {
    const V1_API: ::pgrx::pg_sys::Pg_finfo_record =
        ::pgrx::pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}

/// finalize the intermediate state for sparse vector average
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_aggregate_avg_finalfunc(state: Internal) -> Option<SVecf32Output> {
    match unsafe { state.get_mut::<SVecf32AggregateAvgSumStype>() } {
        Some(state) => {
            let len = state.len();
            let count = state.count();
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
        None => None,
    }
}

/// finalize the intermediate state for sparse vector sum
#[pgrx::pg_extern(immutable, strict, parallel_safe)]
fn _vectors_svecf32_aggregate_sum_finalfunc(state: Internal) -> Option<SVecf32Output> {
    match unsafe { state.get_mut::<SVecf32AggregateAvgSumStype>() } {
        Some(state) => {
            state.filter_zero();
            let indexes = state.indexes();
            let values = state.values();
            Some(SVecf32Output::new(SVecf32Borrowed::new(
                state.dims() as u32,
                indexes,
                values,
            )))
        }
        None => None,
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
