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

#[no_mangle]
#[doc(hidden)]
#[allow(unknown_lints, clippy::no_mangle_with_rust_abi, non_snake_case)]
pub extern "Rust" fn __pgrx_internals_fn__vectors_vecf32_aggregate_avg_sum_sfunc(
) -> ::pgrx::pgrx_sql_entity_graph::SqlGraphEntity {
    extern crate alloc;
    #[allow(unused_imports)]
    use alloc::{vec, vec::Vec};
    type FunctionPointer = for<'a> fn(
        Option<Vecf32AggregateAvgSumStype<'a>>,
        Option<Vecf32Input<'_>>,
    ) -> Option<Vecf32AggregateAvgSumStype<'a>>;
    let submission = ::pgrx::pgrx_sql_entity_graph::PgExternEntity {
        name: "_vectors_vecf32_aggregate_avg_sum_sfunc",
        unaliased_name: stringify!(_vectors_vecf32_aggregate_avg_sum_sfunc),
        module_path: core::module_path!(),
        full_path: concat!(
            core::module_path!(),
            "::",
            stringify!(_vectors_vecf32_aggregate_avg_sum_sfunc)
        ),
        metadata: <FunctionPointer as ::pgrx::pgrx_sql_entity_graph::metadata::FunctionMetadata<
            _,
        >>::entity(),
        fn_args: vec![
            ::pgrx::pgrx_sql_entity_graph::PgExternArgumentEntity {
                pattern: stringify!(state),
                used_ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                    ty_source: "Option < Vecf32AggregateAvgSumStype < '_ > >",
                    ty_id: core::any::TypeId::of::<Option<Vecf32AggregateAvgSumStype<'_>>>(),
                    full_path: core::any::type_name::<Option<Vecf32AggregateAvgSumStype<'_>>>(),
                    module_path: {
                        let ty_name =
                            core::any::type_name::<Option<Vecf32AggregateAvgSumStype<'_>>>();
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
                        <Option<Vecf32AggregateAvgSumStype<'_>>>::entity()
                    },
                },
            },
            ::pgrx::pgrx_sql_entity_graph::PgExternArgumentEntity {
                pattern: stringify!(value),
                used_ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                    ty_source: "Option < Vecf32Input < '_ > >",
                    ty_id: core::any::TypeId::of::<Option<Vecf32Input<'_>>>(),
                    full_path: core::any::type_name::<Option<Vecf32Input<'_>>>(),
                    module_path: {
                        let ty_name = core::any::type_name::<Option<Vecf32Input<'_>>>();
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
                        <Option<Vecf32Input<'_>>>::entity()
                    },
                },
            },
        ],
        fn_return: ::pgrx::pgrx_sql_entity_graph::PgExternReturnEntity::Type {
            ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                ty_source: "Option < Vecf32AggregateAvgSumStype < '_ > >",
                ty_id: core::any::TypeId::of::<Option<Vecf32AggregateAvgSumStype<'_>>>(),
                full_path: core::any::type_name::<Option<Vecf32AggregateAvgSumStype<'_>>>(),
                module_path: {
                    let ty_name = core::any::type_name::<Option<Vecf32AggregateAvgSumStype<'_>>>();
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
                    <Option<Vecf32AggregateAvgSumStype<'_>>>::entity()
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
#[doc = r" accumulate intermediate state for vector average"]
fn _vectors_vecf32_aggregate_avg_sum_sfunc<'a>(
    state: Option<Vecf32AggregateAvgSumStype<'a>>,
    value: Option<Vecf32Input<'_>>,
) -> Option<Vecf32AggregateAvgSumStype<'a>> {
    if value.is_none() {
        return state;
    }
    let value = value.unwrap();
    match state {
        None => Some(Vecf32AggregateAvgSumStype::new_with_slice(
            1,
            value.iter().as_slice(),
        )),
        Some(mut state) => {
            let dims = state.dims();
            let value_dims = value.dims();
            check_matched_dims(dims, value_dims);
            let sum = state.slice_mut();
            for (x, y) in sum.iter_mut().zip(value.iter()) {
                *x += *y;
            }
            state.count += 1;
            Some(state)
        }
    }
}
#[no_mangle]
#[doc(hidden)]
pub unsafe extern "C" fn _vectors_vecf32_aggregate_avg_sum_sfunc_wrapper<'a>(
    _fcinfo: ::pgrx::pg_sys::FunctionCallInfo,
) -> ::pgrx::pg_sys::Datum {
    #[allow(non_snake_case)]
    unsafe fn _vectors_vecf32_aggregate_avg_sum_sfunc_wrapper_inner<'a>(
        _fcinfo: ::pgrx::pg_sys::FunctionCallInfo,
    ) -> ::pgrx::pg_sys::Datum {
        let state_ =
            unsafe { ::pgrx::fcinfo::pg_getarg::<Vecf32AggregateAvgSumStype<'a>>(_fcinfo, 0usize) };
        let value_ = unsafe { ::pgrx::fcinfo::pg_getarg::<Vecf32Input<'_>>(_fcinfo, 1usize) };
        #[allow(unused_unsafe)]
        let result = unsafe { _vectors_vecf32_aggregate_avg_sum_sfunc(state_, value_) };
        match result {
            Some(result) => ::pgrx::datum::IntoDatum::into_datum(result)
                .unwrap_or_else(|| panic!("returned Option<T> was NULL")),
            None => unsafe { ::pgrx::fcinfo::pg_return_null(_fcinfo) },
        }
    }
    #[allow(unused_unsafe)]
    unsafe {
        pgrx::pg_sys::submodules::panic::pgrx_extern_c_guard(move || {
            let mut agg_context: *mut ::pgrx::pg_sys::MemoryContextData = std::ptr::null_mut();
            if ::pgrx::pg_sys::AggCheckCallContext(_fcinfo, &mut agg_context) == 0 {
                ::pgrx::error!("aggregate function called in non-aggregate context",);
            }
            let old_context = ::pgrx::pg_sys::MemoryContextSwitchTo(agg_context);
            let result = _vectors_vecf32_aggregate_avg_sum_sfunc_wrapper_inner(_fcinfo);
            ::pgrx::pg_sys::MemoryContextSwitchTo(old_context);
            result
        })
    }
}
#[no_mangle]
#[doc(hidden)]
pub extern "C" fn pg_finfo__vectors_vecf32_aggregate_avg_sum_sfunc_wrapper(
) -> &'static ::pgrx::pg_sys::Pg_finfo_record {
    const V1_API: ::pgrx::pg_sys::Pg_finfo_record =
        ::pgrx::pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
}

#[no_mangle]
#[doc(hidden)]
#[allow(unknown_lints, clippy::no_mangle_with_rust_abi, non_snake_case)]
pub extern "Rust" fn __pgrx_internals_fn__vectors_vecf32_aggregate_avg_sum_combinefunc(
) -> ::pgrx::pgrx_sql_entity_graph::SqlGraphEntity {
    extern crate alloc;
    #[allow(unused_imports)]
    use alloc::{vec, vec::Vec};
    type FunctionPointer = for<'a> fn(
        Option<Vecf32AggregateAvgSumStype<'a>>,
        Option<Vecf32AggregateAvgSumStype<'a>>,
    ) -> Option<Vecf32AggregateAvgSumStype<'a>>;
    let submission = ::pgrx::pgrx_sql_entity_graph::PgExternEntity {
        name: "_vectors_vecf32_aggregate_avg_sum_combinefunc",
        unaliased_name: stringify!(_vectors_vecf32_aggregate_avg_sum_combinefunc),
        module_path: core::module_path!(),
        full_path: concat!(
            core::module_path!(),
            "::",
            stringify!(_vectors_vecf32_aggregate_avg_sum_combinefunc)
        ),
        metadata: <FunctionPointer as ::pgrx::pgrx_sql_entity_graph::metadata::FunctionMetadata<
            _,
        >>::entity(),
        fn_args: vec![
            ::pgrx::pgrx_sql_entity_graph::PgExternArgumentEntity {
                pattern: stringify!(state1),
                used_ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                    ty_source: "Option < Vecf32AggregateAvgSumStype < '_ > >",
                    ty_id: core::any::TypeId::of::<Option<Vecf32AggregateAvgSumStype<'_>>>(),
                    full_path: core::any::type_name::<Option<Vecf32AggregateAvgSumStype<'_>>>(),
                    module_path: {
                        let ty_name =
                            core::any::type_name::<Option<Vecf32AggregateAvgSumStype<'_>>>();
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
                        <Option<Vecf32AggregateAvgSumStype<'_>>>::entity()
                    },
                },
            },
            ::pgrx::pgrx_sql_entity_graph::PgExternArgumentEntity {
                pattern: stringify!(state2),
                used_ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                    ty_source: "Option < Vecf32AggregateAvgSumStype < '_ > >",
                    ty_id: core::any::TypeId::of::<Option<Vecf32AggregateAvgSumStype<'_>>>(),
                    full_path: core::any::type_name::<Option<Vecf32AggregateAvgSumStype<'_>>>(),
                    module_path: {
                        let ty_name =
                            core::any::type_name::<Option<Vecf32AggregateAvgSumStype<'_>>>();
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
                        <Option<Vecf32AggregateAvgSumStype<'_>>>::entity()
                    },
                },
            },
        ],
        fn_return: ::pgrx::pgrx_sql_entity_graph::PgExternReturnEntity::Type {
            ty: ::pgrx::pgrx_sql_entity_graph::UsedTypeEntity {
                ty_source: "Option < Vecf32AggregateAvgSumStype < '_ > >",
                ty_id: core::any::TypeId::of::<Option<Vecf32AggregateAvgSumStype<'_>>>(),
                full_path: core::any::type_name::<Option<Vecf32AggregateAvgSumStype<'_>>>(),
                module_path: {
                    let ty_name = core::any::type_name::<Option<Vecf32AggregateAvgSumStype<'_>>>();
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
                    <Option<Vecf32AggregateAvgSumStype<'_>>>::entity()
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
#[doc = r" combine two intermediate states for vector average"]
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
#[no_mangle]
#[doc(hidden)]
pub unsafe extern "C" fn _vectors_vecf32_aggregate_avg_sum_combinefunc_wrapper<'a>(
    _fcinfo: ::pgrx::pg_sys::FunctionCallInfo,
) -> ::pgrx::pg_sys::Datum {
    #[allow(non_snake_case)]
    unsafe fn _vectors_vecf32_aggregate_avg_sum_combinefunc_wrapper_inner<'a>(
        _fcinfo: ::pgrx::pg_sys::FunctionCallInfo,
    ) -> ::pgrx::pg_sys::Datum {
        let state1_ =
            unsafe { ::pgrx::fcinfo::pg_getarg::<Vecf32AggregateAvgSumStype<'a>>(_fcinfo, 0usize) };
        let state2_ =
            unsafe { ::pgrx::fcinfo::pg_getarg::<Vecf32AggregateAvgSumStype<'a>>(_fcinfo, 1usize) };
        #[allow(unused_unsafe)]
        let result = unsafe { _vectors_vecf32_aggregate_avg_sum_combinefunc(state1_, state2_) };
        match result {
            Some(result) => ::pgrx::datum::IntoDatum::into_datum(result)
                .unwrap_or_else(|| panic!("returned Option<T> was NULL")),
            None => unsafe { ::pgrx::fcinfo::pg_return_null(_fcinfo) },
        }
    }
    #[allow(unused_unsafe)]
    unsafe {
        pgrx::pg_sys::submodules::panic::pgrx_extern_c_guard(move || {
            let mut agg_context: *mut ::pgrx::pg_sys::MemoryContextData = std::ptr::null_mut();
            if ::pgrx::pg_sys::AggCheckCallContext(_fcinfo, &mut agg_context) == 0 {
                ::pgrx::error!("aggregate function called in non-aggregate context",);
            }
            let old_context = ::pgrx::pg_sys::MemoryContextSwitchTo(agg_context);
            let result = _vectors_vecf32_aggregate_avg_sum_combinefunc_wrapper_inner(_fcinfo);
            ::pgrx::pg_sys::MemoryContextSwitchTo(old_context);
            result
        })
    }
}
#[no_mangle]
#[doc(hidden)]
pub extern "C" fn pg_finfo__vectors_vecf32_aggregate_avg_sum_combinefunc_wrapper(
) -> &'static ::pgrx::pg_sys::Pg_finfo_record {
    const V1_API: ::pgrx::pg_sys::Pg_finfo_record =
        ::pgrx::pg_sys::Pg_finfo_record { api_version: 1 };
    &V1_API
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
