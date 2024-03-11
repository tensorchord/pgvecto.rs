#![allow(unsafe_op_in_unsafe_fn)]

use crate::datatype::typmod::Typmod;
use crate::prelude::*;
use serde::Deserialize;
use std::ffi::CStr;

pub fn helper_offset() -> usize {
    bytemuck::offset_of!(Helper, offset)
}

pub fn helper_size() -> usize {
    std::mem::size_of::<Helper>()
}

pub unsafe fn convert_opclass_to_distance(
    opclass: pgrx::pg_sys::Oid,
) -> (DistanceKind, VectorKind) {
    let opclass_cache_id = pgrx::pg_sys::SysCacheIdentifier_CLAOID as _;
    let tuple = pgrx::pg_sys::SearchSysCache1(opclass_cache_id, opclass.into());
    assert!(
        !tuple.is_null(),
        "cache lookup failed for operator class {opclass:?}"
    );
    let classform = pgrx::pg_sys::GETSTRUCT(tuple).cast::<pgrx::pg_sys::FormData_pg_opclass>();
    let opfamily = (*classform).opcfamily;
    let result = convert_opfamily_to_distance(opfamily);
    pgrx::pg_sys::ReleaseSysCache(tuple);
    result
}

pub unsafe fn convert_opfamily_to_distance(
    opfamily: pgrx::pg_sys::Oid,
) -> (DistanceKind, VectorKind) {
    let opfamily_cache_id = pgrx::pg_sys::SysCacheIdentifier_OPFAMILYOID as _;
    let opstrategy_cache_id = pgrx::pg_sys::SysCacheIdentifier_AMOPSTRATEGY as _;
    let tuple = pgrx::pg_sys::SearchSysCache1(opfamily_cache_id, opfamily.into());
    assert!(
        !tuple.is_null(),
        "cache lookup failed for operator family {opfamily:?}"
    );
    let list = pgrx::pg_sys::SearchSysCacheList(
        opstrategy_cache_id,
        1,
        opfamily.into(),
        0.into(),
        0.into(),
    );
    assert!((*list).n_members == 1);
    let member = (*list).members.as_slice(1)[0];
    let member_tuple = &mut (*member).tuple;
    let amop = pgrx::pg_sys::GETSTRUCT(member_tuple).cast::<pgrx::pg_sys::FormData_pg_amop>();
    assert!((*amop).amopstrategy == 1);
    assert!((*amop).amoppurpose == pgrx::pg_sys::AMOP_ORDER as libc::c_char);
    let operator = (*amop).amopopr;
    let result;
    if operator == regoperatorin("vectors.<->(vectors.vector,vectors.vector)") {
        result = (DistanceKind::L2, VectorKind::Vecf32);
    } else if operator == regoperatorin("vectors.<#>(vectors.vector,vectors.vector)") {
        result = (DistanceKind::Dot, VectorKind::Vecf32);
    } else if operator == regoperatorin("vectors.<=>(vectors.vector,vectors.vector)") {
        result = (DistanceKind::Cos, VectorKind::Vecf32);
    } else if operator == regoperatorin("vectors.<->(vectors.vecf16,vectors.vecf16)") {
        result = (DistanceKind::L2, VectorKind::Vecf16);
    } else if operator == regoperatorin("vectors.<#>(vectors.vecf16,vectors.vecf16)") {
        result = (DistanceKind::Dot, VectorKind::Vecf16);
    } else if operator == regoperatorin("vectors.<=>(vectors.vecf16,vectors.vecf16)") {
        result = (DistanceKind::Cos, VectorKind::Vecf16);
    } else if operator == regoperatorin("vectors.<->(vectors.svector,vectors.svector)") {
        result = (DistanceKind::L2, VectorKind::SVecf32);
    } else if operator == regoperatorin("vectors.<#>(vectors.svector,vectors.svector)") {
        result = (DistanceKind::Dot, VectorKind::SVecf32);
    } else if operator == regoperatorin("vectors.<=>(vectors.svector,vectors.svector)") {
        result = (DistanceKind::Cos, VectorKind::SVecf32);
    } else if operator == regoperatorin("vectors.<->(vectors.bvector,vectors.bvector)") {
        result = (DistanceKind::L2, VectorKind::BVecf32);
    } else if operator == regoperatorin("vectors.<#>(vectors.bvector,vectors.bvector)") {
        result = (DistanceKind::Dot, VectorKind::BVecf32);
    } else if operator == regoperatorin("vectors.<=>(vectors.bvector,vectors.bvector)") {
        result = (DistanceKind::Cos, VectorKind::BVecf32);
    } else if operator == regoperatorin("vectors.<~>(vectors.bvector,vectors.bvector)") {
        result = (DistanceKind::Jaccard, VectorKind::BVecf32);
    } else if operator == regoperatorin("vectors.<->(vectors.veci8,vectors.veci8)") {
        result = (DistanceKind::L2, VectorKind::Veci8);
    } else if operator == regoperatorin("vectors.<#>(vectors.veci8,vectors.veci8)") {
        result = (DistanceKind::Dot, VectorKind::Veci8);
    } else if operator == regoperatorin("vectors.<=>(vectors.veci8,vectors.veci8)") {
        result = (DistanceKind::Cos, VectorKind::Veci8);
    } else {
        bad_opclass();
    };
    pgrx::pg_sys::ReleaseCatCacheList(list);
    pgrx::pg_sys::ReleaseSysCache(tuple);
    result
}

pub unsafe fn options(index_relation: pgrx::pg_sys::Relation) -> IndexOptions {
    let nkeysatts = (*(*index_relation).rd_index).indnkeyatts;
    assert!(nkeysatts == 1, "Can not be built on multicolumns.");
    // get distance
    let opfamily = (*index_relation).rd_opfamily.read();
    let (d, k) = convert_opfamily_to_distance(opfamily);
    // get dims
    let attrs = (*(*index_relation).rd_att).attrs.as_slice(1);
    let attr = &attrs[0];
    let typmod = Typmod::parse_from_i32(attr.type_mod()).unwrap();
    let dims = check_column_dims(typmod.dims()).get();
    // get other options
    let parsed = get_parsed_from_varlena((*index_relation).rd_options);
    IndexOptions {
        vector: VectorOptions { dims, d, v: k },
        segment: parsed.segment,
        optimizing: parsed.optimizing,
        indexing: parsed.indexing,
    }
}

#[derive(Copy, Clone, Debug, Default)]
#[repr(C)]
struct Helper {
    pub vl_len_: i32,
    pub offset: i32,
}

unsafe fn get_parsed_from_varlena(helper: *const pgrx::pg_sys::varlena) -> Parsed {
    let helper = helper as *const Helper;
    if helper.is_null() || (*helper).offset == 0 {
        return Default::default();
    }
    let ptr = (helper as *const libc::c_char).offset((*helper).offset as isize);
    let cstr = CStr::from_ptr(ptr);
    toml::from_str::<Parsed>(cstr.to_str().unwrap()).unwrap()
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Parsed {
    #[serde(default)]
    segment: SegmentsOptions,
    #[serde(default)]
    optimizing: OptimizingOptions,
    #[serde(default)]
    indexing: IndexingOptions,
}

fn regoperatorin(name: &str) -> pgrx::pg_sys::Oid {
    use pgrx::IntoDatum;
    let cstr = std::ffi::CString::new(name).expect("specified name has embedded NULL byte");
    unsafe {
        pgrx::direct_function_call::<pgrx::pg_sys::Oid>(
            pgrx::pg_sys::regoperatorin,
            &[cstr.as_c_str().into_datum()],
        )
        .expect("operator lookup returned NULL")
    }
}
