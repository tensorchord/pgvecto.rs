use crate::datatype::typmod::Typmod;
use crate::error::*;
use base::distance::*;
use base::index::*;
use base::vector::*;
use serde::Deserialize;
use std::ffi::CStr;

#[derive(Copy, Clone, Debug, Default)]
#[repr(C)]
pub struct Helper {
    pub vl_len_: i32,
    pub offset: i32,
}

pub fn helper_offset() -> usize {
    std::mem::offset_of!(Helper, offset)
}

pub fn helper_size() -> usize {
    std::mem::size_of::<Helper>()
}

pub fn convert_opclass_to_vd(opclass_oid: pgrx::pg_sys::Oid) -> Option<(VectorKind, DistanceKind)> {
    let namespace = pgrx::pg_catalog::PgNamespace::search_namespacename(c"vectors").unwrap();
    let namespace = namespace.get().expect("pgvecto.rs is not installed.");
    let opclass = pgrx::pg_catalog::PgOpclass::search_claoid(opclass_oid).unwrap();
    let opclass = opclass.get().expect("pg_catalog is broken.");
    if opclass.opcnamespace() == namespace.oid() {
        if let Ok(name) = opclass.opcname().to_str() {
            if let Some(p) = convert_name_to_vd(name) {
                return Some(p);
            }
        }
    }
    None
}

pub fn convert_opfamily_to_vd(
    opfamily_oid: pgrx::pg_sys::Oid,
) -> Option<(VectorKind, DistanceKind)> {
    let namespace = pgrx::pg_catalog::PgNamespace::search_namespacename(c"vectors").unwrap();
    let namespace = namespace.get().expect("pgvecto.rs is not installed.");
    let opfamily = pgrx::pg_catalog::PgOpfamily::search_opfamilyoid(opfamily_oid).unwrap();
    let opfamily = opfamily.get().expect("pg_catalog is broken.");
    if opfamily.opfnamespace() == namespace.oid() {
        if let Ok(name) = opfamily.opfname().to_str() {
            if let Some(p) = convert_name_to_vd(name) {
                return Some(p);
            }
        }
    }
    None
}

fn convert_name_to_vd(name: &str) -> Option<(VectorKind, DistanceKind)> {
    match name.strip_suffix("_ops") {
        Some("vector_l2") => Some((VectorKind::Vecf32, DistanceKind::L2)),
        Some("vector_dot") => Some((VectorKind::Vecf32, DistanceKind::Dot)),
        Some("vector_cos") => Some((VectorKind::Vecf32, DistanceKind::Cos)),
        Some("vecf16_l2") => Some((VectorKind::Vecf16, DistanceKind::L2)),
        Some("vecf16_dot") => Some((VectorKind::Vecf16, DistanceKind::Dot)),
        Some("vecf16_cos") => Some((VectorKind::Vecf16, DistanceKind::Cos)),
        Some("svector_l2") => Some((VectorKind::SVecf32, DistanceKind::L2)),
        Some("svector_dot") => Some((VectorKind::SVecf32, DistanceKind::Dot)),
        Some("svector_cos") => Some((VectorKind::SVecf32, DistanceKind::Cos)),
        Some("bvector_l2") => Some((VectorKind::BVecf32, DistanceKind::L2)),
        Some("bvector_dot") => Some((VectorKind::BVecf32, DistanceKind::Dot)),
        Some("bvector_cos") => Some((VectorKind::BVecf32, DistanceKind::Cos)),
        Some("bvector_jaccard") => Some((VectorKind::BVecf32, DistanceKind::Jaccard)),
        Some("veci8_l2") => Some((VectorKind::Veci8, DistanceKind::L2)),
        Some("veci8_dot") => Some((VectorKind::Veci8, DistanceKind::Dot)),
        Some("veci8_cos") => Some((VectorKind::Veci8, DistanceKind::Cos)),
        _ => None,
    }
}

unsafe fn convert_varlena_to_soi(
    varlena: *const pgrx::pg_sys::varlena,
) -> (SegmentsOptions, OptimizingOptions, IndexingOptions) {
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
    let helper = varlena as *const Helper;
    if helper.is_null() || unsafe { (*helper).offset == 0 } {
        return Default::default();
    }
    let ptr = unsafe { (helper as *const libc::c_char).offset((*helper).offset as isize) };
    let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().to_string();
    match toml::from_str::<Parsed>(&s) {
        Ok(p) => (p.segment, p.optimizing, p.indexing),
        Err(e) => pgrx::error!("failed to parse options: {}", e),
    }
}

pub unsafe fn options(index_relation: pgrx::pg_sys::Relation) -> IndexOptions {
    let opfamily = unsafe { (*index_relation).rd_opfamily.read() };
    let att = unsafe { &mut *(*index_relation).rd_att };
    let atts = unsafe { att.attrs.as_slice(att.natts as _) };
    if atts.is_empty() {
        pgrx::error!("indexing on no columns is not supported");
    }
    if atts.len() != 1 {
        pgrx::error!("multicolumn index is not supported");
    }
    // get dims
    let typmod = Typmod::parse_from_i32(atts[0].type_mod()).unwrap();
    let dims = check_column_dims(typmod.dims()).get();
    // get v, d
    let (v, d) = convert_opfamily_to_vd(opfamily).unwrap();
    // get segment, optimizing, indexing
    let (segment, optimizing, indexing) =
        unsafe { convert_varlena_to_soi((*index_relation).rd_options) };
    IndexOptions {
        vector: VectorOptions { dims, v, d },
        segment,
        optimizing,
        indexing,
    }
}
