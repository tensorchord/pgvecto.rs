use crate::datatype::memory_bvecf32::BVecf32Input;
use crate::datatype::memory_bvecf32::BVecf32Output;
use crate::datatype::memory_svecf32::SVecf32Input;
use crate::datatype::memory_svecf32::SVecf32Output;
use crate::datatype::memory_vecf16::Vecf16Input;
use crate::datatype::memory_vecf16::Vecf16Output;
use crate::datatype::memory_vecf32::Vecf32Input;
use crate::datatype::memory_vecf32::Vecf32Output;
use crate::datatype::memory_veci8::Veci8Input;
use crate::datatype::memory_veci8::Veci8Output;
use crate::datatype::typmod::Typmod;
use crate::error::*;
use base::distance::*;
use base::index::*;
use base::vector::*;
use pgrx::heap_tuple::PgHeapTuple;
use pgrx::FromDatum;
use serde::Deserialize;
use std::ffi::CStr;
use std::num::NonZero;

#[derive(Copy, Clone, Debug, Default)]
#[repr(C)]
pub struct Reloption {
    vl_len_: i32,
    pub options: i32,
}

impl Reloption {
    pub const TAB: &'static [pgrx::pg_sys::relopt_parse_elt] = &[pgrx::pg_sys::relopt_parse_elt {
        optname: c"options".as_ptr(),
        opttype: pgrx::pg_sys::relopt_type_RELOPT_TYPE_STRING,
        offset: std::mem::offset_of!(Reloption, options) as i32,
    }];
    unsafe fn options(&self) -> &CStr {
        unsafe {
            let ptr = std::ptr::addr_of!(*self)
                .cast::<std::ffi::c_char>()
                .offset(self.options as _);
            CStr::from_ptr(ptr)
        }
    }
}

pub fn convert_opclass_to_vd(opclass_oid: pgrx::pg_sys::Oid) -> Option<(VectorKind, DistanceKind)> {
    let namespace =
        pgrx::pg_catalog::PgNamespace::search_namespacename(crate::SCHEMA_C_STR).unwrap();
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
    let namespace =
        pgrx::pg_catalog::PgNamespace::search_namespacename(crate::SCHEMA_C_STR).unwrap();
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

unsafe fn convert_reloptions_to_options(
    reloptions: *const pgrx::pg_sys::varlena,
) -> (IndexingOptions, IndexAlterableOptions) {
    #[derive(Debug, Clone, Deserialize, Default)]
    #[serde(deny_unknown_fields)]
    struct Parsed {
        #[serde(default)]
        indexing: IndexingOptions,
        #[serde(flatten)]
        alterable: IndexAlterableOptions,
    }
    let reloption = reloptions as *const Reloption;
    if reloption.is_null() || unsafe { (*reloption).options == 0 } {
        return Default::default();
    }
    let s = unsafe { (*reloption).options() }.to_string_lossy();
    match toml::from_str::<Parsed>(&s) {
        Ok(p) => (p.indexing, p.alterable),
        Err(e) => pgrx::error!("failed to parse options: {}", e),
    }
}

pub unsafe fn options(index: pgrx::pg_sys::Relation) -> (IndexOptions, IndexAlterableOptions) {
    let opfamily = unsafe { (*index).rd_opfamily.read() };
    let att = unsafe { &mut *(*index).rd_att };
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
    let vector = VectorOptions { dims, v, d };
    // get indexing, segment, optimizing
    let (indexing, alterable) = unsafe { convert_reloptions_to_options((*index).rd_options) };
    (IndexOptions { vector, indexing }, alterable)
}

#[derive(Debug, Clone, Copy)]
pub struct Opfamily {
    vector: VectorKind,
    #[allow(unused)]
    distance: DistanceKind,
}

impl Opfamily {
    pub unsafe fn datum_to_vector(
        self,
        datum: pgrx::pg_sys::Datum,
        is_null: bool,
    ) -> Option<OwnedVector> {
        if is_null || datum.is_null() {
            return None;
        }
        let vector = match self.vector {
            VectorKind::Vecf32 => {
                let vector = unsafe { Vecf32Input::from_datum(datum, false).unwrap() };
                OwnedVector::Vecf32(vector.for_borrow().for_own())
            }
            VectorKind::Vecf16 => {
                let vector = unsafe { Vecf16Input::from_datum(datum, false).unwrap() };
                OwnedVector::Vecf16(vector.for_borrow().for_own())
            }
            VectorKind::SVecf32 => {
                let vector = unsafe { SVecf32Input::from_datum(datum, false).unwrap() };
                OwnedVector::SVecf32(vector.for_borrow().for_own())
            }
            VectorKind::BVecf32 => {
                let vector = unsafe { BVecf32Input::from_datum(datum, false).unwrap() };
                OwnedVector::BVecf32(vector.for_borrow().for_own())
            }
            VectorKind::Veci8 => {
                let vector = unsafe { Veci8Input::from_datum(datum, false).unwrap() };
                OwnedVector::Veci8(vector.for_borrow().for_own())
            }
        };
        Some(vector)
    }
    pub unsafe fn datum_to_sphere(
        self,
        datum: pgrx::pg_sys::Datum,
        is_null: bool,
    ) -> (Option<OwnedVector>, Option<f32>) {
        if is_null || datum.is_null() {
            return (None, None);
        }
        let tuple = unsafe { PgHeapTuple::from_composite_datum(datum) };
        let center = match self.vector {
            VectorKind::Vecf32 => tuple
                .get_by_index::<Vecf32Output>(NonZero::new(1).unwrap())
                .unwrap()
                .map(|vector| OwnedVector::Vecf32(vector.for_borrow().for_own())),
            VectorKind::Vecf16 => tuple
                .get_by_index::<Vecf16Output>(NonZero::new(1).unwrap())
                .unwrap()
                .map(|vector| OwnedVector::Vecf16(vector.for_borrow().for_own())),
            VectorKind::SVecf32 => tuple
                .get_by_index::<SVecf32Output>(NonZero::new(1).unwrap())
                .unwrap()
                .map(|vector| OwnedVector::SVecf32(vector.for_borrow().for_own())),
            VectorKind::BVecf32 => tuple
                .get_by_index::<BVecf32Output>(NonZero::new(1).unwrap())
                .unwrap()
                .map(|vector| OwnedVector::BVecf32(vector.for_borrow().for_own())),
            VectorKind::Veci8 => tuple
                .get_by_index::<Veci8Output>(NonZero::new(1).unwrap())
                .unwrap()
                .map(|vector| OwnedVector::Veci8(vector.for_borrow().for_own())),
        };
        let radius = tuple.get_by_index::<f32>(NonZero::new(2).unwrap()).unwrap();
        (center, radius)
    }
}

pub unsafe fn opfamily(index: pgrx::pg_sys::Relation) -> Opfamily {
    let opfamily = unsafe { (*index).rd_opfamily.read() };
    let (vector, distance) = convert_opfamily_to_vd(opfamily).unwrap();
    Opfamily { vector, distance }
}
