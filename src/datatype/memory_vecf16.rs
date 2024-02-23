use crate::prelude::*;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::ArgumentError;
use pgrx::pgrx_sql_entity_graph::metadata::Returns;
use pgrx::pgrx_sql_entity_graph::metadata::ReturnsError;
use pgrx::pgrx_sql_entity_graph::metadata::SqlMapping;
use pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
use pgrx::FromDatum;
use pgrx::IntoDatum;
use std::alloc::Layout;
use std::ops::Deref;
use std::ptr::NonNull;

#[repr(C, align(8))]
pub struct Vecf16Header {
    varlena: u32,
    dims: u16,
    kind: u16,
    phantom: [F16; 0],
}

impl Vecf16Header {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<Vecf16Header>();
        let layout_beta = Layout::array::<F16>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn dims(&self) -> usize {
        self.dims as usize
    }
    pub fn slice(&self) -> &[F16] {
        debug_assert_eq!(self.varlena & 3, 0);
        // TODO: force checking it in the future
        // debug_assert_eq!(self.kind, 1);
        // debug_assert_eq!(self.reserved, 0);
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.dims as usize) }
    }
    pub fn for_borrow(&self) -> Vecf16Borrowed<'_> {
        unsafe { Vecf16Borrowed::new_unchecked(self.slice()) }
    }
}

impl Deref for Vecf16Header {
    type Target = [F16];

    fn deref(&self) -> &Self::Target {
        self.slice()
    }
}

pub enum Vecf16Input<'a> {
    Owned(Vecf16Output),
    Borrowed(&'a Vecf16Header),
}

impl<'a> Vecf16Input<'a> {
    unsafe fn new(p: NonNull<Vecf16Header>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            Vecf16Input::Owned(Vecf16Output(q))
        } else {
            unsafe { Vecf16Input::Borrowed(p.as_ref()) }
        }
    }
}

impl Deref for Vecf16Input<'_> {
    type Target = Vecf16Header;

    fn deref(&self) -> &Self::Target {
        match self {
            Vecf16Input::Owned(x) => x,
            Vecf16Input::Borrowed(x) => x,
        }
    }
}

pub struct Vecf16Output(NonNull<Vecf16Header>);

impl Vecf16Output {
    pub fn new(vector: Vecf16Borrowed<'_>) -> Vecf16Output {
        unsafe {
            let slice = vector.slice();
            let layout = Vecf16Header::layout(slice.len());
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Vecf16Header;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(Vecf16Header::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).kind).write(1);
            std::ptr::addr_of_mut!((*ptr).dims).write(slice.len() as u16);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            Vecf16Output(NonNull::new(ptr).unwrap())
        }
    }
    pub fn into_raw(self) -> *mut Vecf16Header {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for Vecf16Output {
    type Target = Vecf16Header;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl Drop for Vecf16Output {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl<'a> FromDatum for Vecf16Input<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<Vecf16Header>()).unwrap();
            unsafe { Some(Vecf16Input::new(ptr)) }
        }
    }
}

impl IntoDatum for Vecf16Output {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vectors.vecf16")
    }
}

unsafe impl SqlTranslatable for Vecf16Input<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vecf16")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vecf16"))))
    }
}

unsafe impl SqlTranslatable for Vecf16Output {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vecf16")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vecf16"))))
    }
}
