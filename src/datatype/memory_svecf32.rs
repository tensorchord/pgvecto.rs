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
pub struct SVecf32Header {
    varlena: u32,
    reserved: u16,
    kind: u16,
    dims: u32,
    len: u32,
    phantom: [u8; 0],
}

impl SVecf32Header {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u32::try_from(len).expect("Vector is too large.");
        let layout = Layout::new::<SVecf32Header>();
        let layout1 = Layout::array::<u32>(len).unwrap();
        let layout2 = Layout::array::<F32>(len).unwrap();
        let layout = layout.extend(layout1).unwrap().0.pad_to_align();
        layout.extend(layout2).unwrap().0.pad_to_align()
    }
    pub fn dims(&self) -> usize {
        self.dims as usize
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    fn indexes(&self) -> &[u32] {
        let ptr = self.phantom.as_ptr().cast();
        unsafe { std::slice::from_raw_parts(ptr, self.len as usize) }
    }
    fn values(&self) -> &[F32] {
        let len = self.len as usize;
        unsafe {
            let ptr = self.phantom.as_ptr().cast::<u32>().add(len);
            let offset = ptr.align_offset(8);
            let ptr = ptr.add(offset).cast();
            std::slice::from_raw_parts(ptr, len)
        }
    }
    pub fn for_borrow(&self) -> SVecf32Borrowed<'_> {
        unsafe { SVecf32Borrowed::new_unchecked(self.dims, self.indexes(), self.values()) }
    }
}

pub enum SVecf32Input<'a> {
    Owned(SVecf32Output),
    Borrowed(&'a SVecf32Header),
}

impl<'a> SVecf32Input<'a> {
    unsafe fn new(p: NonNull<SVecf32Header>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            SVecf32Input::Owned(SVecf32Output(q))
        } else {
            unsafe { SVecf32Input::Borrowed(p.as_ref()) }
        }
    }
}

impl Deref for SVecf32Input<'_> {
    type Target = SVecf32Header;

    fn deref(&self) -> &Self::Target {
        match self {
            SVecf32Input::Owned(x) => x,
            SVecf32Input::Borrowed(x) => x,
        }
    }
}

pub struct SVecf32Output(NonNull<SVecf32Header>);

impl SVecf32Output {
    pub fn new(vector: SVecf32Borrowed<'_>) -> SVecf32Output {
        unsafe {
            let layout = SVecf32Header::layout(vector.len() as usize);
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut SVecf32Header;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(SVecf32Header::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).dims).write(vector.dims());
            std::ptr::addr_of_mut!((*ptr).kind).write(2);
            std::ptr::addr_of_mut!((*ptr).len).write(vector.len());
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            let mut data_ptr = (*ptr).phantom.as_mut_ptr().cast::<u32>();
            std::ptr::copy_nonoverlapping(
                vector.indexes().as_ptr(),
                data_ptr,
                vector.len() as usize,
            );
            data_ptr = data_ptr.add(vector.len() as usize);
            let offset = data_ptr.align_offset(8);
            std::ptr::write_bytes(data_ptr, 0, offset);
            data_ptr = data_ptr.add(offset);
            std::ptr::copy_nonoverlapping(
                vector.values().as_ptr(),
                data_ptr.cast(),
                vector.len() as usize,
            );
            SVecf32Output(NonNull::new(ptr).unwrap())
        }
    }
    pub fn into_raw(self) -> *mut SVecf32Header {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for SVecf32Output {
    type Target = SVecf32Header;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl Drop for SVecf32Output {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl<'a> FromDatum for SVecf32Input<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<SVecf32Header>()).unwrap();
            unsafe { Some(SVecf32Input::new(ptr)) }
        }
    }
}

impl IntoDatum for SVecf32Output {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vectors.svector")
    }
}

unsafe impl SqlTranslatable for SVecf32Input<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("svector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("svector"))))
    }
}

unsafe impl SqlTranslatable for SVecf32Output {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("svector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("svector"))))
    }
}
