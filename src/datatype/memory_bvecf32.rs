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
use std::ops::DerefMut;
use std::ptr::NonNull;

#[repr(C, align(8))]
pub struct BVecf32Header {
    varlena: u32,
    dims: u16,
    kind: u16,
    phantom: [usize; 0],
}

impl BVecf32Header {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<BVecf32Header>();
        let layout_beta = Layout::array::<usize>(len.div_ceil(BVEC_WIDTH)).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn dims(&self) -> usize {
        self.dims as usize
    }
    pub fn data(&self) -> &[usize] {
        unsafe {
            std::slice::from_raw_parts(
                self.phantom.as_ptr(),
                (self.dims as usize).div_ceil(BVEC_WIDTH),
            )
        }
    }
    pub fn for_borrow(&self) -> BVecf32Borrowed<'_> {
        unsafe { BVecf32Borrowed::new_unchecked(self.dims, self.data()) }
    }
}

pub enum BVecf32Input<'a> {
    Owned(BVecf32Output),
    Borrowed(&'a BVecf32Header),
}

impl<'a> BVecf32Input<'a> {
    pub unsafe fn new(p: NonNull<BVecf32Header>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            BVecf32Input::Owned(BVecf32Output(q))
        } else {
            unsafe { BVecf32Input::Borrowed(p.as_ref()) }
        }
    }
}

impl Deref for BVecf32Input<'_> {
    type Target = BVecf32Header;

    fn deref(&self) -> &Self::Target {
        match self {
            BVecf32Input::Owned(x) => x,
            BVecf32Input::Borrowed(x) => x,
        }
    }
}

pub struct BVecf32Output(NonNull<BVecf32Header>);

impl BVecf32Output {
    pub fn new(vector: BVecf32Borrowed<'_>) -> BVecf32Output {
        unsafe {
            let dims = vector.dims() as usize;
            let layout = BVecf32Header::layout(dims);
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut BVecf32Header;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(BVecf32Header::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).kind).write(3);
            std::ptr::addr_of_mut!((*ptr).dims).write(dims as u16);
            std::ptr::copy_nonoverlapping(
                vector.data().as_ptr(),
                (*ptr).phantom.as_mut_ptr(),
                dims.div_ceil(BVEC_WIDTH),
            );
            BVecf32Output(NonNull::new(ptr).unwrap())
        }
    }
    pub fn into_raw(self) -> *mut BVecf32Header {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for BVecf32Output {
    type Target = BVecf32Header;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl DerefMut for BVecf32Output {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl Drop for BVecf32Output {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl<'a> FromDatum for BVecf32Input<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<BVecf32Header>()).unwrap();
            unsafe { Some(BVecf32Input::new(ptr)) }
        }
    }
}

impl IntoDatum for BVecf32Output {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vectors.bvector")
    }
}

unsafe impl SqlTranslatable for BVecf32Input<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("bvector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("bvector"))))
    }
}

unsafe impl SqlTranslatable for BVecf32Output {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("bvector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("bvector"))))
    }
}
