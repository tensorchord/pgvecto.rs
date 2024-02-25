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
use std::cmp::Ordering;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;

pub const VECI8_KIND: u8 = 3;

/// Veci8 utilizes int8 for data storage, originally derived from Vecf32.
/// Given a vector of F32, [a_0, a_1, a_2, ..., a_n], we aim to find the maximum and minimum values. The maximum value, max, is the greatest among {a_0, a_1, a_2, ..., a_n}, and the minimum value, min, is the smallest.
/// We can transform F32 to I8 using the formula (a - (max + min) / 2) / (max - min) * 255, resulting in a vector of I8, [b_0, b_1, b_2, ..., b_n]. Here 255 is the range size that the int8 type can cover, which is the difference between -128 and 127.
/// Converting I8 back to F32 can be achieved by using the formula b * (max - min) / 255 + (max + min) / 2, which gives us a vector of F32, albeit with a slight loss of precision.
/// We use alpha to represent (max - min) / 255, and offset to represent (max + min) / 2 here.
#[repr(C, align(8))]
pub struct Veci8Header {
    varlena: u32,
    len: u16,
    kind: u8,
    reserved: u8,
    alpha: F32,
    offset: F32,
    phantom: [I8; 0],
}

impl Veci8Header {
    fn varlena(size: usize) -> u32 {
        // varattrib_4b type with compress is not set
        (size << 2) as u32
    }

    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        // size of struct VecI8
        let layout_alpha = Layout::new::<Veci8Header>();
        // size of data in VecI8
        let layout_beta = Layout::array::<I8>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn alpha(&self) -> F32 {
        self.alpha
    }

    pub fn offset(&self) -> F32 {
        self.offset
    }

    /// return value after dequantization by index
    /// since index<usize> return &Output, we can't create a new Output and return it as a reference, so we need to use this function to return a new Output directly
    #[inline(always)]
    pub fn index(&self, index: usize) -> F32 {
        self.data()[index].to_f32() * self.alpha() + self.offset()
    }

    pub fn to_ref(&self) -> Veci8Borrowed<'_> {
        Veci8Borrowed::new(self.len, self.data(), self.alpha(), self.offset())
    }

    pub fn data(&self) -> &[I8] {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, VECI8_KIND);
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len as usize) }
    }

    pub fn for_borrow(&self) -> Veci8Borrowed<'_> {
        Veci8Borrowed::new(self.len, self.data(), self.alpha, self.offset)
    }
}

impl PartialEq for Veci8Header {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        let n = self.len();
        for i in 0..n {
            if self.index(i) != other.index(i) {
                return false;
            }
        }
        true
    }
}

impl Eq for Veci8Header {}

impl PartialOrd for Veci8Header {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Veci8Header {
    fn cmp(&self, other: &Self) -> Ordering {
        use Ordering::*;
        if let x @ Less | x @ Greater = self.len().cmp(&other.len()) {
            return x;
        }
        let n = self.len();
        for i in 0..n {
            if let x @ Less | x @ Greater = self.index(i).cmp(&other.index(i)) {
                return x;
            }
        }
        Equal
    }
}

pub enum Veci8Input<'a> {
    Owned(Veci8Output),
    Borrowed(&'a Veci8Header),
}

impl<'a> Veci8Input<'a> {
    pub unsafe fn new(p: NonNull<Veci8Header>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            Veci8Input::Owned(Veci8Output(q))
        } else {
            unsafe { Veci8Input::Borrowed(p.as_ref()) }
        }
    }
}

impl Deref for Veci8Input<'_> {
    type Target = Veci8Header;

    fn deref(&self) -> &Self::Target {
        match self {
            Veci8Input::Owned(x) => x,
            Veci8Input::Borrowed(x) => x,
        }
    }
}

pub struct Veci8Output(NonNull<Veci8Header>);

impl Veci8Output {
    pub fn new(vector: Veci8Borrowed<'_>) -> Veci8Output {
        unsafe {
            let slice = vector.data();
            let layout = Veci8Header::layout(slice.len());
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Veci8Header;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(Veci8Header::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).len).write(slice.len() as u16);
            std::ptr::addr_of_mut!((*ptr).kind).write(VECI8_KIND);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            std::ptr::addr_of_mut!((*ptr).alpha).write(vector.alpha());
            std::ptr::addr_of_mut!((*ptr).offset).write(vector.offset());
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            Veci8Output(NonNull::new(ptr).unwrap())
        }
    }

    pub fn into_raw(self) -> *mut Veci8Header {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for Veci8Output {
    type Target = Veci8Header;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl DerefMut for Veci8Output {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl Drop for Veci8Output {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl<'a> FromDatum for Veci8Input<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<Veci8Header>()).unwrap();
            unsafe { Some(Veci8Input::new(ptr)) }
        }
    }
}

impl IntoDatum for Veci8Output {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vectors.veci8")
    }
}

unsafe impl SqlTranslatable for Veci8Input<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("veci8")))
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("veci8"))))
    }
}

unsafe impl SqlTranslatable for Veci8Output {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("veci8")))
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("veci8"))))
    }
}
