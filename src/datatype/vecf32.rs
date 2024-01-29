use crate::datatype::typmod::Typmod;
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
use service::prelude::*;
use std::alloc::Layout;
use std::cmp::Ordering;
use std::ffi::CStr;
use std::ffi::CString;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Index;
use std::ops::IndexMut;
use std::ptr::NonNull;

#[repr(C, align(8))]
pub struct Vecf32 {
    varlena: u32,
    len: u16,
    kind: u8,
    reserved: u8,
    phantom: [F32; 0],
}

impl Vecf32 {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<Vecf32>();
        let layout_beta = Layout::array::<F32>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn new_in_postgres(slice: &[F32]) -> Vecf32Output {
        unsafe {
            assert!(u16::try_from(slice.len()).is_ok());
            let layout = Vecf32::layout(slice.len());
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Vecf32;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(Vecf32::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).kind).write(0);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            std::ptr::addr_of_mut!((*ptr).len).write(slice.len() as u16);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            Vecf32Output(NonNull::new(ptr).unwrap())
        }
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn data(&self) -> &[F32] {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, 0);
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len as usize) }
    }
    pub fn data_mut(&mut self) -> &mut [F32] {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, 0);
        unsafe { std::slice::from_raw_parts_mut(self.phantom.as_mut_ptr(), self.len as usize) }
    }
}

impl Deref for Vecf32 {
    type Target = [F32];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl DerefMut for Vecf32 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data_mut()
    }
}

impl AsRef<[F32]> for Vecf32 {
    fn as_ref(&self) -> &[F32] {
        self.data()
    }
}

impl AsMut<[F32]> for Vecf32 {
    fn as_mut(&mut self) -> &mut [F32] {
        self.data_mut()
    }
}

impl Index<usize> for Vecf32 {
    type Output = F32;

    fn index(&self, index: usize) -> &Self::Output {
        self.data().index(index)
    }
}

impl IndexMut<usize> for Vecf32 {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.data_mut().index_mut(index)
    }
}

impl PartialEq for Vecf32 {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        let n = self.len();
        for i in 0..n {
            if self[i] != other[i] {
                return false;
            }
        }
        true
    }
}

impl Eq for Vecf32 {}

impl PartialOrd for Vecf32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Vecf32 {
    fn cmp(&self, other: &Self) -> Ordering {
        use Ordering::*;
        if let x @ Less | x @ Greater = self.len().cmp(&other.len()) {
            return x;
        }
        let n = self.len();
        for i in 0..n {
            if let x @ Less | x @ Greater = self[i].cmp(&other[i]) {
                return x;
            }
        }
        Equal
    }
}

pub enum Vecf32Input<'a> {
    Owned(Vecf32Output),
    Borrowed(&'a Vecf32),
}

impl<'a> Vecf32Input<'a> {
    pub unsafe fn new(p: NonNull<Vecf32>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            Vecf32Input::Owned(Vecf32Output(q))
        } else {
            unsafe { Vecf32Input::Borrowed(p.as_ref()) }
        }
    }
}

impl Deref for Vecf32Input<'_> {
    type Target = Vecf32;

    fn deref(&self) -> &Self::Target {
        match self {
            Vecf32Input::Owned(x) => x,
            Vecf32Input::Borrowed(x) => x,
        }
    }
}

pub struct Vecf32Output(NonNull<Vecf32>);

impl Vecf32Output {
    pub fn into_raw(self) -> *mut Vecf32 {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for Vecf32Output {
    type Target = Vecf32;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl DerefMut for Vecf32Output {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl Drop for Vecf32Output {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl<'a> FromDatum for Vecf32Input<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<Vecf32>()).unwrap();
            unsafe { Some(Vecf32Input::new(ptr)) }
        }
    }
}

impl IntoDatum for Vecf32Output {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vectors.vector")
    }
}

unsafe impl SqlTranslatable for Vecf32Input<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vector"))))
    }
}

unsafe impl SqlTranslatable for Vecf32Output {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vector"))))
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_vecf32_in(input: &CStr, _oid: Oid, typmod: i32) -> Vecf32Output {
    use crate::utils::parse::parse_vector;
    let reserve = Typmod::parse_from_i32(typmod).unwrap().dims().unwrap_or(0);
    let v = parse_vector(input.to_bytes(), reserve as usize, |s| s.parse().ok());
    match v {
        Err(e) => {
            SessionError::BadLiteral {
                hint: e.to_string(),
            }
            .friendly();
        }
        Ok(vector) => {
            if vector.is_empty() || vector.len() > 65535 {
                SessionError::BadValueDimensions.friendly();
            }
            Vecf32::new_in_postgres(&vector)
        }
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_vecf32_out(vector: Vecf32Input<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    if let Some(&x) = vector.data().first() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for &x in vector.data().iter().skip(1) {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}
