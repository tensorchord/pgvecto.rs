use crate::prelude::*;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::ArgumentError;
use pgrx::pgrx_sql_entity_graph::metadata::Returns;
use pgrx::pgrx_sql_entity_graph::metadata::ReturnsError;
use pgrx::pgrx_sql_entity_graph::metadata::SqlMapping;
use pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
use pgrx::Array;
use pgrx::FromDatum;
use pgrx::IntoDatum;
use serde::{Deserialize, Serialize};
use std::alloc::Allocator;
use std::alloc::Global;
use std::alloc::Layout;
use std::cmp::Ordering;
use std::ffi::CStr;
use std::ffi::CString;
use std::num::NonZeroU16;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Index;
use std::ops::IndexMut;
use std::ptr::NonNull;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum VectorTypmod {
    Any,
    Dims(NonZeroU16),
}

impl VectorTypmod {
    pub fn parse_from_str(s: &str) -> Option<Self> {
        use VectorTypmod::*;
        if let Ok(x) = s.parse::<NonZeroU16>() {
            Some(Dims(x))
        } else {
            None
        }
    }
    pub fn parse_from_i32(x: i32) -> Option<Self> {
        use VectorTypmod::*;
        if x == -1 {
            Some(Any)
        } else if 1 <= x && x <= u16::MAX as i32 {
            Some(Dims(NonZeroU16::new(x as u16).unwrap()))
        } else {
            None
        }
    }
    pub fn into_option_string(self) -> Option<String> {
        use VectorTypmod::*;
        match self {
            Any => None,
            Dims(x) => Some(i32::from(x.get()).to_string()),
        }
    }
    pub fn into_i32(self) -> i32 {
        use VectorTypmod::*;
        match self {
            Any => -1,
            Dims(x) => i32::from(x.get()),
        }
    }
    pub fn dims(self) -> Option<u16> {
        use VectorTypmod::*;
        match self {
            Any => None,
            Dims(dims) => Some(dims.get()),
        }
    }
}

pgrx::extension_sql!(
    r#"
CREATE TYPE vector (
    INPUT     = vector_in,
    OUTPUT    = vector_out,
    TYPMOD_IN = vector_typmod_in,
    TYPMOD_OUT = vector_typmod_out,
    STORAGE   = EXTENDED,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
);
"#,
    name = "vector",
    creates = [Type(Vector)],
    requires = [vector_in, vector_out, vector_typmod_in, vector_typmod_out],
);

#[repr(C, align(8))]
pub struct Vector {
    varlena: u32,
    len: u16,
    phantom: [Scalar; 0],
}

impl Vector {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<Vector>();
        let layout_beta = Layout::array::<Scalar>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn new(slice: &[Scalar]) -> Box<Self> {
        unsafe {
            assert!(u16::try_from(slice.len()).is_ok());
            let layout = Vector::layout(slice.len());
            let ptr = Global.allocate(layout).unwrap().as_ptr() as *mut Vector;
            std::ptr::addr_of_mut!((*ptr).varlena).write(Vector::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).len).write(slice.len() as u16);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            Box::from_raw(ptr)
        }
    }
    pub fn new_in_postgres(slice: &[Scalar]) -> VectorOutput {
        unsafe {
            assert!(u16::try_from(slice.len()).is_ok());
            let layout = Vector::layout(slice.len());
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Vector;
            std::ptr::addr_of_mut!((*ptr).varlena).write(Vector::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).len).write(slice.len() as u16);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            VectorOutput(NonNull::new(ptr).unwrap())
        }
    }
    pub fn new_zeroed(len: usize) -> Box<Self> {
        unsafe {
            assert!(u16::try_from(len).is_ok());
            let layout = Vector::layout(len);
            let ptr = std::alloc::Global.allocate_zeroed(layout).unwrap().as_ptr() as *mut Vector;
            std::ptr::addr_of_mut!((*ptr).varlena).write(Vector::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).len).write(len as u16);
            Box::from_raw(ptr)
        }
    }
    #[allow(dead_code)]
    pub fn new_zeroed_in_postgres(len: usize) -> VectorOutput {
        unsafe {
            assert!(u64::try_from(len).is_ok());
            let layout = Vector::layout(len);
            let ptr = pgrx::pg_sys::palloc0(layout.size()) as *mut Vector;
            std::ptr::addr_of_mut!((*ptr).varlena).write(Vector::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).len).write(len as u16);
            VectorOutput(NonNull::new(ptr).unwrap())
        }
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn data(&self) -> &[Scalar] {
        debug_assert_eq!(self.varlena & 3, 0);
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len as usize) }
    }
    pub fn data_mut(&mut self) -> &mut [Scalar] {
        debug_assert_eq!(self.varlena & 3, 0);
        unsafe { std::slice::from_raw_parts_mut(self.phantom.as_mut_ptr(), self.len as usize) }
    }
    #[allow(dead_code)]
    pub fn copy(&self) -> Box<Vector> {
        Vector::new(self.data())
    }
    pub fn copy_into_postgres(&self) -> VectorOutput {
        Vector::new_in_postgres(self.data())
    }
}

impl Deref for Vector {
    type Target = [Scalar];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl DerefMut for Vector {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data_mut()
    }
}

impl AsRef<[Scalar]> for Vector {
    fn as_ref(&self) -> &[Scalar] {
        self.data()
    }
}

impl AsMut<[Scalar]> for Vector {
    fn as_mut(&mut self) -> &mut [Scalar] {
        self.data_mut()
    }
}

impl Index<usize> for Vector {
    type Output = Scalar;

    fn index(&self, index: usize) -> &Self::Output {
        self.data().index(index)
    }
}

impl IndexMut<usize> for Vector {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.data_mut().index_mut(index)
    }
}

impl PartialEq for Vector {
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

impl Eq for Vector {}

impl PartialOrd for Vector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Vector {
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

pub enum VectorInput<'a> {
    Owned(VectorOutput),
    Borrowed(&'a Vector),
}

impl<'a> VectorInput<'a> {
    pub unsafe fn new(p: NonNull<Vector>) -> Self {
        let q = NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap();
        if p != q {
            VectorInput::Owned(VectorOutput(q))
        } else {
            VectorInput::Borrowed(p.as_ref())
        }
    }
}

impl Deref for VectorInput<'_> {
    type Target = Vector;

    fn deref(&self) -> &Self::Target {
        match self {
            VectorInput::Owned(x) => x,
            VectorInput::Borrowed(x) => x,
        }
    }
}

pub struct VectorOutput(NonNull<Vector>);

impl VectorOutput {
    pub fn into_raw(self) -> *mut Vector {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for VectorOutput {
    type Target = Vector;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl DerefMut for VectorOutput {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl Drop for VectorOutput {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl<'a> FromDatum for VectorInput<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<Vector>()).unwrap();
            Some(VectorInput::new(ptr))
        }
    }
}

impl IntoDatum for VectorOutput {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vector")
    }
}

unsafe impl SqlTranslatable for VectorInput<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vector"))))
    }
}

unsafe impl SqlTranslatable for VectorOutput {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vector"))))
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_in(input: &CStr, _oid: Oid, typmod: i32) -> VectorOutput {
    fn solve<T>(option: Option<T>, hint: &str) -> T {
        if let Some(x) = option {
            x
        } else {
            FriendlyError::BadVectorString {
                hint: hint.to_string(),
            }
            .friendly()
        }
    }
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum State {
        MatchingLeft,
        Reading,
        MatchedRight,
    }
    use State::*;
    let input = input.to_bytes();
    let typmod = VectorTypmod::parse_from_i32(typmod).unwrap();
    let mut vector = Vec::<Scalar>::with_capacity(typmod.dims().unwrap_or(0) as usize);
    let mut state = MatchingLeft;
    let mut token: Option<String> = None;
    for &c in input {
        match (state, c) {
            (MatchingLeft, b'[') => {
                state = Reading;
            }
            (Reading, b'0'..=b'9' | b'.' | b'e' | b'+' | b'-') => {
                let token = token.get_or_insert(String::new());
                token.push(char::from_u32(c as u32).unwrap());
            }
            (Reading, b',') => {
                let token = solve(token.take(), "Expect a number.");
                vector.push(solve(token.parse().ok(), "Bad number."));
            }
            (Reading, b']') => {
                if let Some(token) = token.take() {
                    vector.push(solve(token.parse().ok(), "Bad number."));
                }
                state = MatchedRight;
            }
            (_, b' ') => {}
            _ => {
                FriendlyError::BadVectorString {
                    hint: format!("Bad charactor with ascii {:#x}.", c),
                }
                .friendly();
            }
        }
    }
    if state != MatchedRight {
        FriendlyError::BadVectorString {
            hint: "Bad sequence.".to_string(),
        }
        .friendly();
    }
    if vector.len() == 0 || vector.len() > 65535 {
        FriendlyError::BadVecForDims.friendly();
    }
    if let Some(dims) = typmod.dims() {
        if dims as usize != vector.len() {
            FriendlyError::BadVecForUnmatchedDims {
                value_dimensions: dims,
                type_dimensions: vector.len() as u16,
            }
            .friendly();
        }
    }
    Vector::new_in_postgres(&vector)
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_out(vector: VectorInput<'_>) -> CString {
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

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_typmod_in(list: Array<&CStr>) -> i32 {
    if list.is_empty() {
        -1
    } else if list.len() == 1 {
        let s = list.get(0).unwrap().unwrap().to_str().unwrap();
        let typmod = VectorTypmod::parse_from_str(s)
            .ok_or(FriendlyError::BadTypmod)
            .friendly();
        typmod.into_i32()
    } else {
        FriendlyError::BadTypmod.friendly();
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_typmod_out(typmod: i32) -> CString {
    let typmod = VectorTypmod::parse_from_i32(typmod).unwrap();
    match typmod.into_option_string() {
        Some(s) => CString::new(format!("({})", s)).unwrap(),
        None => CString::new("()").unwrap(),
    }
}
